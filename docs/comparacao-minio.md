# gostore vs MinIO — comparação de arquitetura e lista de melhorias

Análise do MinIO `RELEASE.2025-02-03T21-03-04Z` (clone raso da tag), focada
nos subsistemas que existem também no gostore: erasure coding, formato de
metadados em disco, bitrot, lock distribuído, transporte entre nós, listagem,
healing, o scanner de fundo e o armazenamento de IAM/config.

## Escala

| | MinIO | gostore |
|---|---|---|
| Linhas de Go (sem testes/gerado) | ~204.000 | ~13.000 |
| Linhas de Go (tudo) | ~342.000 | ~16.000 |
| Dependências diretas | ~120 | 4 (`reedsolomon`, `cpuid`, `highwayhash`, `x/sys`) |
| Console embutido | repositório separado (`minio/console`, React) | ~1.400 linhas, sem dependências, `go:embed` |

O gostore cobre praticamente a mesma **superfície de funcionalidades** para
nó único + um cluster básico com ~6% do código, deixando de fora: S3 Select,
tiering / ILM para nuvem, replicação *site-a-site* ativo-ativo, batch jobs,
métricas Prometheus v2/v3, decommission e rebalance de pools, KMS KES/Vault,
OpenID/LDAP, o scanner com bloom filter, o transporte multiplexado (grid),
MessagePack em tudo, e ~15 anos de casos extremos acumulados.

## Subsistema por subsistema

### Layout do objeto em disco

| | MinIO | gostore |
|---|---|---|
| Arquivo de metadados | `xl.meta` — **MessagePack**, versionado (cabeçalho `XL2 `, v1.3), um *journal* indexado com todas as versões (`object` / `delete` / `legacy`) num só arquivo | `xl.meta` — **JSON**, um arquivo = uma versão atual; versões antigas numa árvore paralela `.gostore.sys/vs/<key>/<vid>/` + um índice `vlog.json` |
| Dados da versão | `bucket/objeto/<versionUUID>/part.1` — versões são subpastas com nome UUID ao lado do `xl.meta` | atual em `bucket/key/part.NNNNN`; versões arquivadas na árvore reservada |
| Objetos pequenos | **inline** no `xl.meta` quando `< 128 KiB` (`xlFlagInlineData`) — uma operação de arquivo, não duas | sempre um `part.00001` separado, mesmo para um objeto de 3 bytes |
| Leitura parcial de versão | cabeçalho indexado → lê a lista de versões sem decodificar cada versão | parse do JSON inteiro |

### Erasure coding

| | MinIO | gostore |
|---|---|---|
| Codec | `klauspost/reedsolomon`, bloco de 1 MiB | igual, bloco de 1 MiB |
| Sets / pools | pools → sets de `setDriveCount` (4/6/…/16); objetos são distribuídos por hash num set; **decom + rebalance** suportados | um set cobrindo todos os discos (ou um set por grupo de argumentos da CLI); sem decom/rebalance |
| Quórum de escrita | `data == parity ? data+1 : data` | sempre `data+1` |
| Bitrot | **streaming / intercalado**: o arquivo do shard é `[hash][bloco][hash][bloco]…`; verificado incrementalmente na leitura, nada no `xl.meta` | HighwayHash **por stripe por disco, guardado no `xl.meta`** — infla os metadados em ~`64B × stripes × N` |
| Healing | sob demanda + **scanner de fundo** (1 em 1024 objetos por ciclo) + **fila MRF** (ver abaixo) + **auto-heal de disco novo** | só sob demanda (`POST /gostore/admin/v1/heal`) |

### Lock distribuído

| | MinIO (`internal/dsync`) | gostore (`internal/cluster` dsync-lite) |
|---|---|---|
| Modelo | quórum de concessões (`N − N/2`, escrita sobe para `N/2+1` em N par) | mesma matemática de quórum |
| Aquisição | **loop de retry com backoff** (mín. 250 ms) até `opts.Timeout` | tentativa única — falha na hora se houver contenção |
| Liveness | refresh contínuo (10 s); **na perda de quórum a operação em andamento é cancelada via `lockLossCallback`** | refresh (20 s), mas a operação **não** é abortada se o refresh falhar |
| Transporte | `dsync.NetLocker` pelo grid | POST HTTP por lock/unlock/refresh |

### Transporte entre nós

| | MinIO | gostore |
|---|---|---|
| Protocolo | `internal/grid` — um **websocket multiplexado** persistente por par de nós, framing MessagePack, request/response **e** streaming | `RemoteDisk` — **uma ida-e-volta HTTP por operação de disco** (`ReadAll`, `WriteAll`, `CreateFile`, …), auth por bearer token |
| Custo | ~0 por operação | um request/response HTTP completo por leitura/escrita de shard |

### Listagem

| | MinIO (`metacache`) | gostore |
|---|---|---|
| Walk | streamado por `askDisks` (quórum) discos através de um canal, mesclado + deduplicado | percorre **um** disco online, recursivo, junta **todas** as chaves em memória, ordena |
| Paginação | o conjunto ordenado é **cacheado** em disco (`.minio.sys/buckets/<b>/.metacache/`); a continuação retoma em O(1) | **re-percorre + re-ordena o bucket inteiro a cada página** |
| Custo num bucket de 1M objetos | primeira página percorre, o resto são leituras de cache | toda página é O(bucket) |

### IAM e configuração de bucket

| | MinIO | gostore |
|---|---|---|
| Armazenamento | plugável: **`IAMObjectStore`** (padrão — o IAM fica como *objetos* em `.minio.sys/config/`, então é erasure-coded e compartilhado pelo cluster de graça) ou `IAMEtcdStore` | arquivos JSON replicados para a raiz de cada volume **local** (`.gostore.sys/iam/store.json`, `.gostore.sys/bucketcfg/config.json`) |
| Comportamento em cluster | uma única fonte da verdade, refresh periódico + watch | **por nó** — um usuário criado no nó A é invisível no nó B |
| Federação | OpenID / LDAP / AssumeRoleWithWebIdentity | root + usuários locais + service accounts + `AssumeRole` só |

### Scanner

| | MinIO (`data-scanner`) | gostore |
|---|---|---|
| Roda | a cada ciclo (atraso inicial de 1 min), **bloom filter por caminho** para pular prefixos inalterados | intervalo fixo (`GOSTORE_SCAN_INTERVAL`, padrão 1 h), sempre walk completo |
| Faz | contabilização de uso de dados, lifecycle/ILM, **heal oportunista**, GC de tier, resync de replicação | só expiração de lifecycle + aborto de multipart velho |

## Lista de melhorias (ranqueada)

### 1. Guardar IAM + config de bucket na camada de objetos — *alto valor, esforço médio*

Trocar os arquivos JSON por-volume por objetos num bucket de sistema
reservado, gravados pela `object.Layer`. Isso torna a config do cluster uma
única fonte da verdade (erasure-coded, leitura por quórum) e remove a maior
ressalva do M6. Adicionar um intervalo curto de refresh para os nós pegarem
mudanças dos outros.

### 2. Bitrot streaming intercalado — *alto valor, esforço médio*

Adotar o layout de arquivo de shard do MinIO, `[hash][bloco]…`. Remove os
arrays `Checksums[][]` do `xl.meta` (hoje o termo dominante para objetos
grandes) e permite verificar + healar por bloco sem carregar os metadados.
Mudam `internal/erasure/bitrot.go` + `set.encodePart` / `readPart`; o campo
`Checksums` do `xlmeta` some.

### 3. Objetos pequenos inline — *alto valor, esforço baixo-médio*

Objetos abaixo de um limite (começar em 128 KiB) vão para dentro do
`xl.meta` num campo `Data []byte` (logicamente ainda dividido por RS, mas um
arquivo só). Corta pela metade as syscalls em cargas de objetos pequenos e
se comporta muito melhor na maioria dos sistemas de arquivos.
`storage.FileInfo` já declara um `Data []byte` não usado — é só ligar ele em
`CreateFile` / `ReadFileStream` / o caminho de part do erasure.

### 4. Fila de heal de operação parcial (estilo MRF) — *alto valor, esforço médio*

Quando uma escrita alcança o quórum mas não todos os N discos, adicionar o
objeto a uma lista persistente (`.gostore.sys/heal/mrf.list`); um worker de
fundo tenta `heal(bucket, key)` de novo nessas entradas. Transforma "heal
manual" em auto-heal depois de falhas transitórias de disco/nó. Enganchar em
`set.putObject` / `encodePart` onde `okCount < n`.

### 5. Listagem paginada com cache (metacache-lite) — *alto valor, esforço médio-alto*

Cachear a slice ordenada de chaves por `(bucket, prefixo, delimitador)` com
um TTL (ex.: 30 s) indexada pelo continuation token, para as páginas do
`ListObjectsV2` depois da primeira serem leituras de cache. Também trocar o
walk para streamar por um canal em vez de montar a slice inteira em memória.
Maior ganho em buckets com > ~100 mil objetos.

### 6. Retry no dsync + cancelamento por perda de lock — *valor médio, esforço baixo-médio*

`internal/cluster/dsync.go`: tornar o `grab()` um loop de retry com backoff
limitado por timeout em vez de tentativa única, e fazer o refresher abortar
o chamador (context cancel / callback) se ele não conseguir mais confirmar o
quórum. Evita uma janela rara de escrita dupla e impede PUTs de objeto de
falharem na hora sob contenção.

### 7. Scanner: heal oportunista + contabilização de uso + skip por bloom — *valor médio, esforço médio*

Estender `internal/scanner`: manter um bloom / cache de "última modificação"
por pasta de objeto para pular subárvores limpas; healar 1 em N objetos por
passada; acumular contagem de objetos + bytes por bucket e expor em
`/gostore/admin/v1/info` (o dashboard já tem os tiles).

### 8. Auto-heal de disco / nó novo — *valor médio, esforço médio*

Detectar um disco não formatado ou vazio no boot (ou quando volta online) e
healar todo objeto que deveria ter um shard ali. Depende do #4 + #7.

### 9. Transporte multiplexado persistente entre nós — *valor médio, esforço alto*

Trocar o HTTP-por-operação do `RemoteDisk` por uma conexão longa com
multiplexação de requests (um protocolo pequeno com framing sobre um único
stream HTTP/2 ou websocket keep-alive). Puro throughput/latência; a corretude
já está ok.

### 10. Decommission e rebalance de pool — *baixa prioridade, esforço alto*

Só importa quando se adiciona/remove capacidade em escala. Fora de escopo
até existir um deploy real com múltiplos pools.

## O que o gostore faz de propósito diferente (e está tudo bem)

- **Um erasure set por cluster** em vez de sets-de-N dentro de pools — mais
  simples, correto para clusters pequenos; precisaria do #10 para escalar a
  centenas de discos.
- **Config em JSON puro** em vez de MessagePack + etcd — a correção certa é o
  #1 (colocar na camada de objetos), não "adicionar etcd".
- **API admin nativa em JSON** em vez do RPC `madmin` criptografado do MinIO —
  o console e o `curl` bastam; sem compatibilidade com `mc admin`.
- **Console embutido sem dependências** em vez de um app React separado — mais
  leve de distribuir, menos rico.
- **Só SigV4** (o MinIO mantém SigV2 para clientes legados).
