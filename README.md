# gostore

Object storage server com API compatível com Amazon S3, escrito em Go.
Reimplementação da arquitetura do MinIO (referência: `RELEASE.2025-02-03T21-03-04Z`).

> Nome `gostore` é provisório. Para renomear o módulo:
> `go mod edit -module github.com/SEU/NOME && grep -rl github.com/lojadopocket/gostore --include='*.go' | xargs sed -i 's#github.com/lojadopocket/gostore#github.com/SEU/NOME#g'`

## Objetivo

Paridade funcional com o MinIO full:

- API S3 completa (path-style + virtual-host style), SigV4/SigV2, presigned, streaming chunked
- Erasure coding distribuído (Reed-Solomon) com bitrot protection e healing
- Server pools (múltiplos conjuntos de discos, múltiplos nós)
- IAM (usuários, grupos, policies estilo AWS, service accounts) + STS
- Versioning, Object Lock (WORM), Lifecycle (ILM), Tagging, Bucket Policy, CORS
- SSE-S3 / SSE-KMS / SSE-C + KMS
- Notificações de eventos
- Replicação de bucket
- Console web

## Arquitetura (estado atual)

```
cmd/gostore/       entrypoint `server`, bootstrap, graceful shutdown, scanner loop
internal/
  config/          config (flags + env GOSTORE_*), grupos de volumes, validação
  logger/          logging estruturado (slog)
  auth/            AWS SigV4 (header / presigned / aws-chunked streaming) — primitivas
  api/             camada HTTP S3 + admin: router, ~30 códigos de erro S3->XML,
                   authenticate + authorize (IAM + bucket policy), handlers de
                   service/bucket/object/multipart/tagging/policy/cors/notification/
                   versioning/object-lock/replication/lifecycle, STS, admin API,
                   /gostore/console (SPA), /gostore/health/*
  object/          object.Layer (abstração central) + types + errors
  object/fs/       backend single-disk: CRUD, multipart, range, versioning real,
                   object lock (WORM), SSE-S3, tagging
  storage/         StorageAPI + LocalDisk (uma pasta = um "disco" do erasure)
  erasure/         Reed-Solomon (klauspost), Set (N discos) + Pool (N sets),
                   xl.meta replicado, bitrot HighwayHash por stripe, quorum,
                   heal, versioning + object-lock por vlog
  cluster/         M6: RemoteDisk (erasure.Disk sobre RPC), RPCServer,
                   lock por quorum (dsync-lite), parse de topologia
  iam/             usuários / service accounts / STS + iam/policy engine AWS
  kms/  sse/       master key local + AES-256-GCM em chunks (SSE-S3)
  bucketcfg/       config por bucket (policy, CORS, tags, notification, versioning,
                   object-lock, replication, lifecycle) — JSON replicado nos volumes
  event/           barramento de notificação -> webhooks
  replication/     cópia async pra bucket local ou endpoint S3 remoto
  scanner/         scanner de background: expiração ILM, abort de multipart velho
  console/         SPA embutida (go:embed) servida pela própria API
```

**Sem banco de dados.** Tudo — dados, xl.meta, IAM, config de bucket — vive nos
volumes.

## Milestones (ordem de dependência — cada um roda de verdade)

| # | Milestone | Entrega verificável | Status |
|---|-----------|---------------------|--------|
| **M0** | Scaffold | módulo, `gostore server`, HTTP sobe, health responde, interfaces `ObjectLayer` + `StorageAPI` | ✅ |
| **M1** | Backend single-disk | `ObjectLayer` FS: bucket CRUD, PUT/GET/HEAD/DELETE, ListObjects v1/v2, prefix/delimiter/paginação | ✅ |
| **M2** | Auth | SigV4 header + presigned + streaming (`aws-chunked` com verificação de assinatura por chunk), credencial root | ✅ |
| **M3** | Object semantics | multipart upload completo, range (`bytes=`), conditional requests (If-Match/If-None-Match/If-*-Since), ETag multipart (`-N`), metadata `x-amz-meta-*`, CopyObject/CopyObjectPart, DeleteObjects em lote | ✅ |
| **M4** | Erasure coding | Reed-Solomon sobre N discos locais (N par, ≥4; paridade = N/2), `xl.meta` próprio replicado, bitrot HighwayHash por stripe/shard, quorum de leitura/escrita, reconstrução automática na leitura, multipart erasure-coded | ✅ |
| **M5** | Server pools | 1 erasure set por argumento do `server`, todos formam 1 pool; placement por hash do nome; listagem faz merge entre sets | ✅ |
| M6 | Distribuído | disco remoto via RPC, cluster multi-nó, lock distribuído (estilo dsync) | ⏳ **pendente** (precisa de protocolo RPC + consenso; não dá pra apressar com segurança) |
| **M6** | Distribuído | `internal/cluster`: `RemoteDisk` (erasure.Disk sobre RPC HTTP interno, bearer token), 1 erasure set abrangendo todos os nós, lock por quorum (dsync-lite: N/2+1 nós concedem, TTL + refresh). Args `http://host:port/data/d{1...4}`, `GOSTORE_CLUSTER_SELF` + `GOSTORE_CLUSTER_SECRET`. Membership estático; config IAM/bucket compartilhada via camada de objetos (`.gostore.sys/`, quorum) + refresh 30 s; lock com retry+backoff e cancelamento por perda de quorum; RPC unário multiplexado (1 conexão por par de nós, handshake `Upgrade: gostore-grid`, fallback HTTP); decommission + rebalance de set online (`/gostore/admin/v1/pool`). | ✅ (lite) |
| **M7** | Healing (lite) | `POST /gostore/admin/v1/heal`: varre e reescreve `xl.meta`/shards perdidos ou corrompidos. Scanner de background faz ILM (heal proativo ainda não). | ✅ (parcial) |
| **M8** | IAM | usuários, service accounts, engine de policy AWS própria (Effect/Action/NotAction/Resource/Condition, Principal, deny-vence), 5 canned policies, admin API `/gostore/admin/v1/`, estado JSON replicado. Grupos ainda não. | ✅ |
| **M9** | STS | `AssumeRole` (credenciais temporárias em memória, policy do chamador ∩ policy inline). OIDC/LDAP ainda não. | ✅ (parcial) |
| **M10** | Bucket features | **versioning real** (versões, `?versionId`, delete markers, `ListObjectVersions`) — **single-disk + erasure**. **Object Lock / WORM** (GOVERNANCE/COMPLIANCE + legal hold, bypass com check de IAM) — **single-disk + erasure**. **lifecycle/ILM** (expiração via scanner), tagging (bucket+objeto), bucket policy (Principal → anônimo/público), CORS. | ✅ |
| **M11** | Encryption | **SSE-S3**: KMS local (master key) + AES-256-GCM em chunks de 64 KiB (stream + range), ETag = md5 do plaintext, transparente no PUT/GET. **single-disk + erasure** (erasure: single-part). SSE-KMS/SSE-C ainda não. | ✅ (parcial) |
| **M12** | Eventos | notificação de bucket → webhooks (filtro evento/prefix/suffix, retry async) | ✅ |
| **M13** | Replicação | cópia async de objetos (PUT/DELETE) pra bucket local ou endpoint S3 remoto (SigV4), filtro por prefixo. Sem fila de retry persistente. | ✅ (lite) |
| **M14** | Console | SPA embutida (`go:embed`) servida em `/gostore/console/` na mesma origem da API; assina SigV4 no browser (Web Crypto). Buckets, objetos (drag&drop, drawer, links presigned, versões), usuários/service accounts, editores de policy/CORS/lifecycle/replicação/versioning, monitoramento + heal. | ✅ |

## Rodando

**Single-disk** (um volume, backend `internal/object/fs`) ou **erasure** (N
volumes pares, N≥4, backend `internal/erasure`). O modo é escolhido pela
quantidade de volumes passada ao `server`.

```bash
# single-disk
gostore server /data

# erasure: 4 discos (2 dados + 2 paridade) -> aguenta perder 2 discos
gostore server /data/d1 /data/d2 /data/d3 /data/d4
# ou com ellipsis
gostore server /data/d{1...4}
```

Na mesma máquina, os N volumes podem ser diretórios do mesmo disco físico
(dá proteção contra bitrot e reconstrução, mas não redundância física) ou —
ideal — N discos/mounts separados.

**Cluster multi-nó (M6):** passe endpoints `http(s)://host:port/path` — os
discos de todos os nós formam **um** erasure set. Em cada nó:

```bash
export GOSTORE_CLUSTER_SECRET=um-segredo-compartilhado
# no node1:
GOSTORE_CLUSTER_SELF=http://node1:9000 gostore server \
  http://node1:9000/data/d{1...4} http://node2:9000/data/d{1...4}
# no node2 (mesmos args, só muda o SELF):
GOSTORE_CLUSTER_SELF=http://node2:9000 gostore server \
  http://node1:9000/data/d{1...4} http://node2:9000/data/d{1...4}
```

8 discos → 4 dados + 4 paridade → sobrevive à perda de um nó inteiro. O
tráfego entre nós (`/gostore/internal/`) usa bearer token — rode numa rede
confiável ou atrás de mTLS. Membership é estático.

### Local

```bash
make build           # -> dist/gostore
GOSTORE_ROOT_USER=gostoreadmin \
GOSTORE_ROOT_PASSWORD=uma-senha-com-min-8-chars \
dist/gostore server --address :9000 --console-address :9001 ./data/disk1

curl -s localhost:9000/gostore/health/ready
```

### Docker

```bash
docker compose up -d --build      # edite as credenciais no docker-compose.yml antes
# S3 API  -> http://<host>:9000
# Console -> http://<host>:9001
```

O container roda como usuário não-root (uid `65532`). Um **named volume** novo
(o padrão do compose) herda a permissão certa da imagem — não precisa fazer
nada. Se você já subiu com uma imagem antiga e tomou `permission denied`,
recrie o volume: `docker compose down -v && docker compose up -d --build`.

Se usar **bind mount de host** (`-v /srv/gostore/data:/data`), o dono do
diretório do host prevalece, então antes:
`sudo mkdir -p /srv/gostore/data && sudo chown -R 65532:65532 /srv/gostore/data`.

### EasyPanel / Coolify / Dokploy (PaaS sem shell)

1. **App type:** Dockerfile (aponta pro repo `gostore`, branch `main`).
2. **Porta exposta:** `9000` (a API S3). A `9001` do console é opcional.
3. **Env vars:**
   ```
   GOSTORE_ROOT_USER=gostoreadmin
   GOSTORE_ROOT_PASSWORD=<senha forte, >=8>
   GOSTORE_REGION=us-east-1
   ```
4. **Volume / Mount:** monta um volume persistente em `/data`. A imagem roda
   como root, então mount root-owned do painel funciona sem chown.
5. Deploy. Domínio do painel -> serviço, porta `9000`.

**Console web:** `https://SEU_DOMINIO/gostore/console/` — login com o
access/secret key, browse de buckets e objetos, upload/download, gestão de
usuários e service accounts, monitoramento.

**Validar sem CLI:** abre `https://SEU_DOMINIO/gostore/health/selftest` — o
servidor faz um round-trip interno (MakeBucket -> PutObject -> GetObject ->
verifica bytes -> ListObjectsV2 -> DeleteObject -> DeleteBucket) e responde
JSON `{"ok": true, "steps": [...]}`. (Desabilita com `GOSTORE_DISABLE_SELFTEST=1`.)

Depois é só apontar `mc` / `aws` / SDK do seu PC pro `https://SEU_DOMINIO`
(path-style) com as credenciais acima.

### VPS (systemd, bare-metal)

```bash
make build-linux                  # -> dist/gostore-linux-amd64
scp dist/gostore-linux-amd64 user@vps:/tmp/gostore
# no VPS:
sudo install -m0755 /tmp/gostore /usr/local/bin/gostore
sudo useradd --system --home /var/lib/gostore --shell /usr/sbin/nologin gostore
sudo mkdir -p /var/lib/gostore/data && sudo chown -R gostore:gostore /var/lib/gostore
sudo cp deploy/gostore.service /etc/systemd/system/
sudo cp deploy/gostore.env /etc/gostore.env && sudo chmod 600 /etc/gostore.env
sudo nano /etc/gostore.env        # >>> troque GOSTORE_ROOT_PASSWORD <<<
sudo systemctl daemon-reload && sudo systemctl enable --now gostore
sudo systemctl status gostore
```

Coloque um TLS terminator (Caddy/nginx) na frente da porta 9000 para produção.

## Usando com clientes S3

Path-style, região `us-east-1` (default). Exemplos com `mc` e `aws`:

```bash
# MinIO client
mc alias set gs http://localhost:9000 gostoreadmin uma-senha-com-min-8-chars
mc mb gs/meu-bucket
mc cp ./arquivo.zip gs/meu-bucket/
mc ls gs/meu-bucket
mc cat gs/meu-bucket/arquivo.zip | sha256sum

# AWS CLI (force path-style)
aws --endpoint-url http://localhost:9000 s3 mb s3://meu-bucket
aws --endpoint-url http://localhost:9000 s3 cp ./big.iso s3://meu-bucket/    # multipart automático
aws --endpoint-url http://localhost:9000 s3 ls s3://meu-bucket
aws --endpoint-url http://localhost:9000 s3api head-object --bucket meu-bucket --key big.iso
```

Config env: `GOSTORE_ROOT_USER`, `GOSTORE_ROOT_PASSWORD` (>=8), `GOSTORE_REGION`,
`GOSTORE_ADDRESS`, `GOSTORE_CONSOLE_ADDRESS`, `GOSTORE_DOMAIN` (vhost-style),
`GOSTORE_LOG_LEVEL`, `GOSTORE_LOG_JSON=1`, `GOSTORE_ALLOW_ANONYMOUS=1` (aceita
requests sem assinatura — só debug), `GOSTORE_KMS_MASTER_KEY` (base64 de 32
bytes; senão é gerada em `.gostore.sys/kms/master.key`), `GOSTORE_SCAN_INTERVAL`
(ex. `30m`; default `1h`), `GOSTORE_DISABLE_SELFTEST=1`.

Tuning do backend erasure (opcionais):
`GOSTORE_INLINE_MAX` (bytes; objetos até esse tamanho ficam dentro do
`xl.meta`; default `131072`, `0` desliga), `GOSTORE_MRF_INTERVAL` (cadência do
auto-heal de escritas parciais; default `5m`), `GOSTORE_LIST_CACHE_TTL`
(cache de listagem por bucket; default `15s`, `0` desliga),
`GOSTORE_CLUSTER_SELF`, `GOSTORE_CLUSTER_SECRET` (modo cluster).

IAM e config de bucket são gravados como objetos em `.gostore.sys/` pela
camada de armazenamento (replicados em todos os discos/nós, leitura por
maioria) e recarregados a cada 30 s — um usuário criado num nó aparece nos
outros sem banco externo.

## Erasure coding (M4) — como funciona

Cada parte de um objeto é quebrada em *stripes* de `blockSize` (1 MiB) por
shard de dados. Cada stripe é codificado em `N/2` shards de dados + `N/2`
shards de paridade (Reed-Solomon), um shard por disco. O objeto sobrevive à
perda de até `N/2` discos.

- `xl.meta` (JSON) é escrito **idêntico em todos os discos** — metadados
  sobrevivem à mesma perda; a versão vencedora é escolhida por maioria.
- **Objetos pequenos** (até `GOSTORE_INLINE_MAX`, default 128 KiB) vão
  *dentro* do `xl.meta` (campo `inline`) — uma operação de arquivo por disco
  em vez de um shard-file extra.
- **Bitrot**: hash HighwayHash-256 por bloco de stripe, gravado **intercalado
  no próprio shard-file** (`[hash|bloco][hash|bloco]…`), então o `xl.meta`
  tem tamanho constante. Na leitura, um bloco cujo hash não bate é descartado
  e o Reed-Solomon reconstrói a partir dos bons — inclusive em leitura por
  range. Objetos escritos por versões antigas (checksums no `xl.meta`) são
  lidos pelo caminho legado automaticamente.
- **Auto-heal**: escritas que atingem quorum mas não todos os discos entram
  numa fila MRF persistente e são re-healadas em segundo plano; o scanner
  ainda heala 1-em-128 objetos por passada; um disco novo/substituído é
  detectado no boot e repovoado sozinho.
- **Quorum**: leitura = `N/2` discos; escrita = `N/2 + 1`.
- **Multipart**: cada parte é um blob erasure-coded próprio em
  `.gostore.sys/multipart/<uploadId>/`; no `CompleteMultipartUpload` as
  partes são decodificadas em streaming e reescritas como as partes do
  objeto final.

Layout no disco:

```
<disco>/<bucket>/<key>/xl.meta        cópia completa dos metadados
<disco>/<bucket>/<key>/part.00001     shard deste disco para a parte 1
<disco>/.gostore.sys/format.json      identidade do disco
<disco>/.gostore.sys/tmp/             staging do rename atômico
```

Ainda **não** tem healing proativo (reescrever shards perdidos de volta ao
disco) — isso é M7; hoje a reconstrução acontece só em memória na leitura.

## O que funciona hoje

**S3 API** — Service: `ListBuckets`. Bucket: `CreateBucket`, `DeleteBucket`,
`HeadBucket`, `GetBucketLocation`, `GetBucketVersioning` (vazio),
`Get/Put/DeleteBucketPolicy`, `Get/Put/DeleteBucketTagging`,
`Get/Put/DeleteBucketCors`, `Get/PutBucketNotification`, `ListObjects` (v1),
`ListObjectsV2`, `ListObjectVersions` (versão `null`), `DeleteObjects` (lote).
Object: `PutObject` (incl. `aws-chunked` streaming), `GetObject` (Range +
condicionais), `HeadObject`, `DeleteObject`, `CopyObject`,
`Get/Put/DeleteObjectTagging`. Multipart completo (`Create`/`UploadPart`/
`UploadPartCopy`/`ListParts`/`ListMultipartUploads`/`Complete`/`Abort`).

Bucket sub-resources: `?policy`, `?tagging`, `?cors`, `?notification`,
`?versioning`, `?object-lock`, `?replication`, `?lifecycle`. Object:
`?tagging`, `?retention`, `?legal-hold`, `?versionId`.

**Auth & IAM** — SigV4 (header + presigned + streaming), múltiplos usuários,
service accounts, engine de policy AWS própria (`deny` vence, Principal,
condições String*/IpAddress), 5 canned policies, bucket policy (anônimo/
público via `Principal:*`), STS `AssumeRole`, `s3:BypassGovernanceRetention`.
Admin API `/gostore/admin/v1/` (info, users, policies, service-accounts,
heal, scanner/run). Estado em JSON replicado nos volumes — **sem DB**.

**Versioning & WORM** — versões reais, `?versionId`, delete markers,
`ListObjectVersions`. Object Lock GOVERNANCE/COMPLIANCE + legal hold por
versão, com enforcement no delete. *Backend single-disk.*

**Criptografia** — SSE-S3: `x-amz-server-side-encryption: AES256` no PUT →
AES-256-GCM em chunks, DEK envelopada pela master key local. GET/HEAD/Range
transparentes. *Backend single-disk.*

**Storage** — single-disk (`internal/object/fs`) ou erasure (1+ sets num
pool, `internal/erasure`); reconstrução na leitura + `heal` sob demanda;
bitrot HighwayHash por stripe.

**Automação** — scanner de background (ILM: expiração de objetos/versões,
abort de multipart velho); replicação async (local ou S3 remoto);
notificações via webhook.

**Console** — SPA embutida, servida pela própria API.

## O que **não** funciona ainda

- **Cluster (M6-lite) tem limites**: membership estático (reiniciar com a
  mesma topologia); config IAM/bucket é replicada só nos discos locais de
  cada nó — mudanças de usuário/policy precisam ser feitas em todos os nós
  ou com um volume de config compartilhado. Sem rebalanceamento ao
  adicionar nós.
- **SSE-KMS / SSE-C** (só SSE-S3), KMS externo. Multipart no erasure não
  criptografa (single-part sim).
- **Grupos IAM**, `AssumeRoleWithWebIdentity` (OIDC/LDAP).
- Lifecycle: transições de storage class (só expiração).
- Replicação: sem fila de retry persistente (best-effort, 3 tentativas).
- Heal proativo de background (o scanner só faz ILM; heal é sob demanda).

## Formato on-disk

```
<volume>/<bucket>/<key>                              dados do objeto (espelha a key)
<volume>/.gostore.sys/format.json                    identidade + versão do disco
<volume>/.gostore.sys/buckets/<bucket>.json          metadata do bucket
<volume>/.gostore.sys/meta/<bucket>/<key>.json       sidecar do objeto (etag, content-type, x-amz-meta-*, parts)
<volume>/.gostore.sys/multipart/<bucket>/<uploadID>/ staging de multipart
<volume>/.gostore.sys/tmp/                            staging de escrita atômica (rename)
```

(single-disk mostrado; no modo erasure é `<disco>/<bucket>/<key>/xl.meta` +
`part.NNNNN`.) Além disso, replicado em `<vol>/.gostore.sys/`:
`iam/store.json` (usuários/policies/service-accounts) e
`bucketcfg/config.json` (policy/CORS/tags/notification por bucket).

Escrita é atômica via `write-tmp + fsync + rename`.

## Testes

```bash
go test ./...
```

Cobre: CRUD de bucket/objeto, listagem prefix/delimiter/paginação, multipart
round-trip + rejeição de part pequena, CopyObject; erasure — round-trip em
vários tamanhos, range straddling stripes, sobrevivência a perda de N/2
discos + falha limpa além disso, bitrot detectado+reparado, heal reescreve
shards, server pools (60 chaves em 3 sets); IAM — engine de policy
(wildcards, deny-vence, resource scoping), enforcement por usuário,
persistência, service account herda+restringe; HTTP e2e — fluxo S3 assinado
com SigV4, streaming chunked com cadeia de assinatura, bucket policy
public-read (anon GET ok / anon PUT 403), object tagging. O signer SigV4 do
console (Web Crypto) é validado ponta-a-ponta contra o servidor.
