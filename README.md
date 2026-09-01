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

## Arquitetura (alvo)

```
cmd/gostore/            entrypoint, subcomando `server`, bootstrap, graceful shutdown
internal/
  config/               carga de config (flags + env GOSTORE_*), validação
  logger/               logging estruturado (slog)
  api/                  camada HTTP S3
    router.go           roteamento (path-style / vhost-style / sub-resources por query)
    handlers_*.go       handlers de bucket / object / multipart
    errors.go           códigos de erro S3 -> XML
    xml.go              tipos de request/response S3
    middleware.go       request-id, logging, recover, auth
  auth/                 AWS Signature V4/V2, presigned, streaming (aws-chunked)
  iam/                  store de identidades; policy/ engine de avaliação; sts.go
  object/               ObjectLayer (abstração central) + types + errors
  storage/              StorageAPI (ops por disco); local.go (FS); remote/ (RPC p/ nós)
  erasure/              Reed-Solomon; set.go; pool.go; server_pools.go; metadata (xl.meta);
                        bitrot.go; heal.go; quorum.go
  bucket/               versioning/ lifecycle/ lock/ tagging/ policy/ replication/ notification/
  crypto/ kms/          SSE + KMS
  event/                barramento de eventos + targets
  scanner/              data scanner de background (uso de disco, ILM, trigger de heal)
  console/              UI web embutida (go:embed) + admin API
web/                    fonte da UI (build -> internal/console)
```

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
| **M7** | Healing (lite) | `POST /gostore/admin/v1/heal`: varre e reescreve `xl.meta`/shards perdidos ou corrompidos, reconstruindo a partir dos bons. Scanner de background ainda não. | ✅ (parcial) |
| **M8** | IAM | usuários, service accounts, policies AWS (engine própria: Effect/Action/Resource/Condition, deny-vence), 5 canned policies, admin API `/gostore/admin/v1/`, estado JSON replicado. Grupos ainda não. | ✅ |
| **M9** | STS | `AssumeRole` (credenciais temporárias em memória com a policy do chamador ± policy inline). `AssumeRoleWithWebIdentity` (OIDC) ainda não. | ✅ (parcial) |
| **M10** | Bucket features | tagging (bucket + objeto), bucket policy (com Principal, habilita acesso anônimo/público), CORS (preflight + headers). **Versioning real e object lock/WORM: pendentes** (mudança profunda no storage layer). | ✅ (parcial) |
| M11 | Encryption | SSE-S3 / SSE-KMS / SSE-C + KMS local | ⏳ **pendente** |
| **M12** | Eventos | notificação de bucket → webhooks (filtro por evento/prefix/suffix, retry async) | ✅ |
| M13 | Replicação | replicação de bucket | ⏳ pendente |
| **M14** | Console | SPA embutida (`go:embed`), servida em `/gostore/console/` na mesma origem da API; assina SigV4 no browser (Web Crypto). Buckets, objetos (upload/download/navegação), usuários/service accounts, monitoramento + heal. | ✅ |

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
ideal — N discos/mounts separados. Cluster multi-nó é M6.

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
requests sem assinatura — só para debug).

## Erasure coding (M4) — como funciona

Cada parte de um objeto é quebrada em *stripes* de `blockSize` (1 MiB) por
shard de dados. Cada stripe é codificado em `N/2` shards de dados + `N/2`
shards de paridade (Reed-Solomon), um shard por disco. O objeto sobrevive à
perda de até `N/2` discos.

- `xl.meta` (JSON) é escrito **idêntico em todos os discos** — metadados
  sobrevivem à mesma perda; a versão vencedora é escolhida por maioria.
- **Bitrot**: hash HighwayHash-256 por stripe por shard, guardado no
  `xl.meta`. Na leitura, um shard cujo hash não bate é descartado e o
  Reed-Solomon reconstrói a partir dos bons — inclusive em leitura por range.
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

**Auth & IAM** — SigV4 (header + presigned + streaming), múltiplos usuários,
service accounts, policies AWS (engine própria, `deny` vence), 5 canned
policies, bucket policy (com acesso anônimo/público via `Principal:*`), STS
`AssumeRole`. Admin API em `/gostore/admin/v1/` (info, users, policies,
service-accounts, heal). Estado IAM/bucket-config em JSON replicado nos
volumes — **sem banco de dados**.

**Storage** — single-disk ou erasure (1+ sets num pool); reconstrução na
leitura + `heal` sob demanda; bitrot HighwayHash.

**Extras** — notificações via webhook; CORS; console web embutido.

## O que **não** funciona ainda

- **Cluster multi-nó** (M6) — hoje é single-node (1+ discos locais). Precisa
  de RPC entre nós + lock distribuído.
- **Versioning real e Object Lock / WORM** (M10) — endpoints de versioning
  são *accept-and-ignore*; toda operação é sobre a versão corrente.
- **SSE / KMS** (M11) — sem criptografia em repouso.
- **Replicação de bucket** (M13).
- **Scanner de background** (M7) — o heal é sob demanda (`POST .../heal`).
- Grupos IAM, `AssumeRoleWithWebIdentity` (OIDC/LDAP), lifecycle/ILM.

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
