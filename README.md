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
| M5 | Server pools | múltiplos sets/pools, hashing de placement por nome de objeto, merge de listagem | ⏳ próximo |
| M5 | Server pools | múltiplos sets/pools, hashing de placement por nome de objeto, merge de listagem |
| M6 | Distribuído | disco remoto via RPC, cluster multi-nó, lock distribuído (estilo dsync) |
| M7 | Healing + scanner | reconstrução automática, scanner de background |
| M8 | IAM | store de user/group/policy, engine de policy AWS, service accounts, admin API |
| M9 | STS | AssumeRole, AssumeRoleWithWebIdentity |
| M10 | Bucket features | versioning -> object lock -> lifecycle -> tagging -> bucket policy -> CORS |
| M11 | Encryption | SSE-S3 / SSE-KMS / SSE-C + KMS local |
| M12 | Eventos | notificações + targets (webhook) |
| M13 | Replicação | replicação de bucket |
| M14 | Console | UI web |

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

**Validar sem CLI:** abre no navegador
`https://SEU_DOMINIO/gostore/health/selftest` — o servidor faz um round-trip
interno (MakeBucket -> PutObject -> GetObject -> verifica bytes -> ListObjectsV2
-> DeleteObject -> DeleteBucket) e responde JSON `{"ok": true, "steps": [...]}`.
Se vier `ok:true`, o engine de storage tá 100%. (Desabilita com
`GOSTORE_DISABLE_SELFTEST=1`.)

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

## O que funciona hoje (M3 single-disk / M4 erasure)

Service: `ListBuckets`. Bucket: `CreateBucket`, `DeleteBucket`, `HeadBucket`,
`GetBucketLocation`, `GetBucketVersioning` (retorna vazio), `ListObjects` (v1),
`ListObjectsV2`, `ListObjectVersions` (só versão `null`), `DeleteObjects` (lote).
Object: `PutObject` (incl. `aws-chunked` streaming), `GetObject` (Range +
condicionais), `HeadObject`, `DeleteObject`, `CopyObject`. Multipart:
`CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `ListParts`,
`ListMultipartUploads`, `CompleteMultipartUpload`, `AbortMultipartUpload`.
Auth: AWS SigV4 (Authorization header + URL presignada + streaming com
verificação de assinatura por chunk), credencial root única.

## O que **não** funciona ainda

Múltiplos server pools (M5) · cluster multi-nó / lock distribuído (M6) ·
healing proativo + scanner (M7) ·
IAM/policies/multi-usuário/service accounts (M8) · STS (M9) · versioning real,
object lock, lifecycle, tagging persistida, bucket policy, CORS enforcement
(M10 — endpoints hoje são *accept-and-ignore*) · SSE/KMS (M11) · notificações
(M12) · replicação (M13) · console web (M14 — hoje é uma página de status).

## Formato on-disk (M1–M3)

```
<volume>/<bucket>/<key>                              dados do objeto (espelha a key)
<volume>/.gostore.sys/format.json                    identidade + versão do disco
<volume>/.gostore.sys/buckets/<bucket>.json          metadata do bucket
<volume>/.gostore.sys/meta/<bucket>/<key>.json       sidecar do objeto (etag, content-type, x-amz-meta-*, parts)
<volume>/.gostore.sys/multipart/<bucket>/<uploadID>/ staging de multipart
<volume>/.gostore.sys/tmp/                            staging de escrita atômica (rename)
```

Escrita é atômica via `write-tmp + fsync + rename`. O layout `xl.meta`
(diretório por objeto, MessagePack) entra no M4.

## Testes

```bash
go test ./...
```

Cobre: CRUD de bucket/objeto, listagem com prefix/delimiter/paginação, multipart
round-trip + rejeição de part pequena, CopyObject, e no nível HTTP: fluxo S3
completo assinado com SigV4, Range, rejeição de assinatura inválida, e upload
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` com cadeia de assinatura por chunk.
