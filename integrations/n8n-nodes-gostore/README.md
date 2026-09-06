# n8n-nodes-gostore

n8n community node for [**gostore**](https://github.com/fdsriopreto-code/gostore-s3) —
an S3-compatible object storage server.

> **You don't strictly need this node.** gostore speaks the S3 protocol, so
> n8n's built-in **S3** node works today: set *S3 Endpoint* to your gostore
> URL, *Region* `us-east-1`, *Force Path Style* on. Use this node when you want
> gostore-specific things as first-class operations: on-the-fly **image
> transforms**, **ingest-key** uploads (no SigV4), presigned URLs, and admin
> (**snapshots**, **run backup**, **folder usage**).

## Install

**n8n → Settings → Community Nodes → Install** → `n8n-nodes-gostore`

Or self-hosted:

```bash
cd ~/.n8n
npm install n8n-nodes-gostore
# restart n8n
```

## Credentials — *gostore API*

| Field | Example |
|---|---|
| Endpoint | `https://storage.example.com` — no trailing slash, **no `/gostore`** |
| Region | `us-east-1` |
| Access Key ID | `gk...` |
| Secret Access Key | `...` |

The credential test hits `GET /gostore/health/live` (unauthenticated) — a green
test means the endpoint is reachable, not that the keys are valid. Run an
*Object → List* to confirm the keys.

## Operations

### Object
| Operation | Notes |
|---|---|
| **Upload** | from a binary field. Options: Content-Type override, **Expire After** (`72h`/`7d`/`2w` → per-object TTL), Cache-Control, `x-amz-meta-*` metadata (JSON). Single PUT — for multi-GB files use the AWS S3 node's multipart. |
| **Download** | to a binary field |
| **Delete** | |
| **List** | prefix, delimiter (`/` → folder view via CommonPrefixes), Return All or Limit. Folders come back as `{ "folder": "prefix/" }` rows. |
| **Copy** | server-side (`x-amz-copy-source`) |
| **Get Metadata** | HEAD — returns the response headers |
| **Get Presigned URL** | GET or PUT, custom TTL. Signed link, share without credentials. |

### Image → Transform
Downloads a resized/cropped render via gostore's image pipeline:
`?w=&h=&fit=contain|cover&format=jpeg|png&q=`. Never upscales. Output is a
binary field.

### Ingest → Push
`PUT /gostore/ingest/{bucket}/{key}` with `Authorization: Bearer gik_…` — no
SigV4. The ingest key is prefix-scoped; a key ending in `/` auto-dates the
filename. Mint keys in the console (bucket Settings → Ingest keys).

### Admin
| Operation | Endpoint |
|---|---|
| **Create Snapshot** | `POST /gostore/admin/v1/snapshot?bucket=X` (bucket versioning required) |
| **Run Backup Now** | `POST /gostore/admin/v1/backup/run` |
| **Get Folder Usage** | `GET /gostore/admin/v1/du?bucket=X&prefix=P/` |
| **Get Data Usage** | `GET /gostore/admin/v1/datausage` |

Admin operations need a key with the `admin:*` action (or the root key).

## Example — avatar upload + thumbnail

1. **HTTP Request** (or a form trigger) → gets the image binary.
2. **gostore** · Object · Upload → `Bucket: app-prod`, `Key: avatars/{{$json.userId}}/original.webp`.
3. **gostore** · Image · Transform → same key, `w: 128`, `h: 128`, `fit: cover`, `format: jpeg` → thumbnail binary.
4. **gostore** · Object · Upload → `Key: avatars/{{$json.userId}}/thumb.jpg`.

## Build from source

```bash
cd integrations/n8n-nodes-gostore
npm install
npm run build            # -> dist/
```

Link into a local n8n for testing:

```bash
npm link
cd ~/.n8n && npm link n8n-nodes-gostore
```

## License

Apache-2.0, same as gostore.
