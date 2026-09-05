# Contribuindo

## Setup

```bash
git clone https://github.com/fdsriopreto-code/gostore
cd gostore
go build ./...
go test ./...
```

Sem dependências de sistema além do Go (versão em `go.mod`). Sem geração de
código, sem `make` obrigatório.

## Regras

- **Todo PR passa em `go vet ./...` e `go test -race ./...`.** A CI roda os dois.
- Recurso novo vem com teste. O padrão do repo é teste de comportamento
  (round-trip de verdade contra o backend `fs` ou um `erasure.Pool` in-memory),
  não mock.
- Sem dependência nova sem discussão prévia numa issue. O projeto é
  deliberadamente enxuto (5 módulos diretos).
- Mantenha o estilo do arquivo em volta: densidade de comentário, nomes, idioma.
- Nada de quebrar o formato on-disk sem um caminho de leitura legado + teste.

## Layout

Veja a seção **Arquitetura** do [README](README.md). Resumo:

- `internal/api/` — camada HTTP S3 + admin
- `internal/object/fs/` — backend single-disk
- `internal/erasure/` — backend erasure coding
- `internal/iam/`, `internal/auth/` — identidade e assinatura
- `internal/console/assets/` — SPA (edite `app.js` / `index.html`, valide com
  `node --check`)

## Áreas abertas

Veja **Estado do projeto → Parcial / não feito** no README: grupos IAM,
SSE-KMS/SSE-C, OIDC/LDAP STS, rebalanceamento de cluster, fila de retry
persistente na replicação.
