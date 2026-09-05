# Security Policy

## Versões suportadas

O projeto ainda está em desenvolvimento ativo. Correções de segurança vão
sempre para a branch `main` e para a release estável mais recente.

## Reportando uma vulnerabilidade

**Não abra uma issue pública para vulnerabilidades.**

Use o canal privado do GitHub Security Advisories:
**Security → Report a vulnerability** neste repositório.

Inclua, se possível:

- versão / commit do gostore
- modo (single-disk, erasure, cluster) e configuração relevante
- passo a passo de reprodução
- impacto (leitura não autorizada, escrita, DoS, bypass de assinatura, etc.)

Resposta inicial em até 5 dias úteis. Vamos coordenar uma data de divulgação
depois que houver correção.

## Escopo

Em escopo: bypass de autenticação/assinatura SigV4, escalonamento de
privilégio no IAM, bypass de bucket policy / Object Lock, path traversal,
leitura de objeto de outro tenant, corrupção silenciosa de dado, exposição de
credencial em log ou resposta.

Fora de escopo: uso de `GOSTORE_ALLOW_ANONYMOUS=1` (documentado como
inseguro), rodar o tráfego de cluster (`/gostore/internal/`) em rede não
confiável sem mTLS (documentado), `/gostore/metrics` aberto sem
`GOSTORE_METRICS_TOKEN`.
