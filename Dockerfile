# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.27 AS build
WORKDIR /src

# Cache modules first.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/gostore ./cmd/gostore

RUN mkdir -p /out/data

# ---- runtime stage ----
# Runs as root so it can write to whatever /data mount the platform provides
# (EasyPanel / Coolify / plain bind mounts are root-owned). This matches the
# official MinIO image. For multi-tenant hosts, override with `--user` and
# pre-chown the volume.
FROM gcr.io/distroless/static-debian12

COPY --from=build /out/gostore /usr/local/bin/gostore
COPY --from=build /out/data /data

# S3 API + web console.
EXPOSE 9000 9001
VOLUME ["/data"]

ENV GOSTORE_ADDRESS=":9000" \
    GOSTORE_CONSOLE_ADDRESS=":9001"

ENTRYPOINT ["/usr/local/bin/gostore"]
CMD ["server", "/data"]
