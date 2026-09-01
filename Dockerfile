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

# Stage an empty data dir so the final COPY can set its ownership (distroless
# has no shell/chown to do it directly).
RUN mkdir -p /out/data

# ---- runtime stage ----
FROM gcr.io/distroless/static-debian12:nonroot

# distroless "nonroot" runs as uid:gid 65532:65532.
COPY --from=build /out/gostore /usr/local/bin/gostore
COPY --from=build --chown=65532:65532 /out/data /data

USER 65532:65532

# S3 API + web console.
EXPOSE 9000 9001
VOLUME ["/data"]

ENV GOSTORE_ADDRESS=":9000" \
    GOSTORE_CONSOLE_ADDRESS=":9001"

ENTRYPOINT ["/usr/local/bin/gostore"]
CMD ["server", "/data"]
