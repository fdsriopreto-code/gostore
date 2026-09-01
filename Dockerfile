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

# ---- runtime stage ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gostore /usr/local/bin/gostore

# S3 API + web console.
EXPOSE 9000 9001
VOLUME ["/data"]

# Override at runtime: GOSTORE_ROOT_USER / GOSTORE_ROOT_PASSWORD, etc.
ENV GOSTORE_ADDRESS=":9000" \
    GOSTORE_CONSOLE_ADDRESS=":9001"

ENTRYPOINT ["/usr/local/bin/gostore"]
CMD ["server", "/data"]
