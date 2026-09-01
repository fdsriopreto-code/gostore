VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GOBIN   ?= $(CURDIR)/dist

.PHONY: build build-linux test vet run docker clean tidy

build:
	go build -trimpath -ldflags "$(LDFLAGS)" -o $(GOBIN)/gostore ./cmd/gostore

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "$(LDFLAGS)" -o $(GOBIN)/gostore-linux-amd64 ./cmd/gostore

test:
	go test ./...

vet:
	go vet ./...

run: build
	$(GOBIN)/gostore server --address :9000 --console-address :9001 ./data/disk1

docker:
	docker build --build-arg VERSION=$(VERSION) -t gostore:$(VERSION) -t gostore:latest .

tidy:
	go mod tidy

clean:
	rm -rf $(GOBIN) ./data
