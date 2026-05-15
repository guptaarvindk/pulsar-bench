# Pulsar benchmark build system
BINARY    := pulsar
VERSION   := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS   := -ldflags "-X main.version=$(VERSION) -s -w"
BUILD_DIR := dist

.PHONY: all build release clean test lint linux-amd64 linux-arm64 darwin-arm64

all: build

## build: Build for the current platform
build:
	go build $(LDFLAGS) -o $(BINARY) .

## release: Build a fully static binary (Linux) with all optimisations
release:
	CGO_ENABLED=0 go build $(LDFLAGS) -trimpath -o $(BINARY) .

## dist: Cross-compile for all target platforms
dist: linux-amd64 linux-arm64 darwin-arm64
	@echo "Binaries written to $(BUILD_DIR)/"

linux-amd64:
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux  GOARCH=amd64 go build $(LDFLAGS) -trimpath -o $(BUILD_DIR)/pulsar-linux-amd64 .

linux-arm64:
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux  GOARCH=arm64 go build $(LDFLAGS) -trimpath -o $(BUILD_DIR)/pulsar-linux-arm64 .

darwin-arm64:
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -trimpath -o $(BUILD_DIR)/pulsar-darwin-arm64 .

## test: Run unit tests
test:
	go test ./...

## lint: Run linter
lint:
	go vet ./...

## clean: Remove build artefacts
clean:
	rm -f $(BINARY)
	rm -rf $(BUILD_DIR)

help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
