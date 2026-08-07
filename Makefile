BINARY_NAME = event-service
BUILD_DIR = bin
VERSION = $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GOTOOLCHAIN = go1.25.7
LDFLAGS = -ldflags "-w -s -X main.version=$(VERSION)"

.PHONY: build build-arm build-host dist test lint fmt deps clean

build:
	mkdir -p $(BUILD_DIR)
	GOTOOLCHAIN=$(GOTOOLCHAIN) CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 \
		go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/event-service

build-arm: build

build-host:
	mkdir -p $(BUILD_DIR)
	GOTOOLCHAIN=$(GOTOOLCHAIN) go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-host ./cmd/event-service

dist: build

test:
	GOTOOLCHAIN=$(GOTOOLCHAIN) go test ./...

lint:
	golangci-lint run

fmt:
	GOTOOLCHAIN=$(GOTOOLCHAIN) go fmt ./...

deps:
	GOTOOLCHAIN=$(GOTOOLCHAIN) go mod download
	GOTOOLCHAIN=$(GOTOOLCHAIN) go mod tidy

clean:
	rm -rf $(BUILD_DIR)
