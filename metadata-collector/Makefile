# Metadata Collector Makefile (Bootstrap)
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.

GO := go

.PHONY: all
all: build

.PHONY: build
build:
	@echo "Building metadata-collector with CGO..."
	CGO_ENABLED=1 $(GO) build .

.PHONY: test
test:
	@echo "Running tests..."
	$(GO) test -v ./...

.PHONY: clean
clean:
	@echo "Cleaning..."
	$(GO) clean
	rm -f metadata-collector

DOCKER_IMAGE := ghcr.io/nvidia/nvsentinel/metadata-collector
DOCKER_TAG := latest

.PHONY: docker-build
docker-build:
	@echo "Building Docker image $(DOCKER_IMAGE):$(DOCKER_TAG)..."
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

.PHONY: docker-publish
docker-publish:
	@echo "Building and publishing Docker image $(DOCKER_IMAGE):$(DOCKER_TAG)..."
	docker buildx build --platform linux/amd64,linux/arm64 --push -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

.PHONY: help
help:
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'


