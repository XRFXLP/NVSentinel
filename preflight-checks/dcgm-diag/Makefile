# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# =============================================================================
# MODULE-SPECIFIC CONFIGURATION
# =============================================================================

# This is a Go module using go-dcgm bindings (requires CGO)
# Build happens inside Docker container with DCGM libraries
IS_GO_MODULE := 1
IS_DOCKER_MODULE := 1

CLEAN_EXTRA_FILES := dcgm-diag

# =============================================================================
# INCLUDE SHARED DEFINITIONS
# =============================================================================

include ../../make/common.mk
include ../../make/go.mk

# =============================================================================
# DOCKER CONFIGURATION
# =============================================================================

# Module path relative to repo root
MODULE_PATH := preflight-checks/dcgm-diag

# Docker buildx platforms
PLATFORMS ?= linux/amd64,linux/arm64

# Cache settings
ifndef DISABLE_REGISTRY_CACHE
CACHE_FROM_ARG := --cache-from=type=registry,ref=$(CONTAINER_REGISTRY)/$(CONTAINER_ORG)/nvsentinel-buildcache:preflight-dcgm-diag
CACHE_TO_ARG := --cache-to=type=registry,ref=$(CONTAINER_REGISTRY)/$(CONTAINER_ORG)/nvsentinel-buildcache:preflight-dcgm-diag,mode=max
else
CACHE_FROM_ARG :=
CACHE_TO_ARG :=
endif

# =============================================================================
# DEFAULT TARGET
# =============================================================================

.PHONY: all
all: lint ## Run lint (default target - full lint-test requires DCGM libs)

# =============================================================================
# OVERRIDE GO TARGETS (CGO requires DCGM libs - use Docker for full build)
# =============================================================================

# Override lint-test since we can't run full tests without DCGM libs
.PHONY: lint-test
lint-test: lint ## Run lint only (tests require DCGM libs, use docker-build)
	@echo "Note: Full tests require DCGM libs. Use 'make docker-build' for complete build."

.PHONY: ci-test
ci-test: lint ## CI test target (lint only - full build via docker)

# =============================================================================
# DOCKER TARGETS
# =============================================================================

.PHONY: setup-buildx
setup-buildx:
	@docker buildx inspect nvsentinel-builder > /dev/null 2>&1 || \
		docker buildx create --name nvsentinel-builder --use

.PHONY: docker-build
docker-build: setup-buildx ## Build Docker image locally
	@echo "Building Docker image for preflight-dcgm-diag (local)..."
	$(if $(filter true,$(DISABLE_REGISTRY_CACHE)),@echo "Registry cache disabled for this build")
	cd $(REPO_ROOT) && docker buildx build \
		--platform linux/amd64 \
		--network=host \
		$(CACHE_FROM_ARG) \
		$(CACHE_TO_ARG) \
		$(DOCKER_EXTRA_ARGS) \
		$(if $(DOCKER_LOAD_ARG),--load,) \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-f $(MODULE_PATH)/Dockerfile \
		-t preflight-dcgm-diag:$(SAFE_REF_NAME) \
		.

.PHONY: docker-publish
docker-publish: setup-buildx ## Build and publish Docker image
	@echo "Building and publishing Docker image for preflight-dcgm-diag (production)..."
	$(if $(filter true,$(DISABLE_REGISTRY_CACHE)),@echo "Registry cache disabled for this build")
	cd $(REPO_ROOT) && docker buildx build \
		--platform $(PLATFORMS) \
		--network=host \
		$(CACHE_FROM_ARG) \
		$(CACHE_TO_ARG) \
		$(DOCKER_EXTRA_ARGS) \
		$(if $(DOCKER_METADATA_FILE),--metadata-file $(DOCKER_METADATA_FILE)) \
		--push \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-f $(MODULE_PATH)/Dockerfile \
		-t $(CONTAINER_REGISTRY)/$(CONTAINER_ORG)/nvsentinel/preflight-dcgm-diag:$(SAFE_REF_NAME) \
		.

# =============================================================================
# MODULE HELP
# =============================================================================

.PHONY: help
help:
	@echo "preflight-checks/dcgm-diag Makefile (Go + go-dcgm bindings)"
	@echo ""
	@echo "This module uses go-dcgm bindings which require CGO and libdcgm.so."
	@echo "Build happens inside Docker container with DCGM 4.2.3+ libraries."
	@echo "The dcgmDiagResponse_version12 API was introduced in DCGM 4.2.3."
	@echo ""
	@echo "Main targets: all, lint, lint-test, ci-test, clean"
	@echo "Docker targets: docker-build, docker-publish"
	@echo ""
	@echo "Build notes:"
	@echo "  - Base image: nvcr.io/nvidia/cloud-native/dcgm:4.2.3-1-ubuntu22.04"
	@echo "  - Compatible with DCGM hostengines 4.2.3+"
	@echo "  - Use 'make docker-build' for local builds"
	@echo "  - Use 'make docker-publish' for CI/production builds"
