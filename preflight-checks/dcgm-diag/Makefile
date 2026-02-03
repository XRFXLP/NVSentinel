# Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
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

IS_GO_MODULE := 0
HAS_DOCKER := 1

PYTHON_PACKAGE_NAME := dcgm_diag

include ../../make/common.mk
include ../../make/python.mk
include ../../make/docker.mk

DCGM_VERSION ?= 4.2.3-1-ubuntu22.04
IMAGE_NAME := preflight-dcgm-diag

.PHONY: all
all: lint-test

.PHONY: setup
setup:
	@echo "Setting up Poetry environment for $(MODULE_NAME)..."
	poetry config virtualenvs.in-project true
	poetry install

docker-build: setup-buildx
	@echo "Building Docker image for $(MODULE_NAME)..."
	cd $(REPO_ROOT) && docker buildx build \
		-f $(MODULE_PATH)/Dockerfile \
		-t $(CONTAINER_REGISTRY)/$(CONTAINER_ORG)/nvsentinel/$(IMAGE_NAME):$(SAFE_REF_NAME) \
		--build-arg DCGM_VERSION=$(DCGM_VERSION) \
		--network=host \
		$(DOCKER_LOAD_ARG) \
		.

docker-publish: setup-buildx
	@echo "Publishing Docker image for $(MODULE_NAME)..."
	cd $(REPO_ROOT) && docker buildx build \
		-f $(MODULE_PATH)/Dockerfile \
		-t $(CONTAINER_REGISTRY)/$(CONTAINER_ORG)/nvsentinel/$(IMAGE_NAME):$(SAFE_REF_NAME) \
		--build-arg DCGM_VERSION=$(DCGM_VERSION) \
		--network=host \
		--push \
		.
