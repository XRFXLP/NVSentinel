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

IS_DOCKER_MODULE := 1

include ../../make/common.mk

DCGM_VERSION ?= 4.2.3-1-ubuntu22.04
IMAGE_NAME := preflight-dcgm-diag
IMAGE_TAG ?= $(VERSION)
FULL_IMAGE := $(CONTAINER_REGISTRY)/$(CONTAINER_ORG)/nvsentinel/$(IMAGE_NAME)

.PHONY: lint-test build test lint clean docker-build docker-publish

lint-test: lint test

build:
	cd $(REPO_ROOT) && poetry -C $(MODULE_PATH) install

test:
	cd $(REPO_ROOT) && poetry -C $(MODULE_PATH) run pytest $(MODULE_PATH)

lint:
	cd $(REPO_ROOT) && poetry -C $(MODULE_PATH) run ruff check $(MODULE_PATH)/dcgm_diag
	cd $(REPO_ROOT) && poetry -C $(MODULE_PATH) run black --check $(MODULE_PATH)/dcgm_diag

clean:
	rm -rf .pytest_cache
	rm -rf dcgm_diag/__pycache__
	rm -rf dist

docker-build:
	cd $(REPO_ROOT) && docker buildx build \
		-f $(MODULE_PATH)/Dockerfile \
		-t $(IMAGE_NAME):$(IMAGE_TAG) \
		--build-arg DCGM_VERSION=$(DCGM_VERSION) \
		--network=host \
		--load \
		.

docker-publish:
	cd $(REPO_ROOT) && docker buildx build \
		-f $(MODULE_PATH)/Dockerfile \
		-t $(FULL_IMAGE):$(IMAGE_TAG) \
		--build-arg DCGM_VERSION=$(DCGM_VERSION) \
		--network=host \
		--push \
		.
