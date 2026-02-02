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

# Build stage - uses DCGM 4.2.3+ image as base because go-dcgm requires CGO and libdcgm.so
# The go-dcgm bindings use dcgmDiagResponse_version12 which was introduced in DCGM 4.2.3.
# Building against 4.2.3+ should be compatible with hostengines 4.2.3 and newer.
ARG DCGM_VERSION=4.2.3-1-ubuntu22.04
ARG GOLANG_VERSION=1.25.0

FROM nvcr.io/nvidia/cloud-native/dcgm:${DCGM_VERSION} AS builder

ARG GOLANG_VERSION

# Install Go and build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://go.dev/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go${GOLANG_VERSION}.linux-amd64.tar.gz \
    && rm go${GOLANG_VERSION}.linux-amd64.tar.gz

ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"
ENV CGO_ENABLED=1

WORKDIR /workspace

# Copy go.mod files first for better caching
COPY data-models/go.mod data-models/go.sum ./data-models/
COPY preflight-checks/dcgm-diag/go.mod ./preflight-checks/dcgm-diag/

# Download dependencies
WORKDIR /workspace/preflight-checks/dcgm-diag
RUN go mod download

# Copy source code
WORKDIR /workspace
COPY data-models/ ./data-models/
COPY preflight-checks/dcgm-diag/ ./preflight-checks/dcgm-diag/

# Build the binary with CGO enabled (required for go-dcgm bindings)
WORKDIR /workspace/preflight-checks/dcgm-diag
ARG VERSION=dev
ARG GIT_COMMIT=none
ARG BUILD_DATE=unknown
RUN go build \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${GIT_COMMIT} -X main.date=${BUILD_DATE}" \
    -o /dcgm-diag .

# Runtime stage - same DCGM version to ensure library compatibility
# The binary is dynamically linked to libdcgm.so, so we need the same version at runtime
FROM nvcr.io/nvidia/cloud-native/dcgm:${DCGM_VERSION}

ARG VERSION
ARG GIT_COMMIT
ARG BUILD_DATE

LABEL org.opencontainers.image.source="https://github.com/nvidia/nvsentinel" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.title="NVSentinel Preflight DCGM Diag" \
      org.opencontainers.image.description="Preflight check for running DCGM diagnostics on GPUs" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}"

# Copy the Go binary
COPY --from=builder /dcgm-diag /usr/local/bin/dcgm-diag

# Create non-root user
RUN useradd -r -u 65532 -g nogroup preflight || true

USER 65532

ENTRYPOINT ["/usr/local/bin/dcgm-diag"]
