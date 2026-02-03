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

# Build stage - uses DCGM image as base because go-dcgm requires CGO and libdcgm.so
# The go-dcgm bindings use dcgmDiagResponse_version12 introduced in DCGM 4.2.3
ARG DCGM_VERSION="4.2.3-1-ubuntu22.04"
ARG GOLANG_VERSION="1.25.0"
ARG VERSION="dev"
ARG GIT_COMMIT="none"
ARG BUILD_DATE=$(date -u +%FT%TZ)

FROM nvcr.io/nvidia/cloud-native/dcgm:${DCGM_VERSION} AS builder

ARG GOLANG_VERSION

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    wget ca-certificates gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://go.dev/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go${GOLANG_VERSION}.linux-amd64.tar.gz \
    && rm go${GOLANG_VERSION}.linux-amd64.tar.gz

ENV PATH="/usr/local/go/bin:${PATH}" \
    GOPATH="/go" \
    CGO_ENABLED=1

WORKDIR /workspace

# Copy go.mod files first for better caching
COPY data-models/go.mod data-models/go.sum ./data-models/
COPY preflight-checks/dcgm-diag/go.mod preflight-checks/dcgm-diag/go.sum ./preflight-checks/dcgm-diag/

RUN --mount=type=cache,target=/go/pkg/mod \
    cd preflight-checks/dcgm-diag && go mod download

# Copy source code
COPY data-models/ ./data-models/
COPY preflight-checks/dcgm-diag/ ./preflight-checks/dcgm-diag/

ARG VERSION
ARG GIT_COMMIT
ARG BUILD_DATE

RUN cd preflight-checks/dcgm-diag && \
    go build -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${GIT_COMMIT} -X main.date=${BUILD_DATE}" \
    -o /dcgm-diag .

# Runtime stage - same DCGM version for libdcgm.so compatibility
FROM nvcr.io/nvidia/cloud-native/dcgm:${DCGM_VERSION}

RUN groupadd -r nvsentinel && useradd -r -g nvsentinel nvsentinel

COPY --from=builder /dcgm-diag /usr/local/bin/dcgm-diag

USER nvsentinel

ENTRYPOINT ["/usr/local/bin/dcgm-diag"]
