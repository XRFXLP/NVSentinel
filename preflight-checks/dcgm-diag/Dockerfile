# Preflight DCGM Diagnostic Checker
#
# This container runs DCGM diagnostics on allocated GPUs before
# the main workload starts.
#
# Build from repository root:
#   docker build -t preflight-dcgm-diag -f preflight-checks/dcgm-diag/Dockerfile .

# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git

# Copy only the modules we need (not go.work to avoid loading all modules)
COPY data-models/go.mod data-models/go.sum ./data-models/
COPY preflight-checks/dcgm-diag/go.mod preflight-checks/dcgm-diag/go.sum ./preflight-checks/dcgm-diag/

# Download dependencies (disable workspace mode)
WORKDIR /build/preflight-checks/dcgm-diag
ENV GOWORK=off
RUN go mod download

# Copy source code
WORKDIR /build
COPY data-models ./data-models
COPY preflight-checks/dcgm-diag ./preflight-checks/dcgm-diag

# Build the binary
WORKDIR /build/preflight-checks/dcgm-diag
RUN CGO_ENABLED=0 GOOS=linux GOWORK=off go build -ldflags="-s -w" -o /preflight-dcgm-diag .

# Runtime stage
# Use NVIDIA's DCGM image as base to get dcgmi CLI
FROM nvidia/dcgm:4.4.2-1-ubuntu22.04

# Copy our binary
COPY --from=builder /preflight-dcgm-diag /usr/local/bin/preflight-dcgm-diag

# Create non-root user
RUN useradd -r -u 65532 -g nogroup preflight

USER preflight

ENTRYPOINT ["/usr/local/bin/preflight-dcgm-diag"]
