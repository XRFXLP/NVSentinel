FROM nvidia/cuda:12.6.0-base-ubuntu22.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

ARG GOLANG_VERSION=1.25.3
RUN curl -fsSL https://go.dev/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz | tar -C /usr/local -xzf -
ENV PATH="/usr/local/go/bin:${PATH}"

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -a -ldflags="-s -w" -o metadata-collector main.go

FROM nvidia/cuda:12.6.0-base-ubuntu22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r nvsentinel && useradd -r -g nvsentinel nvsentinel

WORKDIR /app

COPY --from=builder /workspace/metadata-collector .

RUN chown nvsentinel:nvsentinel /app/metadata-collector

USER nvsentinel

ENTRYPOINT ["/app/metadata-collector"]


