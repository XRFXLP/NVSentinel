FROM public.ecr.aws/docker/library/golang:1.26.3-trixie AS builder

WORKDIR /go/src/nvsentinel

COPY tools/storm-injector/go.mod tools/storm-injector/go.sum tools/storm-injector/
COPY data-models/go.mod data-models/go.sum ./data-models/

RUN cd tools/storm-injector && go mod download

COPY tools/storm-injector/ tools/storm-injector/
COPY data-models/ data-models/

RUN cd tools/storm-injector && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o storm-injector main.go

FROM gcr.io/distroless/static-debian13

COPY --from=builder /go/src/nvsentinel/tools/storm-injector/storm-injector /storm-injector

ENTRYPOINT ["/storm-injector"]
