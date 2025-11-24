FROM public.ecr.aws/docker/library/golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go build -o mock-server main.go

FROM public.ecr.aws/docker/library/alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/mock-server .
EXPOSE 8443
CMD ["./mock-server"]

