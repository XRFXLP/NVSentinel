# The Dockerfile copies a prebuilt binary rather than building from source, so
# `docker build` on its own will silently ship whatever binary happens to be in
# the directory. That produced an image with a byte-identical digest to the
# previous one after a source change, and the stale build was deployed and
# measured before anyone noticed. Always go through this target.
IMAGE ?= docker.io/xrfxlp/mongo-connection-pool
TAG   ?= mcp-$(shell date +%Y%m%d%H%M)

.PHONY: binary image push
binary:
	GOWORK=off CGO_ENABLED=0 GOOS=linux go build -o mongo-connection-pool .

image: binary
	docker build -t $(IMAGE):$(TAG) .
	@echo "built $(IMAGE):$(TAG)"

push: image
	docker push $(IMAGE):$(TAG)
