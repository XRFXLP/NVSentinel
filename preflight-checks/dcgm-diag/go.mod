module github.com/nvidia/nvsentinel/preflight-checks/dcgm-diag

go 1.25.0

require (
	github.com/NVIDIA/go-dcgm v0.0.0-20260115225648-6cbb0463ce9f
	github.com/NVIDIA/go-nvml v0.12.4-1
	github.com/nvidia/nvsentinel/data-models v0.0.0
	google.golang.org/grpc v1.78.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/bits-and-blooms/bitset v1.22.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	go.opentelemetry.io/otel v1.39.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.39.0 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120174246-409b4a993575 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/nvidia/nvsentinel/data-models => ../../data-models
