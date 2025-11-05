module github.com/nvidia/nvsentinel/metadata-collector

go 1.25

toolchain go1.25.3

require (
	github.com/NVIDIA/go-nvml v0.13.0-1
	github.com/nvidia/nvsentinel/commons v0.0.0
)

replace github.com/nvidia/nvsentinel/commons => ../commons
