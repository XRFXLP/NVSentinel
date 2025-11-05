// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/nvidia/nvsentinel/commons/pkg/logger"
)

const (
	defaultAgentName = "metadata-collector"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	logger.SetDefaultStructuredLogger(defaultAgentName, version)
	slog.Info("Starting metadata-collector", "version", version, "commit", commit, "date", date)

	if err := run(); err != nil {
		slog.Error("Metadata collector failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ret := nvml.Init()
	if ret != nvml.SUCCESS {
		return fmt.Errorf("failed to initialize NVML: %v", nvml.ErrorString(ret))
	}

	defer func() {
		ret := nvml.Shutdown()
		if ret != nvml.SUCCESS {
			slog.Error("Failed to shutdown NVML", "error", nvml.ErrorString(ret))
		}
	}()

	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return fmt.Errorf("failed to get device count: %v", nvml.ErrorString(ret))
	}

	hostname, _ := os.Hostname()

	slog.Info("GPU metadata collection started", "node", hostname, "gpu_count", count)

	if nvmlVersion, ret := nvml.SystemGetNVMLVersion(); ret == nvml.SUCCESS {
		slog.Info("NVML version", "version", nvmlVersion)
	}

	fmt.Printf("\n=== GPU Metadata Collector ===\n")
	fmt.Printf("Node: %s\n", hostname)
	fmt.Printf("GPUs Found: %d\n", count)

	fmt.Println("\n=== GPU Details ===")

	for i := range count {
		device, ret := nvml.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			slog.Warn("Failed to get device", "gpu_id", i, "error", nvml.ErrorString(ret))
			continue
		}

		name, _ := device.GetName()
		uuid, _ := device.GetUUID()

		slog.Info("GPU discovered", "gpu_id", i, "name", name, "uuid", uuid)

		fmt.Printf("\nGPU %d:\n", i)
		fmt.Printf("  Name: %s\n", name)
		fmt.Printf("  UUID: %s\n", uuid)
	}

	slog.Info("Metadata collector hello world completed successfully")

	return nil
}
