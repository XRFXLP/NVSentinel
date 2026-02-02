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

	"github.com/NVIDIA/go-dcgm/pkg/dcgm"
	"github.com/NVIDIA/go-nvml/pkg/nvml"
)

// gpuIndexToUUID maps GPU index to UUID, populated during GPU discovery.
var gpuIndexToUUID = make(map[uint]string)

// getGPUGroup returns a DCGM group containing only the GPUs allocated to this container.
// Uses go-nvml to discover allocated GPUs (respects NVIDIA_VISIBLE_DEVICES automatically).
func getGPUGroup() (dcgm.GroupHandle, func(), error) {
	gpuIndices, err := getAllocatedGPUs()
	if err != nil {
		return dcgm.GroupHandle{}, nil, fmt.Errorf("failed to discover GPUs via NVML: %w", err)
	}

	if len(gpuIndices) == 0 {
		return dcgm.GroupHandle{}, nil, fmt.Errorf("no GPUs allocated to this container")
	}

	slog.Info("Checking allocated GPUs", "count", len(gpuIndices), "indices", gpuIndices)

	return createGPUGroup(gpuIndices)
}

// getAllocatedGPUs uses go-nvml to get GPU indices and UUIDs visible to this container.
// NVML respects NVIDIA_VISIBLE_DEVICES, so it only returns GPUs allocated to this container.
// It populates the gpuIndexToUUID map for later use in health reporting.
func getAllocatedGPUs() ([]uint, error) {
	ret := nvml.Init()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("failed to initialize NVML: %v", nvml.ErrorString(ret))
	}

	defer func() {
		if ret := nvml.Shutdown(); ret != nvml.SUCCESS {
			slog.Warn("Failed to shutdown NVML", "error", nvml.ErrorString(ret))
		}
	}()

	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("failed to get device count: %v", nvml.ErrorString(ret))
	}

	var indices []uint

	for i := range count {
		device, ret := nvml.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			slog.Warn("Failed to get device handle", "index", i, "error", nvml.ErrorString(ret))

			continue
		}

		uuid, ret := device.GetUUID()
		if ret != nvml.SUCCESS {
			slog.Warn("Failed to get device UUID", "index", i, "error", nvml.ErrorString(ret))

			continue
		}

		// #nosec G115 -- i is always non-negative from DeviceGetCount
		idx := uint(i)

		indices = append(indices, idx)
		gpuIndexToUUID[idx] = uuid

		slog.Debug("Discovered GPU", "index", idx, "uuid", uuid)
	}

	return indices, nil
}

// GetGPUUUID returns the UUID for a GPU index.
// Returns error if UUID is not found (indicates GPU discovery issue).
func GetGPUUUID(gpuIndex uint) (string, error) {
	if uuid, ok := gpuIndexToUUID[gpuIndex]; ok {
		return uuid, nil
	}

	return "", fmt.Errorf("GPU UUID not found for index %d", gpuIndex)
}

// createGPUGroup creates a DCGM group with the specified GPU indices.
func createGPUGroup(gpuIndices []uint) (dcgm.GroupHandle, func(), error) {
	groupName := fmt.Sprintf("preflight-%d", os.Getpid())

	group, err := dcgm.CreateGroup(groupName)
	if err != nil {
		return dcgm.GroupHandle{}, nil, fmt.Errorf("failed to create DCGM group: %w", err)
	}

	cleanup := func() {
		if destroyErr := dcgm.DestroyGroup(group); destroyErr != nil {
			slog.Warn("Failed to destroy DCGM group", "error", destroyErr)
		}
	}

	for _, idx := range gpuIndices {
		if addErr := dcgm.AddToGroup(group, idx); addErr != nil {
			cleanup()

			return dcgm.GroupHandle{}, nil, fmt.Errorf("failed to add GPU %d to group: %w", idx, addErr)
		}
	}

	return group, cleanup, nil
}
