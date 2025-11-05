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
	"log"
	"os"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
)

func main() {
	log.Println("Starting GPU Metadata Collector (Hello World)")

	ret := nvml.Init()
	if ret != nvml.SUCCESS {
		log.Fatalf("Failed to initialize NVML: %v", nvml.ErrorString(ret))
	}
	defer func() {
		ret := nvml.Shutdown()
		if ret != nvml.SUCCESS {
			log.Printf("Failed to shutdown NVML: %v", nvml.ErrorString(ret))
		}
	}()

	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		log.Fatalf("Failed to get device count: %v", nvml.ErrorString(ret))
	}

	hostname, _ := os.Hostname()

	fmt.Printf("\n=== GPU Metadata Collector ===\n")
	fmt.Printf("Node: %s\n", hostname)
	fmt.Printf("GPUs Found: %d\n", count)
	fmt.Printf("NVML Version: ")
	if version, ret := nvml.SystemGetNVMLVersion(); ret == nvml.SUCCESS {
		fmt.Printf("%s\n", version)
	} else {
		fmt.Printf("Unknown\n")
	}

	fmt.Println("\n=== GPU Details ===")
	for i := range count {
		device, ret := nvml.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			log.Printf("Failed to get device %d: %v", i, nvml.ErrorString(ret))
			continue
		}

		name, _ := device.GetName()
		uuid, _ := device.GetUUID()

		fmt.Printf("\nGPU %d:\n", i)
		fmt.Printf("  Name: %s\n", name)
		fmt.Printf("  UUID: %s\n", uuid)
	}

	fmt.Println("\n✅ Metadata collector hello world successful!")
}
