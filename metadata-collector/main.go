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
