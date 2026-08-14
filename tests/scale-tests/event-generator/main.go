/*
Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

func main() {
	var (
		socketPath         = flag.String("socket", "/var/run/nvsentinel/nvsentinel.sock", "Platform connector socket path")
		backgroundEnabled  = flag.Bool("background", false, "Enable background event generation")
		backgroundInterval = flag.Duration("interval", 15*time.Second, "Background event interval (only used if -background and EVENT_RATE not set)")
	)
	flag.Parse()

	// Get node name from environment (required)
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		log.Fatal("NODE_NAME environment variable is required")
	}

	// Check for EVENT_RATE environment variable (takes precedence)
	eventRateStr := os.Getenv("EVENT_RATE")
	var eventRate float64
	var continuousMode bool

	if eventRateStr != "" {
		// Continuous generation mode (scale testing)
		var err error
		eventRate, err = strconv.ParseFloat(eventRateStr, 64)
		if err != nil {
			log.Fatalf("Invalid EVENT_RATE value '%s': %v", eventRateStr, err)
		}
		if eventRate <= 0 {
			log.Fatalf("EVENT_RATE must be > 0, got %f", eventRate)
		}
		continuousMode = true
		log.Printf("Starting NVSentinel Event Generator - Continuous Mode")
		log.Printf("Configuration:")
		log.Printf("  Node: %s", nodeName)
		log.Printf("  Mode: Continuous (EVENT_RATE=%.2f events/sec)", eventRate)
		log.Printf("  Socket: %s", *socketPath)
	} else if *backgroundEnabled {
		// Background mode (latency testing)
		continuousMode = false
		log.Printf("Starting NVSentinel Event Generator - Dual Mode")
		log.Printf("Configuration:")
		log.Printf("  Node: %s", nodeName)
		log.Printf("  Mode: Background + On-Demand (SIGUSR1)")
		log.Printf("  Socket: %s", *socketPath)
		log.Printf("  Background Interval: %v", *backgroundInterval)
	} else {
		// On-demand only
		continuousMode = false
		log.Printf("Starting NVSentinel Event Generator - On-Demand Mode")
		log.Printf("Configuration:")
		log.Printf("  Node: %s", nodeName)
		log.Printf("  Mode: On-Demand (SIGUSR1 only)")
		log.Printf("  Socket: %s", *socketPath)
	}

	// Setup gRPC connection to local platform connector
	log.Printf("Connecting to Unix socket: %s", *socketPath)
	conn, err := grpc.NewClient(
		"unix://"+*socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect to Unix socket: %v", err)
	}
	defer conn.Close()

	client := pb.NewPlatformConnectorClient(conn)
	log.Printf("Connected to platform connector")

	// Setup context for gRPC calls
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if continuousMode {
		// Continuous generation mode (scale testing)
		log.Printf("🔄 Starting continuous event generation at %.2f events/sec", eventRate)
		continuousEventLoop(ctx, client, nodeName, eventRate)
	} else {
		// Dual mode or On-demand only (latency testing)
		// Setup signal handling for SIGUSR1 (trigger on-demand fatal event)
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGUSR1)

		// Start background event generation if enabled
		if *backgroundEnabled {
			log.Printf("🔄 Starting background event generation (interval: %v)", *backgroundInterval)
			go backgroundEventLoop(ctx, client, nodeName, *backgroundInterval)
		}

		log.Printf("✅ Ready to receive SIGUSR1 signals for fatal event generation")

		// Wait for SIGUSR1 signals
		for sig := range sigChan {
			log.Printf("🚨 Signal received: %v - sending fatal GPU XID event", sig)
			fatalEvent := generateFatalGpuXidEventForFQM(nodeName) // Uses gpu-health-monitor agent
			success := sendHealthEvent(ctx, client, fatalEvent)
			if success {
				log.Printf("✅ Fatal event sent successfully for latency test")
			} else {
				log.Printf("❌ Failed to send fatal event")
			}
		}
	}
}

// flappyChecks defines the check names that cycle healthy↔unhealthy.
// Each unique check name maps to a separate node condition, so N checks = N condition changes per flip.
var flappyChecks = []struct {
	checkName      string
	componentClass string
	errorCode      string
}{
	{"GpuXidError", "GPU", "79"},
	{"GpuMemoryError", "GPU", "74"},
	{"GpuNvlinkWatch", "GPU", "12"},
	{"GpuPowerError", "GPU", "48"},
	{"NVSwitchHealth", "NVSwitch", ""},
	{"GpuThermalError", "GPU", "94"},
	{"GpuDriverError", "GPU", "61"},
	{"GpuEccError", "GPU", "63"},
}

func generateFlappyEvent(nodeName string, counter int) *pb.HealthEvent {
	// Cycle through check names and alternate healthy/unhealthy
	// Even counter = unhealthy, odd counter = healthy for this check
	check := flappyChecks[counter%len(flappyChecks)]
	isHealthy := (counter/len(flappyChecks))%2 == 1

	if isHealthy {
		return &pb.HealthEvent{
			Version:            1,
			Agent:              "event-generator",
			ComponentClass:     check.componentClass,
			CheckName:          check.checkName,
			IsFatal:            false,
			IsHealthy:          true,
			Message:            check.checkName + " recovered",
			RecommendedAction:  pb.RecommendedAction_NONE,
			NodeName:           nodeName,
			GeneratedTimestamp: timestamppb.Now(),
		}
	}
	errCodes := []string{}
	if check.errorCode != "" {
		errCodes = []string{check.errorCode}
	}
	return &pb.HealthEvent{
		Version:            1,
		Agent:              "event-generator",
		ComponentClass:     check.componentClass,
		CheckName:          check.checkName,
		IsFatal:            false, // non-fatal to avoid FQ cordoning
		IsHealthy:          false,
		Message:            check.checkName + " degraded",
		RecommendedAction:  pb.RecommendedAction_NONE,
		ErrorCode:          errCodes,
		NodeName:           nodeName,
		GeneratedTimestamp: timestamppb.Now(),
	}
}

func continuousEventLoop(ctx context.Context, client pb.PlatformConnectorClient, nodeName string, eventsPerSecond float64) {
	// Calculate interval between events
	intervalNs := int64(float64(time.Second) / eventsPerSecond)
	interval := time.Duration(intervalNs)

	rand.Seed(time.Now().UnixNano())

	flappyMode := os.Getenv("FLAPPY_MODE") == "true"
	if flappyMode {
		log.Printf("🔀 FLAPPY_MODE enabled: alternating healthy↔unhealthy across %d check names", len(flappyChecks))
	}
	log.Printf("Continuous mode: Generating events every %v", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	statsInterval := 100 // Log stats every 100 events
	eventCount := 0
	successCount := 0
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Continuous event loop stopped")
			return
		case <-ticker.C:
			var event *pb.HealthEvent

			if flappyMode {
				// Flappy mode: cycle through checks, alternating healthy/unhealthy
				// Every event triggers a real condition CHANGE → UpdateStatus fires every time
				event = generateFlappyEvent(nodeName, eventCount)
			} else {
				// Original weighted random distribution
				eventType := rand.Intn(125)
				if eventType < 80 {
					event = generateHealthyGpuEvent(nodeName)
				} else if eventType < 110 {
					event = generateSystemInfoEvent(nodeName)
				} else if eventType < 120 {
					event = generateFatalGpuXidEvent(nodeName)
				} else {
					event = generateNVSwitchWarningEvent(nodeName)
				}
			}

			success := sendHealthEvent(ctx, client, event)
			eventCount++
			if success {
				successCount++
			}

			// Log statistics periodically
			if eventCount%statsInterval == 0 {
				elapsed := time.Since(startTime)
				actualRate := float64(eventCount) / elapsed.Seconds()
				successRate := float64(successCount) / float64(eventCount) * 100
				log.Printf("📊 Stats: %d events sent (%.1f events/sec), %.1f%% success rate",
					eventCount, actualRate, successRate)
			}
		}
	}
}

func backgroundEventLoop(ctx context.Context, client pb.PlatformConnectorClient, nodeName string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	rand.Seed(time.Now().UnixNano())

	log.Printf("Background event loop started")

	for {
		select {
		case <-ctx.Done():
			log.Printf("Background event loop stopped")
			return
		case <-ticker.C:
			// Generate background event with weighted random selection
			// For background mode: mostly healthy events
			eventType := rand.Intn(100)

			var event *pb.HealthEvent
			if eventType < 50 {
				// 50% chance: Healthy GPU event
				event = generateHealthyGpuEvent(nodeName)
			} else {
				// 50% chance: System info event
				event = generateSystemInfoEvent(nodeName)
			}

			sendHealthEvent(ctx, client, event)
		}
	}
}

func generateHealthyGpuEvent(nodeName string) *pb.HealthEvent {
	return &pb.HealthEvent{
		Version:            1,
		Agent:              "event-generator",
		ComponentClass:     "GPU",
		CheckName:          "GpuHealth",
		IsFatal:            false,
		IsHealthy:          true,
		Message:            "GPU operating normally",
		RecommendedAction:  pb.RecommendedAction_NONE,
		EntitiesImpacted:   []*pb.Entity{{EntityType: "gpu", EntityValue: "0"}},
		NodeName:           nodeName,
		GeneratedTimestamp: timestamppb.Now(),
	}
}

func generateSystemInfoEvent(nodeName string) *pb.HealthEvent {
	return &pb.HealthEvent{
		Version:            1,
		Agent:              "event-generator",
		ComponentClass:     "System",
		CheckName:          "SystemInfo",
		IsFatal:            false,
		IsHealthy:          true,
		Message:            "System heartbeat",
		RecommendedAction:  pb.RecommendedAction_NONE,
		NodeName:           nodeName,
		GeneratedTimestamp: timestamppb.Now(),
	}
}

// generateFatalGpuXidEvent - for continuous mode (API/MongoDB tests)
// Uses agent="event-generator" so FQM IGNORES it (no accidental cordons)
func generateFatalGpuXidEvent(nodeName string) *pb.HealthEvent {
	return &pb.HealthEvent{
		Version:            1,
		Agent:              "event-generator", // FQM ignores - won't cordon
		ComponentClass:     "GPU",
		CheckName:          "GpuXidError",
		IsFatal:            true,
		IsHealthy:          false,
		Message:            "XID 79 - GPU has fallen off the bus",
		RecommendedAction:  pb.RecommendedAction_COMPONENT_RESET,
		ErrorCode:          []string{"79"},
		EntitiesImpacted:   []*pb.Entity{{EntityType: "gpu", EntityValue: "0"}},
		NodeName:           nodeName,
		GeneratedTimestamp: timestamppb.Now(),
	}
}

// generateFatalGpuXidEventForFQM - for SIGUSR1 on-demand mode (FQM latency tests)
// Uses agent="gpu-health-monitor" so FQM MATCHES and CORDONS the node
func generateFatalGpuXidEventForFQM(nodeName string) *pb.HealthEvent {
	return &pb.HealthEvent{
		Version:            1,
		Agent:              "gpu-health-monitor", // FQM matches - WILL cordon!
		ComponentClass:     "GPU",
		CheckName:          "GpuXidError",
		IsFatal:            true,
		IsHealthy:          false,
		Message:            "XID 79 - GPU has fallen off the bus",
		RecommendedAction:  pb.RecommendedAction_COMPONENT_RESET,
		ErrorCode:          []string{"79"},
		EntitiesImpacted:   []*pb.Entity{{EntityType: "gpu", EntityValue: "0"}},
		NodeName:           nodeName,
		GeneratedTimestamp: timestamppb.Now(),
	}
}

func generateNVSwitchWarningEvent(nodeName string) *pb.HealthEvent {
	return &pb.HealthEvent{
		Version:            1,
		Agent:              "event-generator",
		ComponentClass:     "NVSwitch",
		CheckName:          "NVSwitchHealth",
		IsFatal:            false,
		IsHealthy:          false,
		Message:            "NVSwitch minor error detected",
		RecommendedAction:  pb.RecommendedAction_NONE,
		EntitiesImpacted:   []*pb.Entity{{EntityType: "nvswitch", EntityValue: "0"}},
		NodeName:           nodeName,
		GeneratedTimestamp: timestamppb.Now(),
	}
}

func sendHealthEvent(ctx context.Context, client pb.PlatformConnectorClient, event *pb.HealthEvent) bool {
	healthEvents := &pb.HealthEvents{
		Version: 1,
		Events:  []*pb.HealthEvent{event},
	}

	start := time.Now()
	_, err := client.HealthEventOccurredV1(ctx, healthEvents)
	responseTime := time.Since(start)

	if err != nil {
		log.Printf("❌ gRPC Error: %v (took %v)", err, responseTime)
		return false
	}

	// Only log successful sends occasionally to reduce noise
	if rand.Intn(100) == 0 {
		log.Printf("✅ Event sent successfully (took %v)", responseTime)
	}

	return true
}
