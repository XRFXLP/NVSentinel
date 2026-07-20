// storm-injector sends synthetic health events to platform-connector at a
// configurable rate, simulating a fault storm. It is designed to run as a
// DaemonSet on KWOK nodes that have platform-connectors but no real monitors.
//
// Baseline (prod 250 nodes, 7 days): 2M non-fatal + 22k fatal = 3.34 ev/s total
// Per-node rate: 0.0134 ev/s  |  non-fatal:fatal ratio: 91:1
//
// Storm multipliers:
//   1x   →  baseline (~0.013 ev/s/node)
//   10x  →  ~0.13 ev/s/node  — Events LIST starts binding
//   100x →  ~1.34 ev/s/node  — consumer throughput ceiling
//   300x →  ~4 ev/s/node     — etcd saturation range
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

// Production baseline: 0.0134 ev/s per node. STORM_MULTIPLIER scales this.
const baselineEventsPerSecond = 0.0134

// non-fatal:fatal ratio from prod = 91:1
const fatalRatio = 91

var (
	totalSent    atomic.Int64
	totalErrors  atomic.Int64
)

func main() {
	if err := run(); err != nil {
		slog.Error("Fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	nodeName := mustEnv("NODE_NAME")
	socketPath := envOrDefault("SOCKET_PATH", "/var/run/nvsentinel/nodeHealthEvents.sock")
	multiplier, err := strconv.ParseFloat(envOrDefault("STORM_MULTIPLIER", "1"), 64)
	if err != nil {
		return fmt.Errorf("invalid STORM_MULTIPLIER: %w", err)
	}

	// STORE_AND_ANALYSE by default — safe, tests MongoDB+consumers without cordoning.
	// Set PROCESSING_STRATEGY=EXECUTE_REMEDIATION to test full quarantine/drain pipeline.
	strategy := pb.ProcessingStrategy_STORE_AND_ANALYSE
	if os.Getenv("PROCESSING_STRATEGY") == "EXECUTE_REMEDIATION" {
		strategy = pb.ProcessingStrategy_EXECUTE_REMEDIATION
		slog.Warn("EXECUTE_REMEDIATION enabled — events will trigger real cordoning/draining")
	}

	eventsPerSecond := baselineEventsPerSecond * multiplier
	interval := time.Duration(float64(time.Second) / eventsPerSecond)

	slog.Info("Storm injector starting",
		"node", nodeName,
		"socket", socketPath,
		"multiplier", multiplier,
		"rate_per_sec", eventsPerSecond,
		"interval_ms", interval.Milliseconds(),
		"strategy", strategy,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(
		"unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to platform-connector: %w", err)
	}
	defer conn.Close()

	client := pb.NewPlatformConnectorClient(conn)

	// Stats reporter
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(start).Seconds()
				sent := totalSent.Load()
				errs := totalErrors.Load()
				actualRate := float64(sent) / elapsed
				slog.Info("Storm stats",
					"node", nodeName,
					"total_sent", sent,
					"total_errors", errs,
					"actual_ev_per_sec", fmt.Sprintf("%.3f", actualRate),
					"target_ev_per_sec", fmt.Sprintf("%.3f", eventsPerSecond),
				)
			}
		}
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	seq := 0
	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutting down", "total_sent", totalSent.Load(), "total_errors", totalErrors.Load())
			return nil
		case <-ticker.C:
			seq++
			isFatal := (seq % fatalRatio) == 0
			event := buildEvent(nodeName, seq, isFatal, strategy)
			sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := client.HealthEventOccurredV1(sendCtx, &pb.HealthEvents{
				Version: 1,
				Events:  []*pb.HealthEvent{event},
			})
			cancel()
			if err != nil {
				totalErrors.Add(1)
				slog.Warn("Failed to send event", "error", err, "seq", seq)
			} else {
				totalSent.Add(1)
			}
		}
	}
}

func buildEvent(nodeName string, seq int, isFatal bool, strategy pb.ProcessingStrategy) *pb.HealthEvent {
	checkNames := []string{
		"GpuXidError", "SysLogsXIDError", "GpuPowerWatch",
		"GpuMemWatch", "GpuNvlinkWatch", "GpuThermalWatch",
		"SysLogsSXIDError", "GpuDriverWatch",
	}
	checkName := checkNames[seq%len(checkNames)]

	gpuID := rand.Intn(8)
	event := &pb.HealthEvent{
		Version:            1,
		Agent:              "storm-injector",
		ComponentClass:     "GPU",
		CheckName:          checkName,
		NodeName:           nodeName,
		GeneratedTimestamp: timestamppb.Now(),
		ProcessingStrategy: strategy,
		IsFatal:            isFatal,
		IsHealthy:          false,
		EntitiesImpacted: []*pb.Entity{
			{EntityType: "GPU", EntityValue: fmt.Sprintf("GPU-%d-synthetic", gpuID)},
		},
	}

	if isFatal {
		event.Message = fmt.Sprintf("Synthetic fatal GPU fault on GPU %d (seq %d)", gpuID, seq)
		event.RecommendedAction = pb.RecommendedAction_RESTART_VM
		event.ErrorCode = []string{"SYNTHETIC_FATAL"}
	} else {
		event.Message = fmt.Sprintf("Synthetic non-fatal GPU event on GPU %d (seq %d)", gpuID, seq)
		event.RecommendedAction = pb.RecommendedAction_NONE
		event.ErrorCode = []string{"SYNTHETIC_NONFATAL"}
	}

	return event
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("Required env var not set", "key", key)
		os.Exit(1)
	}
	return v
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
