// Preflight DCGM Diagnostic Checker
//
// This init container runs DCGM diagnostics on allocated GPUs before
// the main workload starts. It detects hardware issues early and reports
// them to the Platform Connector for remediation.
//
// Exit codes:
//   - 0: All checks passed
//   - 1: Check failed (hardware issue detected)
//   - 2: Configuration error
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NVIDIA/NVSentinel/preflight-checks/dcgm-diag/pkg/dcgm"
	"github.com/NVIDIA/NVSentinel/preflight-checks/dcgm-diag/pkg/reporter"
)

const (
	exitSuccess     = 0
	exitCheckFailed = 1
	exitConfigError = 2
)

func main() {
	var (
		diagLevel             int
		hostengineAddr        string
		platformConnectorSock string
		timeout               time.Duration
		skipHealthEvent       bool
	)

	flag.IntVar(&diagLevel, "diag-level", 2, "DCGM diagnostic level (1=quick ~30s, 2=extended ~2-3min, 3=full)")
	flag.StringVar(&hostengineAddr, "hostengine-addr", "", "DCGM hostengine address (e.g., dcgm-hostengine.nvsentinel.svc:5555)")
	flag.StringVar(&platformConnectorSock, "platform-connector-socket", "/var/run/nvsentinel/nvsentinel.sock", "Platform Connector Unix socket path")
	flag.DurationVar(&timeout, "timeout", 10*time.Minute, "Overall timeout for diagnostic")
	flag.BoolVar(&skipHealthEvent, "skip-health-event", false, "Skip sending health event (for testing)")
	flag.Parse()

	// Set up structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Get environment variables (set by webhook)
	nodeName := os.Getenv("NODE_NAME")
	podName := os.Getenv("POD_NAME")
	podNamespace := os.Getenv("POD_NAMESPACE")
	gangID := os.Getenv("NVSENTINEL_GANG_ID")

	// Allow env var overrides for hostengine address
	if envAddr := os.Getenv("DCGM_HOSTENGINE_ADDR"); envAddr != "" {
		hostengineAddr = envAddr
	}
	if envLevel := os.Getenv("DCGM_DIAG_LEVEL"); envLevel != "" {
		fmt.Sscanf(envLevel, "%d", &diagLevel)
	}
	if envSock := os.Getenv("PLATFORM_CONNECTOR_SOCKET"); envSock != "" {
		platformConnectorSock = envSock
	}

	slog.Info("Starting preflight DCGM diagnostic",
		"diag_level", diagLevel,
		"hostengine_addr", hostengineAddr,
		"node", nodeName,
		"pod", podName,
		"namespace", podNamespace,
		"gang_id", gangID,
	)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		slog.Warn("Received signal, cancelling", "signal", sig)
		cancel()
	}()

	// Run diagnostic
	exitCode := runDiagnostic(ctx, diagLevel, hostengineAddr, platformConnectorSock, nodeName, skipHealthEvent)
	os.Exit(exitCode)
}

func runDiagnostic(ctx context.Context, diagLevel int, hostengineAddr, platformConnectorSock, nodeName string, skipHealthEvent bool) int {
	// Step 1: Get GPU UUIDs
	slog.Info("Discovering GPUs...")
	gpuUUIDs, err := dcgm.GetGPUUUIDs(ctx)
	if err != nil {
		slog.Error("Failed to discover GPUs", "error", err)
		return exitConfigError
	}
	slog.Info("Found GPUs", "count", len(gpuUUIDs), "uuids", gpuUUIDs)

	// Step 2: Run DCGM diagnostic
	slog.Info("Running DCGM diagnostic", "level", diagLevel)
	client := dcgm.NewClient(hostengineAddr)
	result, err := client.RunDiagnostic(ctx, dcgm.DiagLevel(diagLevel), gpuUUIDs)
	if err != nil {
		slog.Error("DCGM diagnostic command failed", "error", err)
		// Don't exit yet - we might have partial results to report
		if result == nil {
			return exitConfigError
		}
	}

	// Log the raw DCGM output for debugging
	slog.Info("DCGM raw output", "output", result.RawOutput)

	// Log results summary
	slog.Info("DCGM diagnostic completed",
		"has_failures", result.HasFailures,
		"has_warnings", result.HasWarnings,
		"test_count", len(result.TestResults),
	)

	for _, tr := range result.TestResults {
		logLevel := slog.LevelInfo
		if tr.Result == "Fail" {
			logLevel = slog.LevelError
		} else if tr.Result == "Warn" {
			logLevel = slog.LevelWarn
		}
		slog.Log(ctx, logLevel, "Test result",
			"test", tr.TestName,
			"result", tr.Result,
			"gpu_index", tr.GPUIndex,
			"gpu_uuid", tr.GPUUUID,
			"message", tr.Message,
			"error_code", tr.ErrorCode,
		)
	}

	// Step 3: Send health event
	if !skipHealthEvent && platformConnectorSock != "" {
		slog.Info("Reporting to Platform Connector", "socket", platformConnectorSock)
		rep := reporter.NewReporter(platformConnectorSock, nodeName)
		if err := rep.ReportDiagResult(ctx, result); err != nil {
			slog.Error("Failed to report health event", "error", err)
			// Don't fail the preflight check just because reporting failed
			// The diagnostic result is what matters
		}
	}

	// Step 4: Determine exit code
	if result.HasFailures {
		slog.Error("DCGM diagnostic FAILED - GPU hardware issues detected")
		return exitCheckFailed
	}

	if result.HasWarnings {
		slog.Warn("DCGM diagnostic completed with warnings")
	} else {
		slog.Info("DCGM diagnostic PASSED - all GPUs healthy")
	}

	return exitSuccess
}
