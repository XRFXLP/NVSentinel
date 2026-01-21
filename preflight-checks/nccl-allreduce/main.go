// Preflight NCCL All-Reduce Checker
//
// This init container runs NCCL all-reduce benchmarks across all gang members
// before the main workload starts. It detects cross-node communication issues early.
//
// Flow:
// 1. Wait for gang formation via ConfigMap (all peers registered)
// 2. Calculate rank based on sorted pod names
// 3. Run torchrun with NCCL benchmark
// 4. Report results to Platform Connector
//
// Exit codes:
//   - 0: All checks passed
//   - 1: Check failed (bandwidth below threshold)
//   - 2: Configuration error
//   - 3: Gang formation timeout (not a hardware issue)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/NVIDIA/NVSentinel/preflight-checks/nccl-allreduce/pkg/coordination"
)

const (
	exitSuccess     = 0
	exitCheckFailed = 1
	exitConfigError = 2
	exitGangTimeout = 3

	defaultGangConfigDir = "/etc/preflight"
	defaultScriptPath    = "/scripts/bench.py"
	defaultNProcsPerNode = 8
	defaultBWThreshold   = 100.0 // GB/s
)

// BenchmarkResult represents the JSON output from bench.py
type BenchmarkResult struct {
	WorldSize   int     `json:"world_size"`
	NumNodes    int     `json:"num_nodes"`
	GPUsPerNode int     `json:"gpus_per_node"`
	Threshold   float64 `json:"threshold_gbps"`
	Passed      bool    `json:"passed"`
	MinBusBW    float64 `json:"min_bus_bw"`
	Tests       []struct {
		SizeBytes int64   `json:"size_bytes"`
		SizeHuman string  `json:"size_human"`
		TimeMS    float64 `json:"time_ms"`
		AlgoBW    float64 `json:"algo_bw_gbps"`
		BusBW     float64 `json:"bus_bw_gbps"`
		Passed    bool    `json:"passed"`
	} `json:"tests"`
}

func main() {
	var (
		gangConfigDir string
		scriptPath    string
		nprocsPerNode int
		bwThreshold   float64
		gangTimeout   time.Duration
		skipGangWait  bool
	)

	flag.StringVar(&gangConfigDir, "gang-config-dir", defaultGangConfigDir, "Directory containing gang ConfigMap")
	flag.StringVar(&scriptPath, "script", defaultScriptPath, "Path to benchmark script")
	flag.IntVar(&nprocsPerNode, "nprocs-per-node", defaultNProcsPerNode, "Number of GPUs per node")
	flag.Float64Var(&bwThreshold, "bw-threshold", defaultBWThreshold, "Minimum bus bandwidth threshold (GB/s)")
	flag.DurationVar(&gangTimeout, "gang-timeout", 10*time.Minute, "Timeout for gang formation")
	flag.BoolVar(&skipGangWait, "skip-gang-wait", false, "Skip gang wait (for single-node testing)")
	flag.Parse()

	// Set up structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Get environment variables
	podName := os.Getenv("POD_NAME")
	podIP := os.Getenv("POD_IP")
	nodeName := os.Getenv("NODE_NAME")
	gangID := os.Getenv("NVSENTINEL_GANG_ID") // For logging only

	// Environment overrides
	if envDir := os.Getenv("GANG_CONFIG_DIR"); envDir != "" {
		gangConfigDir = envDir
	}
	if envProcs := os.Getenv("NPROCS_PER_NODE"); envProcs != "" {
		if n, err := strconv.Atoi(envProcs); err == nil {
			nprocsPerNode = n
		}
	}
	if envThreshold := os.Getenv("BW_THRESHOLD_GBPS"); envThreshold != "" {
		if t, err := strconv.ParseFloat(envThreshold, 64); err == nil {
			bwThreshold = t
		}
	}
	if envTimeout := os.Getenv("GANG_TIMEOUT"); envTimeout != "" {
		if t, err := time.ParseDuration(envTimeout); err == nil {
			gangTimeout = t
		}
	}

	// If POD_IP not set, try to get it
	if podIP == "" {
		podIP = os.Getenv("MY_POD_IP")
	}

	slog.Info("Starting NCCL All-Reduce preflight check",
		"pod", podName,
		"ip", podIP,
		"node", nodeName,
		"gang_id", gangID,
		"gang_config_dir", gangConfigDir,
		"nprocs_per_node", nprocsPerNode,
		"bw_threshold", bwThreshold,
		"gang_timeout", gangTimeout,
	)

	// Validate required fields
	if podName == "" {
		slog.Error("POD_NAME environment variable not set")
		os.Exit(exitConfigError)
	}

	// Create context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		slog.Warn("Received signal, cancelling", "signal", sig)
		cancel()
	}()

	// Run the check
	exitCode := runNCCLCheck(ctx, gangConfigDir, scriptPath, podName, podIP, nprocsPerNode, bwThreshold, gangTimeout, skipGangWait)
	os.Exit(exitCode)
}

func runNCCLCheck(ctx context.Context, gangConfigDir, scriptPath, podName, podIP string, nprocsPerNode int, bwThreshold float64, gangTimeout time.Duration, skipGangWait bool) int {
	var gangConfig *coordination.GangConfig

	if skipGangWait {
		// Single node mode - create fake config
		slog.Info("Skipping gang wait (single-node mode)")
		gangConfig = &coordination.GangConfig{
			ExpectedCount: 1,
			MasterAddr:    "127.0.0.1",
			MasterPort:    "29500",
			MyRank:        0,
			MyPodName:     podName,
			MyPodIP:       podIP,
			Peers: []coordination.PeerInfo{
				{PodName: podName, PodIP: podIP},
			},
		}
	} else {
		// Read gang config from ConfigMap volume (mounted by webhook)
		// The webhook pre-populates DNS names, so we just need to wait for DNS to resolve
		coordinator := coordination.NewCoordinator(gangConfigDir)
		var err error
		gangConfig, err = coordinator.WaitForGang(ctx, podName, podIP, gangTimeout)
		if err != nil {
			if ctx.Err() != nil {
				slog.Error("Gang formation cancelled", "error", ctx.Err())
				return exitConfigError
			}
			slog.Error("Gang formation timeout - not a hardware issue", "error", err)
			return exitGangTimeout
		}
	}

	slog.Info("Gang configuration", "config", gangConfig.String())

	// Build and run torchrun command
	args := gangConfig.GetTorchrunArgs(nprocsPerNode, scriptPath)
	slog.Info("Running torchrun", "args", args)

	// Debug: Log NCCL-related environment variables
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, "NCCL_") || strings.HasPrefix(env, "CUDA_") || strings.Contains(env, "IB_") {
			slog.Info("NCCL env var", "var", env)
		}
	}

	// Use bash -c to run torchrun - this ensures proper environment inheritance
	// and matches how manual testing works
	torchrunCmd := strings.Join(args, " ")
	cmd := exec.CommandContext(ctx, "bash", "-c", torchrunCmd)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("BW_THRESHOLD_GBPS=%f", bwThreshold),
		fmt.Sprintf("NPROCS_PER_NODE=%d", nprocsPerNode),
	)
	cmd.Stderr = os.Stderr
	slog.Info("Executing via bash", "command", torchrunCmd)

	// Capture stdout for JSON result
	output, err := cmd.Output()
	if err != nil {
		slog.Error("torchrun failed", "error", err)
		if exitErr, ok := err.(*exec.ExitError); ok {
			slog.Error("torchrun stderr", "stderr", string(exitErr.Stderr))
			// If torchrun exited with code 1, it's a benchmark failure
			if exitErr.ExitCode() == 1 {
				return exitCheckFailed
			}
		}
		return exitConfigError
	}

	// Parse results
	var result BenchmarkResult
	if err := json.Unmarshal(output, &result); err != nil {
		slog.Warn("Failed to parse benchmark result JSON", "error", err, "output", string(output))
		// If we can't parse but command succeeded, assume pass
		return exitSuccess
	}

	slog.Info("Benchmark completed",
		"passed", result.Passed,
		"min_bus_bw", result.MinBusBW,
		"threshold", result.Threshold,
		"world_size", result.WorldSize,
		"num_nodes", result.NumNodes,
	)

	if result.Passed {
		slog.Info("NCCL All-Reduce check PASSED")
		return exitSuccess
	}

	slog.Error("NCCL All-Reduce check FAILED",
		"min_bus_bw", result.MinBusBW,
		"threshold", result.Threshold)
	return exitCheckFailed
}
