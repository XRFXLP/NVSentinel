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
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/nvidia/nvsentinel/preflight-checks/dcgm-diag/pkg/diag"
	"github.com/nvidia/nvsentinel/preflight-checks/dcgm-diag/pkg/health"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	slog.Info("Starting preflight dcgm-diag check",
		"version", version, "commit", commit, "date", date)

	if err := run(); err != nil {
		slog.Error("DCGM diagnostic check failed", "error", err)
		os.Exit(1)
	}

	slog.Info("DCGM diagnostic check passed")
	os.Exit(0)
}

func run() error {
	cfg := parseConfig()

	if cfg.connectorSocket == "" {
		return fmt.Errorf("platform connector socket is required (set PLATFORM_CONNECTOR_SOCKET or --connector-socket)")
	}

	if cfg.diagLevel < 1 || cfg.diagLevel > 4 {
		return fmt.Errorf("invalid diagnostic level %d: must be 1, 2, 3, or 4", cfg.diagLevel)
	}

	processingStrategy, err := health.ParseProcessingStrategy(cfg.processingStrategy)
	if err != nil {
		return err
	}

	slog.Info("Event handling strategy configured", "processingStrategy", cfg.processingStrategy)

	// Note: dcgm.RunDiag() is synchronous with no cancellation support.
	// Timeout is enforced by Kubernetes init container timeout instead.
	results, err := diag.Run(cfg.diagLevel, cfg.hostengineAddr)
	if err != nil {
		// Report error without specific GPU UUIDs (we don't know which GPU failed)
		reportErr := health.SendHealthEvent(cfg.connectorSocket, nil, false, false, err.Error(), processingStrategy)
		if reportErr != nil {
			slog.Warn("Failed to report error health event", "error", reportErr)
		}

		return err
	}

	return diag.ProcessResults(results, cfg.connectorSocket, processingStrategy)
}

type config struct {
	diagLevel          int
	hostengineAddr     string
	connectorSocket    string
	processingStrategy string
}

func parseConfig() config {
	var cfg config

	diagLevelDefault := getEnvInt("DCGM_DIAG_LEVEL", 1)
	hostengineDefault := getEnv("DCGM_HOSTENGINE_ADDR", "")
	connectorDefault := getEnv("PLATFORM_CONNECTOR_SOCKET", "")
	strategyDefault := getEnv("PROCESSING_STRATEGY", "EXECUTE_REMEDIATION")

	flag.IntVar(&cfg.diagLevel, "level", diagLevelDefault,
		"DCGM diagnostic level (1=quick ~30s, 2=medium ~2min, 3=long ~15min, 4=extended)")
	flag.StringVar(&cfg.hostengineAddr, "hostengine", hostengineDefault,
		"DCGM hostengine address (e.g., localhost:5555). If empty, uses embedded mode.")
	flag.StringVar(&cfg.connectorSocket, "connector-socket", connectorDefault,
		"Platform connector socket path for health event reporting")
	flag.StringVar(&cfg.processingStrategy, "processing-strategy", strategyDefault,
		"Event processing strategy: EXECUTE_REMEDIATION or STORE_ONLY")
	flag.Parse()

	return cfg
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var result int
		if _, err := fmt.Sscanf(value, "%d", &result); err == nil {
			return result
		}
	}

	return defaultValue
}
