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
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"
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

	if cfg.diagLevel < 1 || cfg.diagLevel > 4 {
		return fmt.Errorf("invalid diagnostic level %d: must be 1, 2, 3, or 4", cfg.diagLevel)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	results, err := runDCGMDiag(ctx, cfg.diagLevel, cfg.hostengineAddr)
	if err != nil {
		reportError(cfg.connectorSocket, err.Error())

		return err
	}

	return processResults(results, cfg.connectorSocket)
}

type config struct {
	diagLevel       int
	hostengineAddr  string
	connectorSocket string
	timeout         time.Duration
	verbose         bool
}

func parseConfig() config {
	var cfg config

	diagLevelDefault := getEnvInt("DCGM_DIAG_LEVEL", 1)
	hostengineDefault := getEnv("DCGM_HOSTENGINE_ADDR", "")
	connectorDefault := getEnv("PLATFORM_CONNECTOR_SOCKET", "")
	timeoutDefault := getEnvDuration("DCGM_DIAG_TIMEOUT", 5*time.Minute)
	verboseDefault := getEnv("DCGM_DIAG_VERBOSE", "false") == "true"

	flag.IntVar(&cfg.diagLevel, "level", diagLevelDefault,
		"DCGM diagnostic level (1=quick ~30s, 2=medium ~2min, 3=long ~15min, 4=extended)")
	flag.StringVar(&cfg.hostengineAddr, "hostengine", hostengineDefault,
		"DCGM hostengine address (e.g., localhost:5555). If empty, uses embedded mode.")
	flag.StringVar(&cfg.connectorSocket, "connector-socket", connectorDefault,
		"Platform connector socket path for health event reporting")
	flag.DurationVar(&cfg.timeout, "timeout", timeoutDefault,
		"Timeout for DCGM diagnostic")
	flag.BoolVar(&cfg.verbose, "verbose", verboseDefault,
		"Enable verbose logging of individual test results")
	flag.Parse()

	// Set log level based on verbose flag
	if cfg.verbose {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})))
	}

	return cfg
}
