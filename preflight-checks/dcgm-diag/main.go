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
	"log/slog"
	"os"

	"github.com/nvidia/nvsentinel/preflight-checks/dcgm-diag/pkg/config"
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
	cfg, err := config.Parse()
	if err != nil {
		return err
	}

	slog.Info("Configuration loaded",
		"diagLevel", cfg.DiagLevel,
		"processingStrategy", cfg.ProcessingStrategy.String())

	results, err := diag.Run(cfg.DiagLevel, cfg.HostengineAddr)
	if err != nil {
		reportErr := health.SendHealthEvent(cfg.ConnectorSocket,
			cfg.NodeName, nil, false, false, err.Error(), cfg.ProcessingStrategy)
		if reportErr != nil {
			slog.Warn("Failed to report error health event", "error", reportErr)
		}

		return err
	}

	return diag.ProcessResults(results, cfg.ConnectorSocket, cfg.NodeName, cfg.ProcessingStrategy)
}
