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

// Package diag provides DCGM diagnostic functionality.
package diag

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/NVIDIA/go-dcgm/pkg/dcgm"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/preflight-checks/dcgm-diag/pkg/gpu"
	"github.com/nvidia/nvsentinel/preflight-checks/dcgm-diag/pkg/health"
)

// Run executes DCGM diagnostics using the go-dcgm bindings.
//
// Note: go-dcgm requires CGO and links against libdcgm.so at compile time.
// The binary must be built with DCGM 4.2.3+ which introduced dcgmDiagResponse_version12.
//
// dcgm.RunDiag() is synchronous with no cancellation support, so timeout enforcement
// is delegated to the Kubernetes init container timeout rather than handled here.
func Run(level int, hostengineAddr string) (*dcgm.DiagResults, error) {
	cleanup, err := initDCGM(hostengineAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DCGM: %w", err)
	}

	defer cleanup()

	diagType := levelToDiagType(level)

	group, groupCleanup, err := gpu.GetGroup()
	if err != nil {
		return nil, fmt.Errorf("failed to create GPU group: %w", err)
	}

	if groupCleanup != nil {
		defer groupCleanup()
	}

	slog.Info("Running DCGM diagnostic", "level", level, "diagType", diagType)

	results, err := dcgm.RunDiag(diagType, group)
	if err != nil {
		return nil, fmt.Errorf("diagnostic failed: %w", err)
	}

	logResults(&results)

	return &results, nil
}

func initDCGM(hostengineAddr string) (func(), error) {
	if hostengineAddr != "" {
		slog.Info("Connecting to DCGM hostengine", "address", hostengineAddr)

		return dcgm.Init(dcgm.Standalone, hostengineAddr, "0")
	}

	slog.Info("Starting DCGM in embedded mode")

	return dcgm.Init(dcgm.Embedded)
}

func levelToDiagType(level int) dcgm.DiagType {
	switch level {
	case 1:
		return dcgm.DiagQuick
	case 2:
		return dcgm.DiagMedium
	case 3:
		return dcgm.DiagLong
	case 4:
		return dcgm.DiagExtended
	default:
		return dcgm.DiagQuick
	}
}

// ProcessResults processes diagnostic results and reports health events.
func ProcessResults(
	results *dcgm.DiagResults,
	connectorSocket string,
	nodeName string,
	processingStrategy pb.ProcessingStrategy,
) error {
	var failures, warnings []dcgm.DiagResult

	for _, result := range results.Software {
		switch result.Status {
		case "fail":
			failures = append(failures, result)
		case "warn":
			warnings = append(warnings, result)
		}
	}

	if len(failures) > 0 {
		msg := formatResults(failures)
		uuids := resultsToUUIDs(failures)

		reportErr := health.SendHealthEvent(connectorSocket, nodeName, uuids, false, true, msg, processingStrategy)
		if reportErr != nil {
			slog.Warn("Failed to report health event", "error", reportErr)
		}

		return fmt.Errorf("DCGM diagnostic failed: %s", msg)
	}

	if len(warnings) > 0 {
		msg := formatResults(warnings)
		uuids := resultsToUUIDs(warnings)

		slog.Warn("DCGM diagnostic warnings", "message", msg)

		reportErr := health.SendHealthEvent(connectorSocket, nodeName, uuids, false, false, msg, processingStrategy)
		if reportErr != nil {
			slog.Warn("Failed to report health event", "error", reportErr)
		}
	}

	slog.Info("DCGM diagnostic completed successfully",
		"tests_run", len(results.Software),
		"warnings", len(warnings))

	if len(warnings) == 0 {
		uuids := gpu.GetAllUUIDs()

		reportErr := health.SendHealthEvent(connectorSocket,
			nodeName, uuids, true, false, "DCGM diagnostic passed", processingStrategy)
		if reportErr != nil {
			slog.Warn("Failed to report healthy event", "error", reportErr)
		}
	}

	return nil
}

func resultsToUUIDs(results []dcgm.DiagResult) []string {
	var uuids []string

	for _, r := range results {
		uuid, err := gpu.GetUUID(r.EntityID)
		if err != nil {
			slog.Warn("Failed to get GPU UUID", "gpuIndex", r.EntityID, "error", err)

			continue
		}

		uuids = append(uuids, uuid)
	}

	return uuids
}

func logResults(results *dcgm.DiagResults) {
	var passed, failed, warned, skipped int

	for _, r := range results.Software {
		switch r.Status {
		case "pass":
			passed++
		case "fail":
			failed++
		case "warn":
			warned++
		case "skip":
			skipped++
		}

		slog.Info("Test result",
			"test", r.TestName,
			"status", r.Status,
			"gpu", r.EntityID,
			"error", r.ErrorMessage,
			"output", r.TestOutput)
	}

	slog.Info("Diagnostic summary",
		"passed", passed,
		"failed", failed,
		"warned", warned,
		"skipped", skipped,
		"total", len(results.Software))
}

func formatResults(results []dcgm.DiagResult) string {
	var parts []string

	for _, r := range results {
		msg := fmt.Sprintf("%s (GPU %d): %s", r.TestName, r.EntityID, r.ErrorMessage)
		if r.ErrorMessage == "" {
			msg = fmt.Sprintf("%s (GPU %d): %s", r.TestName, r.EntityID, r.Status)
		}

		parts = append(parts, msg)
	}

	return strings.Join(parts, "; ")
}
