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

	"github.com/NVIDIA/go-dcgm/pkg/dcgm"
	"github.com/nvidia/nvsentinel/preflight-checks/reporting"
)

const (
	agentName      = "preflight-dcgm-diag"
	componentClass = "GPU"
	checkName      = "DCGM_DIAGNOSTIC"
)

func newReporter(socketPath string) *reporting.Reporter {
	return reporting.NewReporter(reporting.HealthEventConfig{
		SocketPath:     socketPath,
		AgentName:      agentName,
		ComponentClass: componentClass,
		CheckName:      checkName,
	})
}

func reportError(connectorSocket, message string) {
	reporter := newReporter(connectorSocket)
	reporter.ReportError(message)
}

func reportHealthEvent(connectorSocket string, results []dcgm.DiagResult, isFatal bool, message string) error {
	reporter := newReporter(connectorSocket)

	entities := make([]reporting.Entity, 0, len(results))

	for _, r := range results {
		uuid, err := GetGPUUUID(r.EntityID)
		if err != nil {
			slog.Error("Failed to get GPU UUID for health event", "gpuIndex", r.EntityID, "error", err)

			return err
		}

		entities = append(entities, reporting.Entity{
			Type:  "GPU_UUID",
			Value: uuid,
		})
	}

	if err := reporter.Report(entities, isFatal, message); err != nil {
		slog.Warn("Failed to report health event", "error", err)

		return err
	}

	return nil
}
