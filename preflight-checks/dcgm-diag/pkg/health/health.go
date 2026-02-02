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

// Package health provides health event reporting functionality.
package health

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/go-dcgm/pkg/dcgm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/preflight-checks/dcgm-diag/pkg/gpu"
)

const (
	agentName      = "preflight-dcgm-diag"
	componentClass = "GPU"
	checkName      = "DCGM_DIAGNOSTIC"
)

// ReportError reports a fatal error without specific GPU entities.
func ReportError(connectorSocket, message string) {
	if connectorSocket == "" {
		slog.Info("Skipping health event reporting (no connector socket configured)")

		return
	}

	slog.Info("Reporting error health event",
		"socket", connectorSocket,
		"message", message)

	if err := sendHealthEvent(connectorSocket, nil, true, message); err != nil {
		slog.Warn("Failed to report health event", "error", err)
	}
}

// ReportEvent reports a health event for the given diagnostic results.
func ReportEvent(connectorSocket string, results []dcgm.DiagResult, isFatal bool, message string) error {
	if connectorSocket == "" {
		slog.Info("Skipping health event reporting (no connector socket configured)")

		return nil
	}

	var gpuUUIDs []string

	for _, r := range results {
		uuid, err := gpu.GetUUID(r.EntityID)
		if err != nil {
			slog.Error("Failed to get GPU UUID for health event", "gpuIndex", r.EntityID, "error", err)

			return err
		}

		gpuUUIDs = append(gpuUUIDs, uuid)
	}

	slog.Info("Reporting health event",
		"socket", connectorSocket,
		"isFatal", isFatal,
		"gpuCount", len(gpuUUIDs),
		"gpuUUIDs", gpuUUIDs,
		"message", message)

	if err := sendHealthEvent(connectorSocket, gpuUUIDs, isFatal, message); err != nil {
		slog.Error("Failed to report health event", "error", err)

		return err
	}

	slog.Info("Health event reported successfully")

	return nil
}

func sendHealthEvent(socketPath string, gpuUUIDs []string, isFatal bool, message string) error {
	// Handle unix:// prefix
	socketPath = strings.TrimPrefix(socketPath, "unix://")

	// Build entities
	entities := make([]*pb.Entity, 0, len(gpuUUIDs))
	for _, uuid := range gpuUUIDs {
		entities = append(entities, &pb.Entity{
			EntityType:  "GPU_UUID",
			EntityValue: uuid,
		})
	}

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		nodeName = "unknown"
	}

	event := &pb.HealthEvent{
		Version:            1,
		Agent:              agentName,
		ComponentClass:     componentClass,
		CheckName:          checkName,
		IsFatal:            isFatal,
		IsHealthy:          !isFatal,
		Message:            message,
		RecommendedAction:  pb.RecommendedAction_RUN_FIELDDIAG,
		EntitiesImpacted:   entities,
		GeneratedTimestamp: timestamppb.Now(),
		NodeName:           nodeName,
		ProcessingStrategy: pb.ProcessingStrategy_EXECUTE_REMEDIATION,
	}

	slog.Info("Sending health event", "event", event)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(
		fmt.Sprintf("unix://%s", socketPath),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to platform connector: %w", err)
	}

	defer conn.Close()

	client := pb.NewPlatformConnectorClient(conn)

	healthEvents := &pb.HealthEvents{
		Version: 1,
		Events:  []*pb.HealthEvent{event},
	}

	_, err = client.HealthEventOccurredV1(ctx, healthEvents)
	if err != nil {
		return fmt.Errorf("failed to send health event: %w", err)
	}

	return nil
}
