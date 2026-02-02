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

// Package reporting provides shared health event reporting functionality
// for preflight checks.
package reporting

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

// HealthEventConfig holds configuration for health event reporting.
type HealthEventConfig struct {
	// SocketPath is the Unix socket path for the platform connector.
	// Can optionally include "unix://" prefix.
	SocketPath string

	// AgentName identifies the preflight check (e.g., "preflight-dcgm-diag").
	AgentName string

	// ComponentClass identifies the component type (e.g., "GPU", "NETWORK").
	ComponentClass string

	// CheckName identifies the specific check (e.g., "DCGM_DIAGNOSTIC").
	CheckName string
}

// Entity represents an impacted entity in a health event.
type Entity struct {
	Type  string // e.g., "GPU_INDEX", "GPU_UUID", "NIC_NAME"
	Value string // e.g., "0", "GPU-abc123", "eth0"
}

// Reporter handles health event reporting to the platform connector.
type Reporter struct {
	config   HealthEventConfig
	nodeName string
}

// NewReporter creates a new health event reporter.
func NewReporter(config HealthEventConfig) *Reporter {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		nodeName = "unknown"
	}

	return &Reporter{
		config:   config,
		nodeName: nodeName,
	}
}

// ReportError reports a fatal error without specific entities.
func (r *Reporter) ReportError(message string) {
	if err := r.Report(nil, true, message); err != nil {
		slog.Warn("Failed to report health event", "error", err)
	}
}

// Report sends a health event to the platform connector.
func (r *Reporter) Report(entities []Entity, isFatal bool, message string) error {
	if r.config.SocketPath == "" {
		slog.Debug("No platform connector socket configured, skipping health event reporting")

		return nil
	}

	// Handle unix:// prefix
	socketPath := strings.TrimPrefix(r.config.SocketPath, "unix://")

	// Build entities
	pbEntities := make([]*pb.Entity, 0, len(entities))

	for _, e := range entities {
		pbEntities = append(pbEntities, &pb.Entity{
			EntityType:  e.Type,
			EntityValue: e.Value,
		})
	}

	event := &pb.HealthEvent{
		Version:            1,
		Agent:              r.config.AgentName,
		ComponentClass:     r.config.ComponentClass,
		CheckName:          r.config.CheckName,
		IsFatal:            isFatal,
		IsHealthy:          !isFatal,
		Message:            message,
		RecommendedAction:  pb.RecommendedAction_RUN_FIELDDIAG,
		EntitiesImpacted:   pbEntities,
		GeneratedTimestamp: timestamppb.Now(),
		NodeName:           r.nodeName,
		ProcessingStrategy: pb.ProcessingStrategy_EXECUTE_REMEDIATION,
	}

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

	slog.Info("Health event reported successfully", "isFatal", isFatal, "agent", r.config.AgentName)

	return nil
}
