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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

const (
	agentName      = "preflight-dcgm-diag"
	componentClass = "GPU"
	checkName      = "DCGM_DIAGNOSTIC"

	maxRetries  = 5
	retryDelay  = 2 * time.Second
	retryFactor = 1.5
	retryJitter = 0.1
	sendTimeout = 10 * time.Second
)

// SendHealthEvent sends a health event to the platform connector.
func SendHealthEvent(socketPath string, gpuUUIDs []string, isHealthy, isFatal bool, message string) error {
	event := buildHealthEvent(gpuUUIDs, isHealthy, isFatal, message)
	healthEvents := &pb.HealthEvents{
		Version: 1,
		Events:  []*pb.HealthEvent{event},
	}

	slog.Info("Sending health event",
		"isHealthy", isHealthy,
		"isFatal", isFatal,
		"gpuCount", len(gpuUUIDs),
		"message", message)

	// Handle unix:// prefix
	socketPath = strings.TrimPrefix(socketPath, "unix://")

	backoff := wait.Backoff{
		Steps:    maxRetries,
		Duration: retryDelay,
		Factor:   retryFactor,
		Jitter:   retryJitter,
	}

	err := wait.ExponentialBackoff(backoff, func() (bool, error) {
		sendErr := sendToConnector(socketPath, healthEvents)
		if sendErr == nil {
			slog.Info("Health event sent successfully")

			return true, nil
		}

		if isRetryableError(sendErr) {
			slog.Warn("Retryable error sending health event, will retry", "error", sendErr)

			return false, nil
		}

		slog.Error("Non-retryable error sending health event", "error", sendErr)

		return false, fmt.Errorf("non-retryable error: %w", sendErr)
	})
	if err != nil {
		return fmt.Errorf("failed to send health event after %d retries: %w", maxRetries, err)
	}

	return nil
}

func buildHealthEvent(gpuUUIDs []string, isHealthy, isFatal bool, message string) *pb.HealthEvent {
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

	// For healthy events, just store the result without triggering remediation
	recommendedAction := pb.RecommendedAction_RUN_FIELDDIAG
	processingStrategy := pb.ProcessingStrategy_EXECUTE_REMEDIATION

	if isHealthy {
		recommendedAction = pb.RecommendedAction_NONE
		processingStrategy = pb.ProcessingStrategy_STORE_ONLY
	}

	return &pb.HealthEvent{
		Version:            1,
		Agent:              agentName,
		ComponentClass:     componentClass,
		CheckName:          checkName,
		IsFatal:            isFatal,
		IsHealthy:          isHealthy,
		Message:            message,
		RecommendedAction:  recommendedAction,
		EntitiesImpacted:   entities,
		GeneratedTimestamp: timestamppb.Now(),
		NodeName:           nodeName,
		ProcessingStrategy: processingStrategy,
	}
}

func sendToConnector(socketPath string, healthEvents *pb.HealthEvents) error {
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
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

	_, err = client.HealthEventOccurredV1(ctx, healthEvents)
	if err != nil {
		return fmt.Errorf("failed to send health event: %w", err)
	}

	return nil
}

// isRetryableError determines if an error is retryable.
func isRetryableError(err error) bool {
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.Unavailable || s.Code() == codes.DeadlineExceeded {
			return true
		}
	}

	if _, ok := err.(interface{ Temporary() bool }); ok {
		return true
	}

	if errors.Is(err, io.EOF) {
		return true
	}

	errStr := err.Error()

	return strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection refused")
}
