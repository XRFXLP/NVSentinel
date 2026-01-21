// Package reporter sends health events to the Platform Connector via gRPC.
package reporter

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/NVIDIA/NVSentinel/preflight-checks/dcgm-diag/pkg/dcgm"
	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	agentName      = "preflight-dcgm-diag"
	componentClass = "GPU"
	checkName      = "DCGMDiagnostic"

	maxRetries   = 5
	initialDelay = 500 * time.Millisecond
)

// Reporter sends health events to Platform Connector.
type Reporter struct {
	socketPath string
	nodeName   string
}

// NewReporter creates a new health event reporter.
func NewReporter(socketPath, nodeName string) *Reporter {
	return &Reporter{
		socketPath: socketPath,
		nodeName:   nodeName,
	}
}

// ReportDiagResult sends health events based on DCGM diagnostic results.
func (r *Reporter) ReportDiagResult(ctx context.Context, result *dcgm.DiagResult) error {
	var events []*pb.HealthEvent

	if result.HasFailures || result.HasWarnings {
		// Report individual test failures/warnings
		for _, tr := range result.TestResults {
			if tr.Result == "Fail" || tr.Result == "Warn" {
				event := r.buildHealthEvent(tr)
				events = append(events, event)
			}
		}
	} else {
		// All tests passed - send healthy event
		event := r.buildHealthyEvent(result)
		events = append(events, event)
	}

	if len(events) == 0 {
		slog.Info("No health events to report")
		return nil
	}

	return r.sendEvents(ctx, events)
}

// buildHealthEvent creates a HealthEvent from a test result.
func (r *Reporter) buildHealthEvent(tr dcgm.TestResult) *pb.HealthEvent {
	actionStr, isFatal := dcgm.MapTestToRecommendedAction(tr.TestName, tr.Result)
	action := mapStringToAction(actionStr)

	isHealthy := tr.Result != "Fail"

	message := fmt.Sprintf("DCGM %s test %s", tr.TestName, strings.ToLower(tr.Result))
	if tr.Message != "" {
		message += ": " + tr.Message
	}

	var errorCodes []string
	if tr.ErrorCode != "" {
		errorCodes = append(errorCodes, tr.ErrorCode)
	}

	var entities []*pb.Entity
	if tr.GPUUUID != "" {
		entities = append(entities, &pb.Entity{
			EntityType:  "GPU",
			EntityValue: tr.GPUUUID,
		})
	}

	return &pb.HealthEvent{
		Version:            1,
		Agent:              agentName,
		ComponentClass:     componentClass,
		CheckName:          fmt.Sprintf("%s_%s", checkName, tr.TestName),
		IsFatal:            isFatal,
		IsHealthy:          isHealthy,
		Message:            message,
		RecommendedAction:  action,
		ErrorCode:          errorCodes,
		EntitiesImpacted:   entities,
		NodeName:           r.nodeName,
		GeneratedTimestamp: timestamppb.New(time.Now()),
		ProcessingStrategy: pb.ProcessingStrategy_EXECUTE_REMEDIATION,
		Metadata: map[string]string{
			"test_name":   tr.TestName,
			"test_result": tr.Result,
			"gpu_index":   fmt.Sprintf("%d", tr.GPUIndex),
		},
	}
}

// buildHealthyEvent creates a healthy HealthEvent when all tests pass.
func (r *Reporter) buildHealthyEvent(result *dcgm.DiagResult) *pb.HealthEvent {
	var entities []*pb.Entity
	for _, uuid := range result.GPUUUIDs {
		entities = append(entities, &pb.Entity{
			EntityType:  "GPU",
			EntityValue: uuid,
		})
	}

	return &pb.HealthEvent{
		Version:            1,
		Agent:              agentName,
		ComponentClass:     componentClass,
		CheckName:          checkName,
		IsFatal:            false,
		IsHealthy:          true,
		Message:            fmt.Sprintf("DCGM diagnostic level %d passed for all GPUs", result.Level),
		RecommendedAction:  pb.RecommendedAction_NONE,
		EntitiesImpacted:   entities,
		NodeName:           r.nodeName,
		GeneratedTimestamp: timestamppb.New(time.Now()),
		ProcessingStrategy: pb.ProcessingStrategy_EXECUTE_REMEDIATION,
		Metadata: map[string]string{
			"diag_level": fmt.Sprintf("%d", result.Level),
			"gpu_count":  fmt.Sprintf("%d", len(result.GPUUUIDs)),
		},
	}
}

// sendEvents sends health events to Platform Connector with retries.
func (r *Reporter) sendEvents(ctx context.Context, events []*pb.HealthEvent) error {
	healthEvents := &pb.HealthEvents{
		Version: 1,
		Events:  events,
	}

	slog.Info("Sending health events to Platform Connector",
		"socket", r.socketPath,
		"event_count", len(events))

	delay := initialDelay
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := r.sendOnce(ctx, healthEvents)
		if err == nil {
			slog.Info("Successfully sent health events")
			return nil
		}

		lastErr = err
		if !isRetryable(err) {
			slog.Error("Non-retryable error sending health events", "error", err)
			return fmt.Errorf("non-retryable error: %w", err)
		}

		slog.Warn("Retryable error sending health events",
			"attempt", attempt,
			"max_retries", maxRetries,
			"error", err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			delay = time.Duration(float64(delay) * 1.5)
		}
	}

	return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

// sendOnce attempts to send health events once.
func (r *Reporter) sendOnce(ctx context.Context, events *pb.HealthEvents) error {
	conn, err := grpc.NewClient(
		"unix://"+r.socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to create gRPC client: %w", err)
	}
	defer conn.Close()

	client := pb.NewPlatformConnectorClient(conn)
	_, err = client.HealthEventOccurredV1(ctx, events)
	return err
}

// isRetryable checks if an error is retryable.
func isRetryable(err error) bool {
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
			return true
		}
	}

	errStr := err.Error()
	return strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "EOF")
}

// mapStringToAction converts action string to protobuf enum.
func mapStringToAction(action string) pb.RecommendedAction {
	switch action {
	case "NONE":
		return pb.RecommendedAction_NONE
	case "CONTACT_SUPPORT":
		return pb.RecommendedAction_CONTACT_SUPPORT
	case "RUN_DCGMEUD":
		return pb.RecommendedAction_RUN_DCGMEUD
	case "COMPONENT_RESET":
		return pb.RecommendedAction_COMPONENT_RESET
	default:
		return pb.RecommendedAction_UNKNOWN
	}
}
