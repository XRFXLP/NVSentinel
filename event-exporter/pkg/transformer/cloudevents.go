package transformer

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

type CloudEvent struct {
	SpecVersion string         `json:"specversion"`
	Type        string         `json:"type"`
	Source      string         `json:"source"`
	ID          string         `json:"id"`
	Time        string         `json:"time"`
	Data        map[string]any `json:"data"`
}

func ToCloudEvent(event *pb.HealthEvent, metadata map[string]string) (*CloudEvent, error) {
	if event == nil {
		return nil, fmt.Errorf("event is nil")
	}

	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	if event.GeneratedTimestamp != nil {
		timestamp = event.GeneratedTimestamp.AsTime().UTC().Format(time.RFC3339Nano)
	}

	entities := make([]map[string]any, 0, len(event.EntitiesImpacted))
	for _, e := range event.EntitiesImpacted {
		entities = append(entities, map[string]any{
			"entityType":  e.EntityType,
			"entityValue": e.EntityValue,
		})
	}

	errorCodes := make([]string, len(event.ErrorCode))

	copy(errorCodes, event.ErrorCode)

	healthEventData := map[string]any{
		"version":            event.Version,
		"agent":              event.Agent,
		"componentClass":     event.ComponentClass,
		"checkName":          event.CheckName,
		"isFatal":            event.IsFatal,
		"isHealthy":          event.IsHealthy,
		"message":            event.Message,
		"recommendedAction":  int32(event.RecommendedAction),
		"errorCode":          errorCodes,
		"entitiesImpacted":   entities,
		"generatedTimestamp": timestamp,
		"nodeName":           event.NodeName,
	}

	if len(event.Metadata) > 0 {
		healthEventData["metadata"] = event.Metadata
	}

	if event.QuarantineOverrides != nil {
		healthEventData["quarantineOverrides"] = map[string]any{
			"force": event.QuarantineOverrides.Force,
			"skip":  event.QuarantineOverrides.Skip,
		}
	}

	if event.DrainOverrides != nil {
		healthEventData["drainOverrides"] = map[string]any{
			"force": event.DrainOverrides.Force,
			"skip":  event.DrainOverrides.Skip,
		}
	}

	clusterName := metadata["cluster"]
	if clusterName == "" {
		clusterName = "unknown"
	}

	return &CloudEvent{
		SpecVersion: "1.0",
		Type:        "com.nvidia.nvsentinel.health.v1",
		Source:      fmt.Sprintf("nvsentinel://%s/healthevents", clusterName),
		ID:          uuid.New().String(),
		Time:        timestamp,
		Data: map[string]any{
			"metadata":    metadata,
			"healthEvent": healthEventData,
		},
	}, nil
}
