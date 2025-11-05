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

package reconciler

import (
	"context"
	"fmt"
	"testing"
	"time"

	datamodels "github.com/nvidia/nvsentinel/data-models/pkg/model"
	protos "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	config "github.com/nvidia/nvsentinel/health-events-analyzer/pkg/config"
	"github.com/nvidia/nvsentinel/health-events-analyzer/pkg/publisher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Mock Publisher
type mockPublisher struct {
	mock.Mock
}

func (m *mockPublisher) HealthEventOccurredV1(ctx context.Context, events *protos.HealthEvents, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	args := m.Called(ctx, events)
	return args.Get(0).(*emptypb.Empty), args.Error(1)
}

// Mock CollectionClient
type mockCollectionClient struct {
	mock.Mock
}

func (m *mockCollectionClient) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error) {
	args := m.Called(ctx, pipeline, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*mongo.Cursor), args.Error(1)
}

func createMockCursor(docs []bson.M) (*mongo.Cursor, error) {
	// Create raw BSON documents to avoid DocumentSequenceStyle issues
	var rawDocs []interface{}
	for _, doc := range docs {
		// Marshal to BSON and then unmarshal to ensure proper format
		data, err := bson.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal document: %w", err)
		}

		var rawDoc bson.Raw
		rawDoc = bson.Raw(data)
		rawDocs = append(rawDocs, rawDoc)
	}

	return mongo.NewCursorFromDocuments(rawDocs, nil, nil)
}

var (
	rules = []config.HealthEventsAnalyzerRule{
		{
			Name:        "rule1",
			Description: "check multiple remediations are completed within 2 minutes",
			TimeWindow:  "2m",
			Sequence: []config.SequenceStep{{
				Criteria: map[string]interface{}{
					"healtheventstatus.faultremediated": "true",
					"healthevent.nodename":              "this.healthevent.nodename",
				},
				ErrorCount: 5,
			}},
			RecommendedAction: "CONTACT_SUPPORT",
		},
		{
			Name:        "rule2",
			Description: "check the occurrence of XID error 13",
			TimeWindow:  "2m",
			Sequence: []config.SequenceStep{{
				Criteria: map[string]interface{}{
					"healthevent.entitiesimpacted.0.entitytype":  "GPU",
					"healthevent.entitiesimpacted.0.entityvalue": "this.healthevent.entitiesimpacted.0.entityvalue",
					"healthevent.errorcode.0":                    "13",
					"healthevent.nodename":                       "this.healthevent.nodename",
					"healthevent.checkname":                      "{\"$ne\": \"HealthEventsAnalyzer\"}",
				},
				ErrorCount: 3,
			}},
			RecommendedAction: "CONTACT_SUPPORT",
		},
		{
			Name:        "rule3",
			Description: "check the occurrence of XID error 13 and XID error 31",
			TimeWindow:  "3m",
			Sequence: []config.SequenceStep{
				{
					Criteria: map[string]interface{}{
						"healthevent.entitiesimpacted.0.entitytype":  "GPU",
						"healthevent.entitiesimpacted.0.entityvalue": "this.healthevent.entitiesimpacted.0.entityvalue",
						"healthevent.errorcode.0":                    "13",
						"healthevent.nodename":                       "this.healthevent.nodename",
					},
					ErrorCount: 1,
				},
				{
					Criteria: map[string]interface{}{
						"healthevent.entitiesimpacted.0.entitytype":  "GPU",
						"healthevent.entitiesimpacted.0.entityvalue": "this.healthevent.entitiesimpacted.0.entityvalue",
						"healthevent.errorcode.0":                    "31",
						"healthevent.nodename":                       "this.healthevent.nodename",
					},
					ErrorCount: 1,
				}},
			RecommendedAction: "CONTACT_SUPPORT",
		},
	}
	healthEvent_13 = datamodels.HealthEventWithStatus{
		CreatedAt: time.Now(),
		HealthEvent: &protos.HealthEvent{
			Version:        1,
			Agent:          "gpu-health-monitor",
			ComponentClass: "GPU",
			CheckName:      "GpuXidError",
			IsFatal:        true,
			IsHealthy:      false,
			Message:        "XID error occurred",
			ErrorCode:      []string{"13"},
			EntitiesImpacted: []*protos.Entity{{
				EntityType:  "GPU",
				EntityValue: "1",
			}},
			Metadata: map[string]string{
				"SerialNumber": "1655322004581",
			},
			GeneratedTimestamp: &timestamppb.Timestamp{
				Seconds: time.Now().Unix(),
				Nanos:   0,
			},
			NodeName: "node1",
		},
		HealthEventStatus: datamodels.HealthEventStatus{},
	}
	healthEvent_48 = datamodels.HealthEventWithStatus{
		CreatedAt: time.Now(),
		HealthEvent: &protos.HealthEvent{
			Version:        1,
			Agent:          "gpu-health-monitor",
			ComponentClass: "GPU",
			CheckName:      "GpuXidError",
			IsFatal:        true,
			IsHealthy:      false,
			Message:        "XID error occurred",
			ErrorCode:      []string{"48"},
			EntitiesImpacted: []*protos.Entity{{
				EntityType:  "GPU",
				EntityValue: "1",
			}},
			Metadata: map[string]string{
				"SerialNumber": "1655322004581",
			},
			GeneratedTimestamp: &timestamppb.Timestamp{
				Seconds: time.Now().Unix(),
				Nanos:   0,
			},
			NodeName: "node1",
		},
		HealthEventStatus: datamodels.HealthEventStatus{},
	}
)

func TestHandleEvent(t *testing.T) {

	ctx := context.Background()

	t.Run("rule matches and event is published", func(t *testing.T) {
		// Create fresh mock instances for this test
		mockClient := new(mockCollectionClient)
		mockPublisher := &mockPublisher{}
		cfg := HealthEventsAnalyzerReconcilerConfig{
			HealthEventsAnalyzerRules: &config.TomlConfig{Rules: []config.HealthEventsAnalyzerRule{rules[1]}},
			CollectionClient:          mockClient,
			Publisher:                 publisher.NewPublisher(mockPublisher),
		}
		reconciler := NewReconciler(cfg)

		// Create the expected health event that the publisher will create (transformed)
		expectedTransformedEvent := &protos.HealthEvent{
			Version:            healthEvent_13.HealthEvent.Version,
			Agent:              "health-events-analyzer", // Publisher sets this
			CheckName:          "rule2",                  // Publisher sets this to ruleName
			ComponentClass:     healthEvent_13.HealthEvent.ComponentClass,
			Message:            healthEvent_13.HealthEvent.Message,
			RecommendedAction:  protos.RecommendedAction_CONTACT_SUPPORT, // From rule2
			ErrorCode:          healthEvent_13.HealthEvent.ErrorCode,
			IsHealthy:          false, // Publisher sets this
			IsFatal:            true,  // Publisher sets this
			EntitiesImpacted:   healthEvent_13.HealthEvent.EntitiesImpacted,
			Metadata:           healthEvent_13.HealthEvent.Metadata,
			GeneratedTimestamp: healthEvent_13.HealthEvent.GeneratedTimestamp,
			NodeName:           healthEvent_13.HealthEvent.NodeName,
		}
		expectedHealthEvents := &protos.HealthEvents{
			Version: 1,
			Events:  []*protos.HealthEvent{expectedTransformedEvent},
		}

		mockPublisher.On("HealthEventOccurredV1", ctx, expectedHealthEvents).Return(&emptypb.Empty{}, nil)

		mockCursor, _ := createMockCursor([]bson.M{{"ruleMatched": true}})
		mockClient.On("Aggregate", ctx, mock.Anything, mock.Anything).Return(mockCursor, nil)

		published, _ := reconciler.handleEvent(ctx, &healthEvent_13)
		assert.True(t, published)
		mockClient.AssertExpectations(t)
		mockPublisher.AssertExpectations(t)
	})

	t.Run("match multiple remediations rule", func(t *testing.T) {
		// Create fresh mock instances for this test
		mockClient := new(mockCollectionClient)
		mockPublisher := &mockPublisher{}
		cfg := HealthEventsAnalyzerReconcilerConfig{
			HealthEventsAnalyzerRules: &config.TomlConfig{Rules: rules},
			CollectionClient:          mockClient,
			Publisher:                 publisher.NewPublisher(mockPublisher),
		}
		reconciler := NewReconciler(cfg)

		// This test uses all rules, so rule1 (IsMultipleRemediationsRule: true) will match
		expectedTransformedEvent := &protos.HealthEvent{
			Version:            healthEvent_13.HealthEvent.Version,
			Agent:              "health-events-analyzer", // Publisher sets this
			CheckName:          "rule1",                  // Publisher sets this to ruleName
			ComponentClass:     healthEvent_13.HealthEvent.ComponentClass,
			Message:            healthEvent_13.HealthEvent.Message,
			RecommendedAction:  protos.RecommendedAction_CONTACT_SUPPORT,
			ErrorCode:          healthEvent_13.HealthEvent.ErrorCode,
			IsHealthy:          false, // Publisher sets this
			IsFatal:            true,  // Publisher sets this
			EntitiesImpacted:   healthEvent_13.HealthEvent.EntitiesImpacted,
			Metadata:           healthEvent_13.HealthEvent.Metadata,
			GeneratedTimestamp: healthEvent_13.HealthEvent.GeneratedTimestamp,
			NodeName:           healthEvent_13.HealthEvent.NodeName,
		}
		expectedHealthEvents := &protos.HealthEvents{
			Version: 1,
			Events:  []*protos.HealthEvent{expectedTransformedEvent},
		}

		mockPublisher.On("HealthEventOccurredV1", ctx, expectedHealthEvents).Return(&emptypb.Empty{}, nil)
		mockCursor, _ := createMockCursor([]bson.M{{"ruleMatched": true}})
		mockClient.On("Aggregate", ctx, mock.Anything, mock.Anything).Return(mockCursor, nil)

		published, _ := reconciler.handleEvent(ctx, &healthEvent_13)
		assert.True(t, published)
		mockClient.AssertExpectations(t)
		mockPublisher.AssertExpectations(t)
	})

	t.Run("received event with different XID", func(t *testing.T) {
		mockClient := new(mockCollectionClient)
		mockPublisher := &mockPublisher{}
		cfg := HealthEventsAnalyzerReconcilerConfig{
			HealthEventsAnalyzerRules: &config.TomlConfig{Rules: rules},
			CollectionClient:          mockClient,
			Publisher:                 publisher.NewPublisher(mockPublisher),
		}
		reconciler := NewReconciler(cfg)

		mockCursor, _ := createMockCursor([]bson.M{{"ruleMatched": false}})
		mockClient.On("Aggregate", ctx, mock.Anything, mock.Anything).Return(mockCursor, nil)

		published, _ := reconciler.handleEvent(ctx, &healthEvent_48)
		assert.False(t, published)
		mockClient.AssertExpectations(t)
		mockPublisher.AssertNotCalled(t, "HealthEventOccurredV1")
	})
	t.Run("one sequence matched", func(t *testing.T) {
		mockClient := new(mockCollectionClient)
		mockPublisher := &mockPublisher{}
		cfg := HealthEventsAnalyzerReconcilerConfig{
			HealthEventsAnalyzerRules: &config.TomlConfig{Rules: rules},
			CollectionClient:          mockClient,
			Publisher:                 publisher.NewPublisher(mockPublisher),
		}
		reconciler := NewReconciler(cfg)
		healthEvent_13.HealthEvent.ErrorCode = []string{"31"}

		mockCursor, _ := createMockCursor([]bson.M{{"ruleMatched": false}})
		mockClient.On("Aggregate", ctx, mock.Anything, mock.Anything).Return(mockCursor, nil)

		published, _ := reconciler.handleEvent(ctx, &healthEvent_13)
		assert.False(t, published)
		mockClient.AssertExpectations(t)
		mockPublisher.AssertNotCalled(t, "HealthEventOccurredV1")

		healthEvent_13.HealthEvent.ErrorCode = []string{"13"}
	})

	t.Run("empty rules list", func(t *testing.T) {
		mockClient := new(mockCollectionClient)
		mockPublisher := &mockPublisher{}
		cfg := HealthEventsAnalyzerReconcilerConfig{
			HealthEventsAnalyzerRules: &config.TomlConfig{Rules: []config.HealthEventsAnalyzerRule{}},
			CollectionClient:          mockClient,
			Publisher:                 publisher.NewPublisher(mockPublisher),
		}
		reconciler := NewReconciler(cfg)

		published, err := reconciler.handleEvent(ctx, &healthEvent_13)
		assert.NoError(t, err)
		assert.False(t, published)
		mockClient.AssertNotCalled(t, "Aggregate")
		mockPublisher.AssertNotCalled(t, "HealthEventOccurredV1")
	})
}
