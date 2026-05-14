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

package dedup

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commondedup "github.com/nvidia/nvsentinel/commons/pkg/dedup"
	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

func TestDeduplicatorFiltersDuplicateEvents(t *testing.T) {
	now := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)
	tracker := commondedup.NewTracker(3*time.Minute, commondedup.WithNow(func() time.Time { return now }))
	filter := NewDeduplicator(tracker, nil)
	event := &pb.HealthEvent{
		NodeName:         "node-a",
		CheckName:        "SysLogsXIDError",
		ErrorCode:        []string{"79"},
		EntitiesImpacted: []*pb.Entity{{EntityType: "gpu", EntityValue: "GPU-1"}},
	}

	keep, err := filter.Filter(context.Background(), event)
	require.NoError(t, err)
	assert.True(t, keep)

	keep, err = filter.Filter(context.Background(), event)
	require.NoError(t, err)
	assert.False(t, keep)
}

func TestDeduplicatorSkipChecksAlwaysKeep(t *testing.T) {
	now := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)
	tracker := commondedup.NewTracker(3*time.Minute, commondedup.WithNow(func() time.Time { return now }))
	filter := NewDeduplicator(tracker, []string{"SysLogsGPUFallenOff"})
	event := &pb.HealthEvent{NodeName: "node-a", CheckName: "SysLogsGPUFallenOff"}

	for range 2 {
		keep, err := filter.Filter(context.Background(), event)
		require.NoError(t, err)
		assert.True(t, keep)
	}
}

func TestDeduplicatorHealthyClearsUnhealthyCounterpart(t *testing.T) {
	now := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)
	tracker := commondedup.NewTracker(3*time.Minute, commondedup.WithNow(func() time.Time { return now }))
	filter := NewDeduplicator(tracker, nil)
	unhealthy := &pb.HealthEvent{
		NodeName:         "node-a",
		CheckName:        "SysLogsXIDError",
		ErrorCode:        []string{"79"},
		EntitiesImpacted: []*pb.Entity{{EntityType: "gpu", EntityValue: "GPU-1"}},
	}
	healthy := &pb.HealthEvent{
		NodeName:         unhealthy.NodeName,
		CheckName:        unhealthy.CheckName,
		ErrorCode:        append([]string(nil), unhealthy.ErrorCode...),
		IsHealthy:        true,
		EntitiesImpacted: []*pb.Entity{{EntityType: "gpu", EntityValue: "GPU-1"}},
	}

	keep, err := filter.Filter(context.Background(), unhealthy)
	require.NoError(t, err)
	require.True(t, keep)

	keep, err = filter.Filter(context.Background(), healthy)
	require.NoError(t, err)
	require.True(t, keep)

	keep, err = filter.Filter(context.Background(), unhealthy)
	require.NoError(t, err)
	assert.True(t, keep)

	keep, err = filter.Filter(context.Background(), healthy)
	require.NoError(t, err)
	assert.False(t, keep)
}

func TestErrCodeLabelCanonicalizesErrorCodes(t *testing.T) {
	event := &pb.HealthEvent{ErrorCode: []string{"95", "79"}}

	assert.Equal(t, "79,95", errCodeLabel(event))
	assert.Equal(t, noErrorCodeLabel, errCodeLabel(&pb.HealthEvent{}))
}
