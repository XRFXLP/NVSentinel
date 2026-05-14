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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

func TestKey(t *testing.T) {
	tests := []struct {
		name  string
		left  *pb.HealthEvent
		right *pb.HealthEvent
		equal bool
	}{
		{
			name: "canonicalizes entity and error code order",
			left: &pb.HealthEvent{
				NodeName:  "node-a",
				CheckName: "SysLogsXIDError",
				ErrorCode: []string{"79", "31"},
				EntitiesImpacted: []*pb.Entity{
					{EntityType: "gpu", EntityValue: "GPU-1"},
					{EntityType: "pci", EntityValue: "0000:b3:00.0"},
				},
			},
			right: &pb.HealthEvent{
				NodeName:  "node-a",
				CheckName: "SysLogsXIDError",
				ErrorCode: []string{"31", "79"},
				EntitiesImpacted: []*pb.Entity{
					{EntityType: "pci", EntityValue: "0000:b3:00.0"},
					{EntityType: "gpu", EntityValue: "GPU-1"},
				},
			},
			equal: true,
		},
		{
			name: "includes check name",
			left: &pb.HealthEvent{
				NodeName:         "node-a",
				CheckName:        "InfiniBandStateCheck",
				EntitiesImpacted: []*pb.Entity{{EntityType: "port", EntityValue: "mlx5_0/1"}},
			},
			right: &pb.HealthEvent{
				NodeName:         "node-a",
				CheckName:        "InfiniBandDegradationCheck",
				EntitiesImpacted: []*pb.Entity{{EntityType: "port", EntityValue: "mlx5_0/1"}},
			},
			equal: false,
		},
		{
			name: "includes health state",
			left: &pb.HealthEvent{
				NodeName:         "node-a",
				CheckName:        "InfiniBandStateCheck",
				EntitiesImpacted: []*pb.Entity{{EntityType: "port", EntityValue: "mlx5_0/1"}},
			},
			right: &pb.HealthEvent{
				NodeName:         "node-a",
				CheckName:        "InfiniBandStateCheck",
				IsHealthy:        true,
				EntitiesImpacted: []*pb.Entity{{EntityType: "port", EntityValue: "mlx5_0/1"}},
			},
			equal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.equal {
				assert.Equal(t, Key(tt.left), Key(tt.right))
			} else {
				assert.NotEqual(t, Key(tt.left), Key(tt.right))
			}
		})
	}
}

func TestTrackerDeduplicatesWithinTTLAndReemitsAfterExpiry(t *testing.T) {
	now := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)
	tracker := NewTracker(3*time.Minute, WithNow(func() time.Time { return now }))
	event := &pb.HealthEvent{NodeName: "node-a", CheckName: "SysLogsXIDError", ErrorCode: []string{"79"}}

	require.False(t, tracker.IsDuplicate(event))
	tracker.Mark(event)

	assert.True(t, tracker.IsDuplicate(event))

	now = now.Add(3 * time.Minute)

	assert.False(t, tracker.IsDuplicate(event))
	assert.Empty(t, tracker.seen)
}

func TestTrackerEvictExpiredRemovesStaleEntries(t *testing.T) {
	now := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)
	tracker := NewTracker(3*time.Minute, WithNow(func() time.Time { return now }))
	stale := &pb.HealthEvent{NodeName: "node-a", CheckName: "SysLogsXIDError", ErrorCode: []string{"79"}}
	fresh := &pb.HealthEvent{NodeName: "node-a", CheckName: "SysLogsSXIDError", ErrorCode: []string{"95"}}

	tracker.Mark(stale)
	now = now.Add(2 * time.Minute)
	tracker.Mark(fresh)
	now = now.Add(90 * time.Second)

	tracker.EvictExpired()

	assert.False(t, tracker.IsDuplicate(stale))
	assert.True(t, tracker.IsDuplicate(fresh))
}

func TestClearUnhealthyCounterpart(t *testing.T) {
	now := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)
	tracker := NewTracker(3*time.Minute, WithNow(func() time.Time { return now }))
	unhealthy := &pb.HealthEvent{
		NodeName:  "node-a",
		CheckName: "SysLogsXIDError",
		ErrorCode: []string{"79"},
		EntitiesImpacted: []*pb.Entity{
			{EntityType: "gpu", EntityValue: "GPU-1"},
		},
	}
	healthy := &pb.HealthEvent{
		NodeName:  unhealthy.NodeName,
		CheckName: unhealthy.CheckName,
		ErrorCode: append([]string(nil), unhealthy.ErrorCode...),
		IsHealthy: true,
		EntitiesImpacted: []*pb.Entity{
			{EntityType: "gpu", EntityValue: "GPU-1"},
		},
	}

	tracker.Mark(unhealthy)
	tracker.Mark(healthy)

	require.True(t, tracker.IsDuplicate(unhealthy))
	require.True(t, tracker.IsDuplicate(healthy))

	tracker.ClearUnhealthyCounterpart(healthy)

	assert.False(t, tracker.IsDuplicate(unhealthy))
	assert.True(t, tracker.IsDuplicate(healthy))
}

func TestClearUnhealthyCounterpartNoopForUnhealthyEvent(t *testing.T) {
	now := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)
	tracker := NewTracker(3*time.Minute, WithNow(func() time.Time { return now }))
	event := &pb.HealthEvent{NodeName: "node-a", CheckName: "SysLogsXIDError"}

	tracker.Mark(event)
	tracker.ClearUnhealthyCounterpart(event)

	assert.True(t, tracker.IsDuplicate(event))
}
