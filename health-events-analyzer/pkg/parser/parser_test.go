// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"reflect"
	"testing"

	datamodels "github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"go.mongodb.org/mongo-driver/bson"
)

func TestParseSequenceString(t *testing.T) {
	remediated := true

	tests := []struct {
		name     string
		criteria map[string]any
		event    datamodels.HealthEventWithStatus
		want     bson.D
		wantErr  bool
	}{
		{
			name: "criteria with fault remediation check and .this reference",
			criteria: map[string]any{
				"healtheventstatus.faultremediated": true,
				"healthevent.nodename":              "this.healthevent.nodename",
				"healthevent.isfatal":               "this.healthevent.isfatal",
			},
			event: datamodels.HealthEventWithStatus{
				HealthEvent: &protos.HealthEvent{
					NodeName: "gpu-node-1",
					IsFatal:  true,
				},
				HealthEventStatus: datamodels.HealthEventStatus{
					FaultRemediated: &remediated,
				},
			},
			want: bson.D{
				{Key: "healtheventstatus.faultremediated", Value: true},
				{Key: "healthevent.nodename", Value: "gpu-node-1"},
				{Key: "healthevent.isfatal", Value: true},
			},
			wantErr: false,
		},
		{
			name: "criteria with MongoDB $ne operator",
			criteria: map[string]any{
				"healthevent.nodename": "this.healthevent.nodename",
				"healthevent.agent":    `{"$ne":"health-events-analyzer"}`,
			},
			event: datamodels.HealthEventWithStatus{
				HealthEvent: &protos.HealthEvent{
					NodeName: "test-node",
					Agent:    "health-events-analyzer",
				},
			},
			want: bson.D{
				{Key: "healthevent.nodename", Value: "test-node"},
				{Key: "healthevent.agent", Value: map[string]any{"$ne": "health-events-analyzer"}},
			},
			wantErr: false,
		},
		{
			name: "criteria with errorcode array access",
			criteria: map[string]any{
				"healthevent.errorcode.0": "this.healthevent.errorcode.0",
				"healthevent.nodename":    "this.healthevent.nodename",
			},
			event: datamodels.HealthEventWithStatus{
				HealthEvent: &protos.HealthEvent{
					NodeName:  "node2",
					ErrorCode: []string{"48"},
				},
			},
			want: bson.D{
				{Key: "healthevent.errorcode.0", Value: "48"},
				{Key: "healthevent.nodename", Value: "node2"},
			},
			wantErr: false,
		},
		{
			name: "criteria with mixed types - booleans, strings, and references",
			criteria: map[string]any{
				"healthevent.isfatal":                             true,
				"healthevent.ishealthy":                           false,
				"healthevent.checkname":                           "this.healthevent.checkname",
				"healthevent.nodename":                            "this.healthevent.nodename",
				"healthevent.agent":                               "gpu-health-monitor",
				"healtheventstatus.userpodsevictionstatus.status": "this.healtheventstatus.userpodsevictionstatus.status",
			},
			event: datamodels.HealthEventWithStatus{
				HealthEvent: &protos.HealthEvent{
					NodeName:  "node2",
					CheckName: "GpuXidError",
					IsFatal:   true,
					IsHealthy: false,
					Agent:     "gpu-health-monitor",
				},
				HealthEventStatus: datamodels.HealthEventStatus{
					UserPodsEvictionStatus: datamodels.OperationStatus{
						Status:  datamodels.StatusInProgress,
						Message: "Evicting pods from node",
					},
				},
			},
			want: bson.D{
				{Key: "healthevent.isfatal", Value: true},
				{Key: "healthevent.ishealthy", Value: false},
				{Key: "healthevent.checkname", Value: "GpuXidError"},
				{Key: "healthevent.nodename", Value: "node2"},
				{Key: "healthevent.agent", Value: "gpu-health-monitor"},
				{Key: "healtheventstatus.userpodsevictionstatus.status", Value: datamodels.StatusInProgress},
			},
			wantErr: false,
		},
		{
			name: "criteria with complex $in operator",
			criteria: map[string]any{
				"healthevent.checkname": `{"$in":["GpuXidError","GpuMemWatch","GpuNvswitchFatalWatch"]}`,
				"healthevent.nodename":  "this.healthevent.nodename",
			},
			event: datamodels.HealthEventWithStatus{
				HealthEvent: &protos.HealthEvent{
					NodeName:  "check-node",
					CheckName: "XID_ERROR",
				},
			},
			want: bson.D{
				{Key: "healthevent.checkname", Value: map[string]any{"$in": []any{"GpuXidError", "GpuMemWatch", "GpuNvswitchFatalWatch"}}},
				{Key: "healthevent.nodename", Value: "check-node"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSequenceString(tt.criteria, tt.event)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSequenceString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !bsonDEqual(got, tt.want) {
				t.Errorf("ParseSequenceString() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Helper function to compare bson.D values
func bsonDEqual(a, b bson.D) bool {
	if len(a) != len(b) {
		return false
	}

	aMap := make(map[string]any)
	bMap := make(map[string]any)

	for _, elem := range a {
		aMap[elem.Key] = elem.Value
	}
	for _, elem := range b {
		bMap[elem.Key] = elem.Value
	}

	// Compare maps
	return reflect.DeepEqual(aMap, bMap)
}
