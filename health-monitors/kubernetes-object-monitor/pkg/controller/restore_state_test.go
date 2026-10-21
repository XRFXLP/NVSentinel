// Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
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

package controller_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/config"
	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/controller"
)

func TestRestoreState_SavedMatches_QueuesOnlyMatchesOfEnabledPolicies(t *testing.T) {
	podGVK := schema.GroupVersionKind{Version: "v1", Kind: "Pod"}
	policies := []config.Policy{
		{Name: "pod-health", Enabled: true},
		{Name: "pod-disabled", Enabled: false},
	}

	tests := []struct {
		name      string
		matches   map[string]string
		wantQueue []types.NamespacedName
	}{
		{
			name:      "namespaced object",
			matches:   map[string]string{"pod-health/gpu-operator/operator-0": "node-a"},
			wantQueue: []types.NamespacedName{{Namespace: "gpu-operator", Name: "operator-0"}},
		},
		{
			name:      "cluster-scoped object",
			matches:   map[string]string{"pod-health/node-a": "node-a"},
			wantQueue: []types.NamespacedName{{Name: "node-a"}},
		},
		{
			name:    "disabled policy",
			matches: map[string]string{"pod-disabled/gpu-operator/operator-0": "node-a"},
		},
		{
			name:    "policy of another reconciler",
			matches: map[string]string{"node-not-ready/node-a": "node-a"},
		},
		{
			name:    "policy name is only a prefix of the key's policy",
			matches: map[string]string{"pod-health-extra/gpu-operator/operator-0": "node-a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reconciler := controller.NewResourceReconciler(nil, nil, nil, nil, policies, podGVK)

			events := reconciler.RestoreState(tt.matches)

			queued := make([]types.NamespacedName, 0, len(events))

			for _, e := range events {
				require.Equal(t, podGVK, e.Object.GetObjectKind().GroupVersionKind())
				queued = append(queued, types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()})
			}

			assert.ElementsMatch(t, tt.wantQueue, queued)
		})
	}
}
