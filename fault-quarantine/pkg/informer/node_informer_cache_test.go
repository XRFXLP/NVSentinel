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

package informer

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/config"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/nodecache"
)

func TestNodeCacheTransform_RetainAll_StripsOnlyStatus(t *testing.T) {
	input := testFullNode()
	wantMetadata := input.ObjectMeta.DeepCopy()
	wantSpec := input.Spec.DeepCopy()

	// The zero value retains every key, so this is the status-only transform
	// the informer used before the retained set was derived from the rules.
	transformed, err := nodecache.Keys{}.Transform()(input)
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}

	node, ok := transformed.(*v1.Node)
	if !ok {
		t.Fatalf("Transform() returned %T", transformed)
	}

	if node != input {
		t.Fatal("Transform() returned a copy instead of mutating in place")
	}
	if !reflect.DeepEqual(node.ObjectMeta, *wantMetadata) {
		t.Fatalf("cached node metadata changed:\n got: %#v\nwant: %#v", node.ObjectMeta, *wantMetadata)
	}
	if !reflect.DeepEqual(node.Spec, *wantSpec) {
		t.Fatalf("cached node spec changed:\n got: %#v\nwant: %#v", node.Spec, *wantSpec)
	}
	if !reflect.DeepEqual(node.Status, v1.NodeStatus{}) {
		t.Fatalf("cached node retained status: %#v", node.Status)
	}
}

func TestNewNodeInformer_DerivedKeys_CachesOnlyRetainedEntries(t *testing.T) {
	client := fake.NewClientset(testFullNode())

	retained := nodecache.Derive(testRuleConfig(), nodecache.Operational{
		GPUNodeLabelKey: GPUNodeLabel,
		LabelPrefix:     "k8saas.nvidia.com/",
	})

	nodeInformer, err := NewNodeInformer(client, 0, GPUNodeLabel, GPUNodeLabelValue, retained)
	if err != nil {
		t.Fatalf("NewNodeInformer() error = %v", err)
	}

	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })

	if err := nodeInformer.Run(stopCh); err != nil {
		t.Fatalf("NodeInformer.Run() error = %v", err)
	}

	node, err := nodeInformer.GetNode("test-node")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}

	if !reflect.DeepEqual(node.Status, v1.NodeStatus{}) {
		t.Fatalf("cached node retained status: %#v", node.Status)
	}

	// The rule reads this key, so it survives.
	if node.Labels["opt-out"] != "false" {
		t.Fatalf("cached node dropped a label the rules read: %#v", node.Labels)
	}

	// The circuit breaker selects on this key, so it survives even though no
	// rule mentions it.
	if node.Labels[GPUNodeLabel] != GPUNodeLabelValue {
		t.Fatalf("cached node dropped the GPU label the breaker selects on: %#v", node.Labels)
	}

	// Nothing reads this one.
	if _, present := node.Labels["label"]; present {
		t.Fatalf("cached node retained a label nothing reads: %#v", node.Labels)
	}
	if _, present := node.Annotations["annotation"]; present {
		t.Fatalf("cached node retained an annotation nothing reads: %#v", node.Annotations)
	}

	// Spec is never pruned: the cordon path and untaint detection read it.
	if node.Spec.PodCIDR != "10.0.0.0/24" || !node.Spec.Unschedulable {
		t.Fatalf("cached node is missing spec fields: %#v", node.Spec)
	}
}

// testRuleConfig is a ruleset of the shape the chart ships: one Node rule that
// guards a label read with `in`, so that an operator can opt a node out.
func testRuleConfig() config.TomlConfig {
	return config.TomlConfig{
		LabelPrefix: "k8saas.nvidia.com/",
		RuleSets: []config.QuarantineRuleSet{{
			RuleSetMeta: config.RuleSetMeta{
				Enabled: true,
				Name:    "test",
				Match: config.Match{
					All: []config.Rule{{
						Kind:       "Node",
						Expression: `!('opt-out' in node.metadata.labels && node.metadata.labels['opt-out'] == "false")`,
					}},
				},
			},
		}},
	}
}

func BenchmarkNodeCacheTransform(b *testing.B) {
	fullNode := testFullNode()
	fullJSON, err := json.Marshal(fullNode)
	if err != nil {
		b.Fatalf("json.Marshal(full node) error = %v", err)
	}

	retained := nodecache.Derive(testRuleConfig(), nodecache.Operational{
		GPUNodeLabelKey: GPUNodeLabel,
		LabelPrefix:     "k8saas.nvidia.com/",
	})
	transform := retained.Transform()

	transformed, err := transform(fullNode)
	if err != nil {
		b.Fatalf("Transform() error = %v", err)
	}
	slimJSON, err := json.Marshal(transformed)
	if err != nil {
		b.Fatalf("json.Marshal(slim node) error = %v", err)
	}

	b.ResetTimer()

	for b.Loop() {
		if _, err := transform(fullNode); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(len(fullJSON)), "full-json-bytes")
	b.ReportMetric(float64(len(slimJSON)), "cached-json-bytes")
}

func testFullNode() *v1.Node {
	return &v1.Node{
		APIVersion: "v1", Kind: "Node",
		Name:            "test-node",
		UID:             types.UID("test-uid"),
		ResourceVersion: "42",
		Labels: map[string]string{
			"label":      "value",
			"opt-out":    "false",
			GPUNodeLabel: GPUNodeLabelValue,
		},
		Annotations:     map[string]string{"annotation": "value"},
		OwnerReferences: []metav1.OwnerReference{{Name: "owner"}},
		ManagedFields:   []metav1.ManagedFieldsEntry{{Manager: "manager"}},
		Spec: v1.NodeSpec{
			Unschedulable: true,
			Taints: []v1.Taint{{
				Key:       "key",
				Value:     "value",
				Effect:    v1.TaintEffectNoSchedule,
				TimeAdded: &metav1.Time{Time: time.Unix(1, 0)},
			}},
			PodCIDR: "10.0.0.0/24",
		},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{v1.ResourceCPU: resource.MustParse("8")},
			Conditions: []v1.NodeCondition{{
				Type:   v1.NodeReady,
				Status: v1.ConditionTrue,
			}},
		},
	}
}
