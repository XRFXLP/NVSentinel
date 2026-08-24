// Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
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

package kubeclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func node(labels, annotations map[string]string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "node-1",
			ResourceVersion: "1",
			Labels:          labels,
			Annotations:     annotations,
		},
	}
}

func TestNodeMergePatch_MetadataChanges_ReturnsExpectedPatch(t *testing.T) {
	tests := []struct {
		name     string
		original *v1.Node
		modified *v1.Node
		expected string
	}{
		{
			name:     "no change produces no patch",
			original: node(map[string]string{"a": "1"}, map[string]string{"b": "2"}),
			modified: node(map[string]string{"a": "1"}, map[string]string{"b": "2"}),
			expected: "",
		},
		{
			name:     "adds a label",
			original: node(map[string]string{"a": "1"}, nil),
			modified: node(map[string]string{"a": "1", "b": "2"}, nil),
			expected: `{"metadata":{"labels":{"b":"2"},"resourceVersion":"1"}}`,
		},
		{
			name:     "changes a label without mentioning the others",
			original: node(map[string]string{"a": "1", "b": "2"}, nil),
			modified: node(map[string]string{"a": "9", "b": "2"}, nil),
			expected: `{"metadata":{"labels":{"a":"9"},"resourceVersion":"1"}}`,
		},
		{
			name:     "removes a label with an explicit null",
			original: node(map[string]string{"a": "1", "b": "2"}, nil),
			modified: node(map[string]string{"a": "1"}, nil),
			expected: `{"metadata":{"labels":{"b":null},"resourceVersion":"1"}}`,
		},
		{
			name:     "adds an annotation",
			original: node(nil, nil),
			modified: node(nil, map[string]string{"bootstrap": "true"}),
			expected: `{"metadata":{"annotations":{"bootstrap":"true"},"resourceVersion":"1"}}`,
		},
		{
			name:     "carries labels and annotations in a single patch",
			original: node(map[string]string{"a": "1"}, nil),
			modified: node(map[string]string{"a": "2"}, map[string]string{"bootstrap": "true"}),
			expected: `{"metadata":{"annotations":{"bootstrap":"true"},"labels":{"a":"2"},"resourceVersion":"1"}}`,
		},
		{
			name:     "a nil map and an empty map are the same thing",
			original: node(nil, nil),
			modified: node(map[string]string{}, map[string]string{}),
			expected: "",
		},
		{
			name:     "sets a label onto a node that had none",
			original: node(nil, nil),
			modified: node(map[string]string{"a": "1"}, nil),
			expected: `{"metadata":{"labels":{"a":"1"},"resourceVersion":"1"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patch, err := NodeMergePatch(tt.original, tt.modified)
			require.NoError(t, err)

			if tt.expected == "" {
				assert.Nil(t, patch, "equivalent nodes must not cost an API call")
				return
			}

			assert.JSONEq(t, tt.expected, string(patch))
		})
	}
}

// TestNodeMergePatchLeavesProjectedFieldsAlone pins the reason the patch is built key
// by key. Informer caches often hold a projected Node — the labeler's transform keeps
// only one annotation and clears Spec entirely — and a patch derived from that
// projection must not describe the fields the projection dropped, or it would erase
// them on the real object.
func TestNodeMergePatch_ProjectedFields_LeavesThemAlone(t *testing.T) {
	projected := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "node-1",
			ResourceVersion: "1",
			Labels:          map[string]string{"gpu": "true"},
			Annotations:     map[string]string{"kept": "yes"},
		},
	}

	modified := projected.DeepCopy()
	modified.Labels["driver.installed"] = "true"

	patch, err := NodeMergePatch(projected, modified)
	require.NoError(t, err)

	assert.JSONEq(t,
		`{"metadata":{"labels":{"driver.installed":"true"},"resourceVersion":"1"}}`,
		string(patch),
	)
	assert.NotContains(t, string(patch), "annotations",
		"an untouched annotation must not appear in the patch")
	assert.NotContains(t, string(patch), "spec",
		"a cleared Spec must never reach the patch, or real taints would be dropped")
}

func TestNodeMergePatch_SpecChanges_ReturnsNoPatch(t *testing.T) {
	original := node(nil, nil)
	modified := original.DeepCopy()
	modified.Spec.Unschedulable = true
	modified.Spec.Taints = []v1.Taint{{Key: "held", Effect: v1.TaintEffectNoSchedule}}

	patch, err := NodeMergePatch(original, modified)
	require.NoError(t, err)

	assert.Nil(t, patch, "spec is out of scope until a caller needs it")
}
