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
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
)

// NodeMergePatch builds an RFC 7386 JSON merge patch carrying the label and
// annotation differences between original and modified. It returns a nil patch when
// the two already agree, so callers can skip the write instead of spending an API
// call on a no-op.
//
// The patch is assembled key by key rather than by marshalling modified, because
// marshalling a Node emits every populated field. A caller that read original from an
// informer cache holding a projected Node would then patch the projection's gaps back
// over the real object. Emitting only keys that differ means fields missing from both
// sides are left untouched.
//
// Spec fields such as taints and unschedulable are deliberately out of scope: a merge
// patch replaces a list wholesale, so patching taints from a projected Node whose Spec
// had been cleared would silently drop every taint on the real object.
func NodeMergePatch(original, modified *v1.Node) ([]byte, error) {
	metadata := map[string]any{}

	if labels := stringMapMergePatch(original.Labels, modified.Labels); labels != nil {
		metadata["labels"] = labels
	}

	if annotations := stringMapMergePatch(original.Annotations, modified.Annotations); annotations != nil {
		metadata["annotations"] = annotations
	}

	if len(metadata) == 0 {
		return nil, nil
	}

	patch, err := json.Marshal(map[string]any{"metadata": metadata})
	if err != nil {
		return nil, fmt.Errorf("marshal merge patch for node %s: %w", original.Name, err)
	}

	return patch, nil
}

// stringMapMergePatch returns the merge patch entries that turn original into
// modified: added and changed keys map to their new value, removed keys map to nil so
// the API server deletes them. It returns nil when the two maps already agree.
func stringMapMergePatch(original, modified map[string]string) map[string]any {
	patch := map[string]any{}

	for key, value := range modified {
		if current, exists := original[key]; !exists || current != value {
			patch[key] = value
		}
	}

	for key := range original {
		if _, exists := modified[key]; !exists {
			patch[key] = nil
		}
	}

	if len(patch) == 0 {
		return nil
	}

	return patch
}
