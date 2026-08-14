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

package devicecounts

import (
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/client-go/tools/cache"
)

const NodeResourceSliceIndex = "nodeResourceSlice"

// ResourceSlicesForNode returns node-local ResourceSlices whose spec.nodeName matches the node.
// Uses the NodeResourceSliceIndex for O(slices_per_node) lookup; falls back to full scan if the
// index is unavailable (e.g. store does not implement cache.Indexer).
func ResourceSlicesForNode(store cache.Store, node *corev1.Node) []*resourcev1.ResourceSlice {
	if store == nil {
		return nil
	}

	if indexer, ok := store.(cache.Indexer); ok {
		objs, err := indexer.ByIndex(NodeResourceSliceIndex, node.Name)
		if err == nil {
			slices := make([]*resourcev1.ResourceSlice, 0, len(objs))
			for _, obj := range objs {
				if rs, ok := obj.(*resourcev1.ResourceSlice); ok {
					slices = append(slices, rs)
				}
			}
			slog.Debug("ResourceSlicesForNode via index",
				"node", node.Name,
				"found", len(slices),
			)
			return slices
		}
	}

	// Fallback: full scan (O(S) — used only if index is missing)
	all := store.List()
	resourceSlices := []*resourcev1.ResourceSlice{}
	for _, obj := range all {
		resourceSlice, ok := obj.(*resourcev1.ResourceSlice)
		if !ok {
			continue
		}
		if resourceSliceBelongsToNode(resourceSlice, node.Name) {
			resourceSlices = append(resourceSlices, resourceSlice)
		}
	}
	slog.Debug("ResourceSlicesForNode via scan",
		"node", node.Name,
		"found", len(resourceSlices),
		"scanned", len(all),
	)
	return resourceSlices
}

func resourceSliceBelongsToNode(resourceSlice *resourcev1.ResourceSlice, nodeName string) bool {
	resourceSliceNodeName, ok := resourceSliceNodeName(resourceSlice)

	return ok && resourceSliceNodeName == nodeName
}

func resourceSliceNodeName(resourceSlice *resourcev1.ResourceSlice) (string, bool) {
	if resourceSlice == nil || resourceSlice.Spec.NodeName == nil || *resourceSlice.Spec.NodeName == "" {
		return "", false
	}

	return *resourceSlice.Spec.NodeName, true
}
