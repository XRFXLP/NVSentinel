// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package initializer

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	toolscache "k8s.io/client-go/tools/cache"
)

// mandatoryPaths are kept regardless of what the policies read. The first four
// are required by the informer itself for keying, watch bookkeeping and
// deletion handling; name and namespace are additionally read directly by the
// reconciler when it builds the health event and its dedup key.
var mandatoryPaths = [][]string{
	{"apiVersion"},
	{"kind"},
	{"metadata", "name"},
	{"metadata", "namespace"},
	{"metadata", "uid"},
	{"metadata", "resourceVersion"},
	{"metadata", "deletionTimestamp"},
}

// pruningTransform returns a cache.TransformFunc that keeps only the given
// paths and drops everything else from the object.
//
// Objects are cached as unstructured, which costs roughly twelve bytes of heap
// per byte of object: every field becomes a map entry with a boxed value and
// its own allocation. Nothing recovers that overhead for fields no policy ever
// reads, and on a production node those are the large ones -- managedFields,
// the image list and the label set are together about 85% of the object while
// a predicate such as `status.conditions.exists(...)` touches none of them.
//
// Pruning happens once on the way into the cache, so the saving applies for as
// long as the object is resident.
func pruningTransform(paths [][]string) toolscache.TransformFunc {
	keep := append(append([][]string{}, mandatoryPaths...), paths...)

	return func(obj any) (any, error) {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			// Tombstones and typed objects are passed through untouched.
			return obj, nil
		}

		pruned := map[string]any{}

		for _, p := range keep {
			if v, found, err := unstructured.NestedFieldNoCopy(u.Object, p...); err == nil && found {
				if err := unstructured.SetNestedField(pruned, v, p...); err != nil {
					// Keeping the whole object is always correct; failing to
					// prune must never change what a policy sees.
					return obj, nil
				}
			}
		}

		u.Object = pruned

		return u, nil
	}
}

// describePaths renders paths for logging, e.g. ["status.conditions"].
func describePaths(paths [][]string) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		s := ""
		for i, seg := range p {
			if i > 0 {
				s += "."
			}

			s += seg
		}

		out = append(out, s)
	}

	return out
}

var _ = fmt.Sprintf
