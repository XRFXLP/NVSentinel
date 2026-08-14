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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

// populateIndexer returns an indexer pre-loaded with numNodes nodes each having
// slicesPerNode ResourceSlices. Returns the indexer and a sample node.
func populateIndexer(numNodes, slicesPerNode int) (cache.Indexer, *corev1.Node) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		NodeResourceSliceIndex: func(obj interface{}) ([]string, error) {
			rs, ok := obj.(*resourcev1.ResourceSlice)
			if !ok || rs.Spec.NodeName == nil || *rs.Spec.NodeName == "" {
				return nil, nil
			}
			return []string{*rs.Spec.NodeName}, nil
		},
	})
	for n := 0; n < numNodes; n++ {
		nodeName := fmt.Sprintf("node-%05d", n)
		for s := 0; s < slicesPerNode; s++ {
			name := fmt.Sprintf("rs-%05d-%02d", n, s)
			_ = indexer.Add(&resourcev1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: resourcev1.ResourceSliceSpec{
					NodeName: &nodeName,
				},
			})
		}
	}
	sampleNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-00000"}}
	return indexer, sampleNode
}

// populatePlainStore returns a plain store (no index) for the scan path.
func populatePlainStore(numNodes, slicesPerNode int) (cache.Store, *corev1.Node) {
	store := cache.NewStore(cache.MetaNamespaceKeyFunc)
	for n := 0; n < numNodes; n++ {
		nodeName := fmt.Sprintf("node-%05d", n)
		for s := 0; s < slicesPerNode; s++ {
			name := fmt.Sprintf("rs-%05d-%02d", n, s)
			_ = store.Add(&resourcev1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: resourcev1.ResourceSliceSpec{
					NodeName: &nodeName,
				},
			})
		}
	}
	sampleNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-00000"}}
	return store, sampleNode
}

// BenchmarkResourceSlicesScan measures the O(S) full-scan path (pre-fix).
func BenchmarkResourceSlicesScan(b *testing.B) {
	for _, tc := range []struct {
		nodes, slicesPerNode int
	}{
		{1_000, 5},
		{5_000, 5},
		{25_000, 5},
		{100_000, 5},
	} {
		total := tc.nodes * tc.slicesPerNode
		b.Run(fmt.Sprintf("nodes=%d/total_slices=%d", tc.nodes, total), func(b *testing.B) {
			store, node := populatePlainStore(tc.nodes, tc.slicesPerNode)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = ResourceSlicesForNode(store, node)
			}
		})
	}
}

// BenchmarkResourceSlicesIndex measures the O(slices_per_node) indexed path (post-fix).
func BenchmarkResourceSlicesIndex(b *testing.B) {
	for _, tc := range []struct {
		nodes, slicesPerNode int
	}{
		{1_000, 5},
		{5_000, 5},
		{25_000, 5},
		{100_000, 5},
	} {
		total := tc.nodes * tc.slicesPerNode
		b.Run(fmt.Sprintf("nodes=%d/total_slices=%d", tc.nodes, total), func(b *testing.B) {
			indexer, node := populateIndexer(tc.nodes, tc.slicesPerNode)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = ResourceSlicesForNode(indexer, node)
			}
		})
	}
}
