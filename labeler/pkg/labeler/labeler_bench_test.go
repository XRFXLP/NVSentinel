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

package labeler

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/nvidia/nvsentinel/labeler/pkg/devicecounts"
)

// BenchmarkReconcileAllNodes measures the throughput of a full reconcile pass
// (all N nodes, K ResourceSlices each, all in one partition) for the O(N×K)
// implementation introduced in xrfxlp/1599.
//
// Run both this file and its counterpart in NVSentinel-labeler-patch to compare:
//
//	go test -run=^$ -bench=BenchmarkReconcileAllNodes -benchtime=3x ./labeler/pkg/labeler/
func BenchmarkReconcileAllNodes(b *testing.B) {
	const slicesPerNode = 5

	for _, nodeCount := range []int{100, 500, 1000, 5000, 9000} {
		b.Run(fmt.Sprintf("N=%d/K=%d", nodeCount, slicesPerNode), func(b *testing.B) {
			l := newBenchLabeler(b, nodeCount, slicesPerNode)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l.reconcileAllNodes()
			}
		})
	}
}

// newBenchLabeler constructs a Labeler with a fake clientset and fully
// in-memory informer stores. PATCH calls go to the fake clientset (microseconds).
// Use BenchmarkReconcileAllNodesEnvtest for real API server latency.
func newBenchLabeler(b *testing.B, nodeCount, slicesPerNode int) *Labeler {
	b.Helper()
	nodes := makeBenchNodes(b, nodeCount)
	nodeInformer, rsInformer := buildBenchInformers(b, nodes, slicesPerNode)

	clientObjs := make([]k8sruntime.Object, len(nodes))
	for i, n := range nodes {
		clientObjs[i] = n.DeepCopy()
	}
	return buildBenchLabeler(fake.NewSimpleClientset(clientObjs...), nodeInformer, rsInformer, buildBenchDeviceCountManager(b))
}

func makeBenchNodes(b *testing.B, count int) []*corev1.Node {
	b.Helper()
	nodes := make([]*corev1.Node, count)
	for i := range nodes {
		nodes[i] = &corev1.Node{
			Name:   fmt.Sprintf("bench-node-%06d", i),
			Labels: map[string]string{},
		}
	}
	return nodes
}

// BenchmarkReconcileAllNodesEnvtest is identical to BenchmarkReconcileAllNodes
// but replaces the fake clientset with a real envtest API server, so PATCH calls
// pay real serialization and etcd write costs. This bounds the expected real-world
// startup reconcile time more tightly than the in-memory benchmark.
//
// A fresh Labeler is constructed each iteration so nodePatcher.pendingVersions
// is always empty, keeping every iteration's work consistent (N real PATCHes,
// not N GETs on subsequent passes).
//
// Run with:
//
//	eval $(setup-envtest use --use-env -p env)
//	go test -run=^$ -bench=BenchmarkReconcileAllNodesEnvtest -benchtime=3x ./labeler/pkg/labeler/
func BenchmarkReconcileAllNodesEnvtest(b *testing.B) {
	const slicesPerNode = 5

	for _, nodeCount := range []int{100, 200, 500, 1000, 9000} {
		b.Run(fmt.Sprintf("N=%d/K=%d", nodeCount, slicesPerNode), func(b *testing.B) {
			nodes := makeBenchNodes(b, nodeCount)

			// Start a real API server once per sub-benchmark, outside the timer.
			clientset := startEnvtestWithNodes(b, nodes)

			// Informer caches are populated directly — no informer sync needed.
			nodeInformer, rsInformer := buildBenchInformers(b, nodes, slicesPerNode)
			manager := buildBenchDeviceCountManager(b)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Fresh Labeler resets nodePatcher so every iteration does N real PATCHes.
				l := buildBenchLabeler(clientset, nodeInformer, rsInformer, manager)
				l.reconcileAllNodes()
			}
		})
	}
}

// startEnvtestWithNodes starts a real kube-apiserver and creates all nodes in
// it concurrently. The environment is stopped via b.Cleanup.
func startEnvtestWithNodes(b *testing.B, nodes []*corev1.Node) kubernetes.Interface {
	b.Helper()

	env := &envtest.Environment{}
	restConfig, err := env.Start()
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, env.Stop()) })

	clientset, err := kubernetes.NewForConfig(restConfig)
	require.NoError(b, err)

	// Create nodes in parallel — sequential creation at N=1000 would itself take seconds.
	const workers = 32
	ch := make(chan *corev1.Node, len(nodes))
	for _, n := range nodes {
		ch <- n
	}
	close(ch)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for range workers {
		wg.Go(func() {
			for n := range ch {
				_, err := clientset.CoreV1().Nodes().Create(context.Background(), n.DeepCopy(), metav1.CreateOptions{})
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	require.NoError(b, firstErr)

	return clientset
}

// buildBenchInformers creates the node and ResourceSlice informers populated
// directly via the indexer (no API server sync). Shared across b.N iterations.
func buildBenchInformers(b *testing.B, nodes []*corev1.Node, slicesPerNode int) (cache.SharedIndexInformer, cache.SharedIndexInformer) {
	b.Helper()

	nodeInformer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &corev1.Node{}, 0, cache.Indexers{})
	for _, n := range nodes {
		require.NoError(b, nodeInformer.GetIndexer().Add(n))
	}

	rsInformer := cache.NewSharedIndexInformer(
		&cache.ListWatch{}, &resourcev1.ResourceSlice{}, 0,
		cache.Indexers{devicecounts.ResourceSliceNodeNameIndex: devicecounts.ResourceSliceNodeNameIndexFunc},
	)
	for _, n := range nodes {
		for k := range slicesPerNode {
			nodeName := n.Name
			rs := &resourcev1.ResourceSlice{
				Name: fmt.Sprintf("rs-%s-%d", nodeName, k),
				Spec: resourcev1.ResourceSliceSpec{
					NodeName: &nodeName,
					Driver:   "gpu.nvidia.com",
					Pool: resourcev1.ResourcePool{
						Name:               "pool-" + nodeName,
						ResourceSliceCount: int64(slicesPerNode),
					},
				},
			}
			require.NoError(b, rsInformer.GetIndexer().Add(rs))
		}
	}
	return nodeInformer, rsInformer
}

func buildBenchDeviceCountManager(b *testing.B) *devicecounts.Manager {
	b.Helper()
	manager, err := devicecounts.NewManager(devicecounts.Config{
		Enabled: true,
		Classes: []devicecounts.ClassConfig{{
			Name:              "gpu",
			Enabled:           true,
			Labels:            devicecounts.Labels{Current: "bench.nvsentinel/gpu-current", Expected: "bench.nvsentinel/gpu-expected"},
			CurrentExpression: "resourceSlices.size()",
		}},
	})
	require.NoError(b, err)
	return manager
}

func buildBenchLabeler(clientset kubernetes.Interface, nodeInformer, rsInformer cache.SharedIndexInformer, manager *devicecounts.Manager) *Labeler {
	newPodInformer := func(indices cache.Indexers) cache.SharedIndexInformer {
		return cache.NewSharedIndexInformer(&cache.ListWatch{}, &corev1.Pod{}, 0, indices)
	}
	return &Labeler{
		ctx:                   context.Background(),
		clientset:             clientset,
		nodeInformer:          nodeInformer,
		nodeLister:            listersv1.NewNodeLister(nodeInformer.GetIndexer()),
		resourceSliceInformer: rsInformer,
		podInformer: newPodInformer(cache.Indexers{
			NodeDCGMIndex:   podNodeIndexerByLabel("app", "nvidia-dcgm"),
			NodeDriverIndex: podNodeIndexerByLabel("app", "nvidia-driver-daemonset"),
		}),
		crdDriverInformer:    newPodInformer(cache.Indexers{NodeDriverIndex: podNodeIndexerByLabel(driverComponentLabel, driverComponentValue)}),
		gkeInstallerInformer: newPodInformer(cache.Indexers{NodeGKEDriverInstallerIndex: podNodeIndexerByLabel("k8s-app", "nvidia-driver-installer")}),
		informersSynced:      []cache.InformerSynced{func() bool { return true }},
		kataLabels:           buildKataLabels(""),
		deviceCounts:         manager,
	}
}
