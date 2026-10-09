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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/nodecache"
)

func countTestNode(name string, gpu bool) *v1.Node {
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"pool": "a"}}}
	if gpu {
		node.Labels[GPUNodeLabel] = GPUNodeLabelValue
	}

	return node
}

// startCountingInformer runs a NodeInformer over a fake clientset seeded with nodes.
func startCountingInformer(t *testing.T, nodes ...*v1.Node) (*NodeInformer, *fake.Clientset) {
	t.Helper()

	client := fake.NewClientset()
	for _, n := range nodes {
		require.NoError(t, client.Tracker().Add(n))
	}

	ni, err := NewNodeInformer(client, 0, GPUNodeLabel, GPUNodeLabelValue, nodecache.Keys{})
	require.NoError(t, err)

	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })
	require.NoError(t, ni.Run(stopCh))

	return ni, client
}

// recountGPUNodes is the listing GetNodeCounts used to do, kept as the reference.
func recountGPUNodes(t *testing.T, ni *NodeInformer) int {
	t.Helper()

	nodes, err := ni.lister.List(labels.Set{GPUNodeLabel: GPUNodeLabelValue}.AsSelector())
	require.NoError(t, err)

	return len(nodes)
}

func requireGPUTotal(t *testing.T, ni *NodeInformer, want int) {
	t.Helper()

	require.Eventually(t, func() bool {
		total, _, err := ni.GetNodeCounts()
		return err == nil && total == want
	}, 10*time.Second, 10*time.Millisecond, "GPU node total never reached %d", want)
}

func TestGetNodeCounts_InitialListIsCountedBeforeSync(t *testing.T) {
	ni, _ := startCountingInformer(t,
		countTestNode("gpu-1", true), countTestNode("gpu-2", true), countTestNode("cpu-1", false))

	// Run returned, so the count must already be complete: no waiting here.
	total, _, err := ni.GetNodeCounts()
	require.NoError(t, err)
	require.Equal(t, 2, total)
}

func TestGetNodeCounts_TracksAddUpdateDelete(t *testing.T) {
	ctx := t.Context()
	ni, client := startCountingInformer(t, countTestNode("gpu-1", true), countTestNode("cpu-1", false))
	nodes := client.CoreV1().Nodes()

	_, err := nodes.Create(ctx, countTestNode("gpu-2", true), metav1.CreateOptions{})
	require.NoError(t, err)
	requireGPUTotal(t, ni, 2)

	_, err = nodes.Create(ctx, countTestNode("cpu-2", false), metav1.CreateOptions{})
	require.NoError(t, err)
	requireGPUTotal(t, ni, 2)

	// cpu-1 gains the GPU label.
	_, err = nodes.Update(ctx, countTestNode("cpu-1", true), metav1.UpdateOptions{})
	require.NoError(t, err)
	requireGPUTotal(t, ni, 3)

	// gpu-1 loses it.
	_, err = nodes.Update(ctx, countTestNode("gpu-1", false), metav1.UpdateOptions{})
	require.NoError(t, err)
	requireGPUTotal(t, ni, 2)

	// An update that leaves the label alone changes nothing.
	unchanged := countTestNode("gpu-2", true)
	unchanged.Labels["pool"] = "b"
	_, err = nodes.Update(ctx, unchanged, metav1.UpdateOptions{})
	require.NoError(t, err)

	// A GPU node and a non-GPU node leave the cache.
	require.NoError(t, nodes.Delete(ctx, "gpu-2", metav1.DeleteOptions{}))
	require.NoError(t, nodes.Delete(ctx, "cpu-2", metav1.DeleteOptions{}))
	requireGPUTotal(t, ni, 1)

	require.Equal(t, recountGPUNodes(t, ni), 1, "counter and a full recount disagree")
}

func TestCountDeletedNode_Tombstone(t *testing.T) {
	ni := &NodeInformer{gpuNodeLabelKey: GPUNodeLabel, gpuNodeLabelValue: GPUNodeLabelValue}
	ni.gpuNodes.Store(2)

	ni.countDeletedNode(cache.DeletedFinalStateUnknown{Key: "gpu-1", Obj: countTestNode("gpu-1", true)})
	require.Equal(t, int64(1), ni.gpuNodes.Load(), "a tombstone for a GPU node must uncount it")

	ni.countDeletedNode(cache.DeletedFinalStateUnknown{Key: "cpu-1", Obj: countTestNode("cpu-1", false)})
	require.Equal(t, int64(1), ni.gpuNodes.Load(), "a tombstone for a non-GPU node must not change the count")
}

func TestGetNodeCounts_MatchesRecountAfterMixedChanges(t *testing.T) {
	ctx := t.Context()

	seed := make([]*v1.Node, 0, 40)
	for i := range 40 {
		seed = append(seed, countTestNode(fmt.Sprintf("n-%d", i), i%2 == 0))
	}

	ni, client := startCountingInformer(t, seed...)
	nodes := client.CoreV1().Nodes()

	for i := range 40 {
		name := fmt.Sprintf("n-%d", i)

		switch i % 4 {
		case 0: // GPU node loses the label
			_, err := nodes.Update(ctx, countTestNode(name, false), metav1.UpdateOptions{})
			require.NoError(t, err)
		case 1: // non-GPU node gains it
			_, err := nodes.Update(ctx, countTestNode(name, true), metav1.UpdateOptions{})
			require.NoError(t, err)
		case 2: // GPU node is deleted
			require.NoError(t, nodes.Delete(ctx, name, metav1.DeleteOptions{}))
		case 3: // non-GPU node is deleted
			require.NoError(t, nodes.Delete(ctx, name, metav1.DeleteOptions{}))
		}
	}

	// 10 nodes gained the label; every other GPU node either lost it or was deleted.
	requireGPUTotal(t, ni, 10)
	require.Equal(t, recountGPUNodes(t, ni), 10, "counter and a full recount disagree")
}

func TestIsGPUNode_EmptyValueRequiresTheLabel(t *testing.T) {
	ni := &NodeInformer{gpuNodeLabelKey: GPUNodeLabel, gpuNodeLabelValue: ""}

	unlabelled := countTestNode("cpu-1", false)
	require.False(t, ni.isGPUNode(unlabelled), "a node without the label must not match an empty value")

	labelled := countTestNode("gpu-1", false)
	labelled.Labels[GPUNodeLabel] = ""
	require.True(t, ni.isGPUNode(labelled), "a node carrying the label with an empty value must match")
}
