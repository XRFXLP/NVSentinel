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

package controller

import (
	"context"
	"testing"

	"github.com/nvidia/nvsentinel/preflight/pkg/gang"
	gangtypes "github.com/nvidia/nvsentinel/preflight/pkg/gang/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNamespaceReconciler_AddsLabeledNamespace(t *testing.T) {
	active := NewActiveNamespaces()
	ns := preflightNamespace("team-a")
	r, _ := newNSReconcilerWith(t, active, ns)

	reconcileNS(t, r, "team-a")

	assert.True(t, active.Contains("team-a"))
}

func TestNamespaceReconciler_IgnoresUnlabeledNamespace(t *testing.T) {
	active := NewActiveNamespaces()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "other-ns"}}
	r, _ := newNSReconcilerWith(t, active, ns)

	reconcileNS(t, r, "other-ns")

	assert.False(t, active.Contains("other-ns"))
}

func TestNamespaceReconciler_RemovesNamespaceWhenLabelDropped(t *testing.T) {
	active := NewActiveNamespaces()
	active.Add("team-a")

	// Namespace exists but label has been removed.
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}}
	r, _ := newNSReconcilerWith(t, active, ns)

	reconcileNS(t, r, "team-a")

	assert.False(t, active.Contains("team-a"))
}

func TestNamespaceReconciler_RemovesDeletedNamespace(t *testing.T) {
	active := NewActiveNamespaces()
	active.Add("team-a")

	// Namespace does not exist in the fake client (already deleted).
	r, _ := newNSReconcilerWith(t, active)

	reconcileNS(t, r, "team-a")

	assert.False(t, active.Contains("team-a"))
}

// TestNamespaceReconciler_PodTransformFollowsActiveSet verifies the end-to-end
// interaction: a namespace reconciled as preflight-enabled results in its pods
// receiving the full gang-field transform (not a stub), so that gang discovery
// via an active PreflightConfig continues to work.
func TestNamespaceReconciler_PodTransformFollowsActiveSet(t *testing.T) {
	active := NewActiveNamespaces()
	ns := preflightNamespace("team-a")

	// Set up both the namespace reconciler and a PreflightConfig reconciler,
	// as they would co-exist in production.
	nsReconciler, _ := newNSReconcilerWith(t, active, ns)
	pfcResolver := gang.NewResolver(&mockDiscoverer{}, nil)
	pfc := volcanoPFC("team-a", "default")
	pfcReconciler, _ := newReconcilerWith(t, pfcResolver, pfc)

	// Both reconcilers process their objects.
	reconcileNS(t, nsReconciler, "team-a")
	reconcile(t, pfcReconciler, "team-a", "default")

	// team-a is now active: its pods must get the full transform.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-0",
			Namespace: "team-a",
			Annotations: map[string]string{
				"scheduling.k8s.io/group-name": "training",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			Volumes: []corev1.Volume{{
				Name: gangtypes.GangConfigVolumeName,
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "gang-config"},
					},
				},
			}},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.1", Phase: corev1.PodRunning},
	}

	result, err := active.transform(pod)
	require.NoError(t, err)
	got := result.(*corev1.Pod)

	// Full transform: gang fields retained, heavy fields dropped.
	assert.Equal(t, "node-a", got.Spec.NodeName)
	assert.Equal(t, "10.0.0.1", got.Status.PodIP)
	assert.Equal(t, "training", got.Annotations["scheduling.k8s.io/group-name"])
	assert.Len(t, got.Spec.Volumes, 1)
	assert.Equal(t, gangtypes.GangConfigVolumeName, got.Spec.Volumes[0].Name)
	assert.Empty(t, got.Spec.Containers)

	// The volcano discoverer registered by the PreflightConfig is active.
	assert.Equal(t, "volcano", pfcResolver.For("team-a").Name())
}

func TestPreflightConfig_GangDiscoveryWorksAfterNamespaceActivation(t *testing.T) {
	active := NewActiveNamespaces()
	resolver := gang.NewResolver(&mockDiscoverer{}, nil)

	nsReconciler, _ := newNSReconcilerWith(t, active, preflightNamespace("preflight-ns"))
	pfcReconciler, _ := newReconcilerWith(t, resolver, volcanoPFC("preflight-ns", "default"))

	// Namespace labeled → active set populated.
	reconcileNS(t, nsReconciler, "preflight-ns")
	require.True(t, active.Contains("preflight-ns"))

	// PreflightConfig CR applied → volcano discoverer registered for the namespace.
	reconcile(t, pfcReconciler, "preflight-ns", "default")
	require.Equal(t, "volcano", resolver.For("preflight-ns").Name())

	// A pod arrives with a gang annotation and heavy spec fields, as a real GPU
	// training pod would look like before the cache transform runs.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-0",
			Namespace: "preflight-ns",
			Annotations: map[string]string{
				"scheduling.k8s.io/group-name": "training",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node-a",
			Containers: []corev1.Container{{Name: "cuda", Image: "nvcr.io/nvidia/cuda:latest"}},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.1", Phase: corev1.PodRunning},
	}

	// preflight-ns is active → full transform: gang annotation retained, heavy
	// fields (containers, etc.) dropped.
	cached, err := active.transform(pod)
	require.NoError(t, err)
	cachedPod := cached.(*corev1.Pod)

	assert.Equal(t, "training", cachedPod.Annotations["scheduling.k8s.io/group-name"],
		"full transform must retain gang annotation so discoverer can identify the gang")
	assert.Empty(t, cachedPod.Spec.Containers,
		"full transform must still drop heavy fields")

	// The volcano discoverer registered by the PreflightConfig CR can discover
	// the gang from the cached (pruned) pod — injection works despite pruning.
	d := resolver.For("preflight-ns")
	assert.True(t, d.CanHandle(cachedPod),
		"discoverer must be able to handle a cached pod from a preflight-enabled namespace")
	assert.NotEmpty(t, d.ExtractGangID(cachedPod))

	// Negative case: a pod from a non-preflight namespace is stubbed (annotation
	// dropped) → the discoverer cannot handle it.
	otherPod := pod.DeepCopy()
	otherPod.Namespace = "other-ns"

	stubbed, err := active.transform(otherPod)
	require.NoError(t, err)
	stubPod := stubbed.(*corev1.Pod)

	assert.Empty(t, stubPod.Annotations,
		"stub transform must drop annotations from non-preflight namespaces")
	assert.False(t, d.CanHandle(stubPod),
		"discoverer must NOT handle a stubbed pod from a non-preflight namespace")
}

func preflightNamespace(name string) *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{preflightNamespaceLabel: "enabled"},
		},
	}
}

func newNSReconcilerWith(t *testing.T, active *ActiveNamespaces, objs ...client.Object) (*NamespaceReconciler, client.Client) {
	t.Helper()

	scheme := pfcTestScheme(t)
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	return NewNamespaceReconciler(c, active), c
}

func reconcileNS(t *testing.T, r *NamespaceReconciler, name string) {
	t.Helper()

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Name: name},
	})
	require.NoError(t, err)
}
