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

package controller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	drainv1alpha1 "github.com/nvidia/nvsentinel/plugins/slinky-drainer/api/v1alpha1"
)

const (
	testSlinkyNamespace = "slinky-test"
	testTimeout         = 30 * time.Second
	testPollInterval    = 500 * time.Millisecond
)

type testEnvContext struct {
	client client.Client
	ctx    context.Context
}


func TestReconcile_FullDrainCycle(t *testing.T) {
	tc := setupTestEnv(t, "drain-full-cycle")

	node := createNode(t, tc, "test-node-drain-cycle", nil)
	// Create pod without drain condition so the reconciler pauses waiting for it,
	// giving us time to observe the annotation before it gets cleaned up.
	pod := createSlinkyPod(t, tc, node.Name)
	createDrainRequest(t, tc, "drain-full-cycle", drainv1alpha1.DrainRequestSpec{
		NodeName:         node.Name,
		ErrorCode:        []string{"79"},
		EntitiesImpacted: []drainv1alpha1.EntityImpacted{{Type: "GPU", Value: "0"}},
		Reason:           "GPU has fallen off the bus",
	})

	assertNodeAnnotation(t, tc, node.Name, "[J] [NVSentinel] 79 GPU:0 - GPU has fallen off the bus")

	// Now mark the pod as drain-ready so the reconciler can proceed.
	markPodDrainReady(t, tc, pod.Name, pod.Namespace)

	waitForDrainComplete(t, tc, "drain-full-cycle", "default")
	waitForPodDeletion(t, tc, pod.Name, pod.Namespace)
	waitForAnnotationRemoved(t, tc, node.Name)
}

func TestReconcile_CancelledDrainRemovesAnnotation(t *testing.T) {
	tc := setupTestEnv(t, "drain-cancelled")

	node := createNode(t, tc, "test-node-cancelled", nil)
	// Create pod without drain condition so the reconciler sets the annotation and waits.
	createSlinkyPod(t, tc, node.Name)
	createDrainRequest(t, tc, "drain-cancelled", drainv1alpha1.DrainRequestSpec{
		NodeName:  node.Name,
		ErrorCode: []string{"79"},
		Reason:    "GPU has fallen off the bus",
	})

	assertNodeAnnotation(t, tc, node.Name, "[J] [NVSentinel] 79 - GPU has fallen off the bus")

	// Simulate cancellation: delete the DrainRequest (as node-drainer would).
	deleteDrainRequest(t, tc, "drain-cancelled", "default")

	waitForDrainRequestGone(t, tc, "drain-cancelled", "default")
	waitForAnnotationRemoved(t, tc, node.Name)
}

func TestReconcile_PreExistingAnnotationPreserved(t *testing.T) {
	tc := setupTestEnv(t, "drain-preexisting")

	node := createNode(t, tc, "test-node-preexisting", map[string]string{
		annotationKey: "Manual drain by operator",
	})
	createDrainRequest(t, tc, "drain-preexisting", drainv1alpha1.DrainRequestSpec{
		NodeName:  node.Name,
		ErrorCode: []string{"79"},
		Reason:    "GPU has fallen off the bus",
	})

	waitForDrainComplete(t, tc, "drain-preexisting", "default")
	assertNodeAnnotation(t, tc, node.Name, "Manual drain by operator")
}

// setupTestEnv bootstraps an envtest API server, wires up the reconciler with a
// unique controller name, starts the manager, and returns a context for the test.
func setupTestEnv(t *testing.T, controllerName string) *testEnvContext {
	t.Helper()

	scheme := clientgoscheme.Scheme
	require.NoError(t, drainv1alpha1.AddToScheme(scheme))

	te := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := te.Start()
	require.NoError(t, err, "failed to start envtest")

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	require.NoError(t, err, "failed to create manager")

	reconciler := &DrainRequestReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		PodCheckInterval: 1 * time.Second,
		DrainTimeout:     5 * time.Minute,
		SlinkyNamespace:  testSlinkyNamespace,
	}

	require.NoError(t, mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&corev1.Pod{},
		"spec.nodeName",
		func(obj client.Object) []string {
			return []string{obj.(*corev1.Pod).Spec.NodeName}
		},
	), "failed to set up field indexer")

	require.NoError(t,
		ctrl.NewControllerManagedBy(mgr).
			Named(controllerName).
			For(&drainv1alpha1.DrainRequest{}).
			Complete(reconciler),
		"failed to setup controller",
	)

	ctx, cancel := context.WithCancel(context.Background())
	mgrDone := make(chan struct{})

	go func() {
		defer close(mgrDone)

		if err := mgr.Start(ctx); err != nil {
			t.Logf("manager exited: %v", err)
		}
	}()

	k := mgr.GetClient()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testSlinkyNamespace}}
	_ = k.Create(ctx, ns)

	t.Cleanup(func() {
		cancel()
		<-mgrDone

		if err := te.Stop(); err != nil {
			t.Logf("failed to stop envtest: %v", err)
		}
	})

	return &testEnvContext{client: k, ctx: ctx}
}

func createNode(t *testing.T, tc *testEnvContext, name string, annotations map[string]string) *corev1.Node {
	t.Helper()

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: annotations,
		},
	}
	require.NoError(t, tc.client.Create(tc.ctx, node))

	return node
}

func createSlinkyPod(t *testing.T, tc *testEnvContext, nodeName string) *corev1.Pod {
	t.Helper()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "slinky-pod-" + nodeName,
			Namespace: testSlinkyNamespace,
		},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: "slurmd", Image: "nvcr.io/nvidia/slinky:latest"}},
		},
	}
	require.NoError(t, tc.client.Create(tc.ctx, pod))

	return pod
}

func markPodDrainReady(t *testing.T, tc *testEnvContext, podName, podNamespace string) {
	t.Helper()

	pod := &corev1.Pod{}
	require.NoError(t, tc.client.Get(tc.ctx, types.NamespacedName{Name: podName, Namespace: podNamespace}, pod))

	pod.Status.Conditions = []corev1.PodCondition{
		{Type: slurmNodeStateDrainConditionType, Status: corev1.ConditionTrue},
	}
	require.NoError(t, tc.client.Status().Update(tc.ctx, pod))
}

func createDrainRequest(t *testing.T, tc *testEnvContext, name string, spec drainv1alpha1.DrainRequestSpec) {
	t.Helper()

	dr := &drainv1alpha1.DrainRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       spec,
	}
	require.NoError(t, tc.client.Create(tc.ctx, dr))
}

func deleteDrainRequest(t *testing.T, tc *testEnvContext, name, namespace string) {
	t.Helper()

	dr := &drainv1alpha1.DrainRequest{}
	require.NoError(t, tc.client.Get(tc.ctx, types.NamespacedName{Name: name, Namespace: namespace}, dr))
	require.NoError(t, tc.client.Delete(tc.ctx, dr))
}

func waitForDrainRequestGone(t *testing.T, tc *testEnvContext, drName, drNamespace string) {
	t.Helper()

	require.Eventually(t, func() bool {
		dr := &drainv1alpha1.DrainRequest{}
		err := tc.client.Get(tc.ctx, types.NamespacedName{Name: drName, Namespace: drNamespace}, dr)

		return apierrors.IsNotFound(err)
	}, testTimeout, testPollInterval, "DrainRequest %s/%s should be deleted", drNamespace, drName)
}

func waitForDrainComplete(t *testing.T, tc *testEnvContext, drName, drNamespace string) {
	t.Helper()

	require.Eventually(t, func() bool {
		dr := &drainv1alpha1.DrainRequest{}
		if err := tc.client.Get(tc.ctx, types.NamespacedName{Name: drName, Namespace: drNamespace}, dr); err != nil {
			return false
		}

		for _, c := range dr.Status.Conditions {
			if c.Type == drainCompleteConditionType && c.Status == metav1.ConditionTrue {
				return true
			}
		}

		return false
	}, testTimeout, testPollInterval, "DrainRequest %s/%s should have DrainComplete=True", drNamespace, drName)
}

func waitForPodDeletion(t *testing.T, tc *testEnvContext, podName, podNamespace string) {
	t.Helper()

	require.Eventually(t, func() bool {
		p := &corev1.Pod{}
		if err := tc.client.Get(tc.ctx, types.NamespacedName{Name: podName, Namespace: podNamespace}, p); err != nil {
			return true
		}

		return p.DeletionTimestamp != nil
	}, testTimeout, testPollInterval, "Pod %s/%s should be marked for deletion", podNamespace, podName)
}

func waitForAnnotationRemoved(t *testing.T, tc *testEnvContext, nodeName string) {
	t.Helper()

	require.Eventually(t, func() bool {
		node := &corev1.Node{}
		if err := tc.client.Get(tc.ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
			return false
		}

		_, exists := node.Annotations[annotationKey]

		return !exists
	}, testTimeout, testPollInterval, "Annotation on node %s should be removed", nodeName)
}

func assertNodeAnnotation(t *testing.T, tc *testEnvContext, nodeName, expectedValue string) {
	t.Helper()

	require.Eventually(t, func() bool {
		node := &corev1.Node{}
		if err := tc.client.Get(tc.ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
			return false
		}

		val, exists := node.Annotations[annotationKey]

		return exists && val == expectedValue
	}, testTimeout, testPollInterval, "Node %s should have annotation %q=%q", nodeName, annotationKey, expectedValue)

	node := &corev1.Node{}
	require.NoError(t, tc.client.Get(tc.ctx, types.NamespacedName{Name: nodeName}, node))
	assert.Equal(t, expectedValue, node.Annotations[annotationKey])
}

