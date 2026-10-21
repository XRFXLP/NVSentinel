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

// The tests in this file need envtest binaries. See the module Makefile, or:
//
//	go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
//	source <(setup-envtest use -p env)

package initializer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/annotations"
	celenv "github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/cel"
	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/config"
	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/policy"
)

const (
	restartTestNode      = "restart-node"
	restartTestNamespace = "gpu-operator"
	restartTestPod       = "operator-pod"
	restartTestWait      = 10 * time.Second
	restartTestTick      = 100 * time.Millisecond
)

// TestRegisterControllers_Restart_RecoversMatchesFromEarlierRun runs the monitor through
// registerControllers and a real manager, stops it, changes the pod, and starts it again.
// A match from the first run must recover when the pod recovers or is deleted while the
// monitor is stopped, and must stay when the pod is still unhealthy.
func TestRegisterControllers_Restart_RecoversMatchesFromEarlierRun(t *testing.T) {
	tests := []struct {
		name          string
		whileStopped  func(t *testing.T, c client.Client)
		expectHealthy bool
	}{
		{
			name: "pod deleted while stopped",
			whileStopped: func(t *testing.T, c client.Client) {
				t.Helper()

				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: restartTestNamespace, Name: restartTestPod}}
				require.NoError(t, c.Delete(t.Context(), pod, client.GracePeriodSeconds(0)))
				require.Eventually(t, func() bool {
					return apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{}))
				}, restartTestWait, restartTestTick)
			},
			expectHealthy: true,
		},
		{
			name: "pod recovered while stopped",
			whileStopped: func(t *testing.T, c client.Client) {
				t.Helper()

				setRestartTestPodPhase(t, c, corev1.PodRunning)
			},
			expectHealthy: true,
		},
		{
			name:          "pod still unhealthy while stopped",
			whileStopped:  func(*testing.T, client.Client) {},
			expectHealthy: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restConfig := startEnvtest(t)

			c, err := client.New(restConfig, client.Options{})
			require.NoError(t, err)

			createRestartTestObjects(t, c)

			policies := []config.Policy{restartTestPodPolicy()}

			first := startRegisteredMonitor(t, restConfig, policies)
			require.Eventually(t, func() bool {
				return first.publisher.count(false) == 1
			}, restartTestWait, restartTestTick, "the first run did not publish the unhealthy event")
			require.Eventually(t, func() bool {
				return policyMatchAnnotation(t, c) != ""
			}, restartTestWait, restartTestTick, "the first run did not save the match on the node")
			first.stop()

			tt.whileStopped(t, c)

			second := startRegisteredMonitor(t, restConfig, policies)

			if tt.expectHealthy {
				require.Eventually(t, func() bool {
					return second.publisher.count(true) == 1
				}, restartTestWait, restartTestTick, "the restarted monitor did not publish the healthy event")
				require.Eventually(t, func() bool {
					return policyMatchAnnotation(t, c) == ""
				}, restartTestWait, restartTestTick, "the restarted monitor did not remove the match from the node")

				return
			}

			require.Never(t, func() bool {
				return second.publisher.count(true) > 0
			}, 3*time.Second, restartTestTick, "the restarted monitor published a healthy event for an unhealthy pod")
			require.NotEmpty(t, policyMatchAnnotation(t, c), "the restarted monitor removed the match of an unhealthy pod")
		})
	}
}

type registeredMonitor struct {
	publisher *lockedPublisher
	stop      func()
}

// startRegisteredMonitor builds the manager the way InitializeAll does, registers the
// controllers before the manager starts, and starts it.
func startRegisteredMonitor(t *testing.T, restConfig *rest.Config, policies []config.Policy) registeredMonitor {
	t.Helper()

	plan, err := buildCachePlan(restConfig, policies, time.Hour)
	require.NoError(t, err)

	opts := buildManagerOptions(Params{MetricsBindAddress: "0", HealthProbeBindAddress: "0"}, plan.options)

	// Each run creates the same controller names in one process.
	skipNameValidation := true
	opts.Controller.SkipNameValidation = &skipNameValidation

	mgr, err := ctrl.NewManager(restConfig, opts)
	require.NoError(t, err)

	celEnv, err := celenv.NewEnvironment(mgr.GetAPIReader())
	require.NoError(t, err)

	evaluator, err := policy.NewEvaluator(celEnv, policies)
	require.NoError(t, err)

	pub := &lockedPublisher{}

	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, registerControllers(ctx, mgr, evaluator, pub, policies, 1))

	done := make(chan error, 1)

	go func() { done <- mgr.Start(ctx) }()

	stopped := false
	stop := func() {
		if stopped {
			return
		}

		stopped = true

		cancel()
		require.NoError(t, <-done)
	}
	t.Cleanup(stop)

	require.True(t, mgr.GetCache().WaitForCacheSync(ctx), "cache did not sync")

	return registeredMonitor{publisher: pub, stop: stop}
}

func restartTestPodPolicy() config.Policy {
	return config.Policy{
		Name:    "operator-pod-health",
		Enabled: true,
		Resource: config.ResourceSpec{
			Version:   "v1",
			Kind:      "Pod",
			Namespace: restartTestNamespace,
		},
		Predicate: config.PredicateSpec{
			Expression: `resource.status.phase != 'Running' && resource.status.phase != 'Succeeded'`,
		},
		NodeAssociation: &config.AssociationSpec{
			Expression: `resource.spec.nodeName`,
		},
		HealthEvent: config.HealthEventSpec{
			ComponentClass:    "Software",
			Message:           "Operator pod is not healthy",
			RecommendedAction: "CONTACT_SUPPORT",
			ErrorCode:         []string{"OPERATOR_POD_UNHEALTHY"},
		},
	}
}

func createRestartTestObjects(t *testing.T, c client.Client) {
	t.Helper()

	require.NoError(t, c.Create(t.Context(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: restartTestNode}}))
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: restartTestNamespace}}))
	require.NoError(t, c.Create(t.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: restartTestNamespace, Name: restartTestPod},
		Spec: corev1.PodSpec{
			NodeName:   restartTestNode,
			Containers: []corev1.Container{{Name: "operator", Image: "busybox"}},
		},
	}))

	setRestartTestPodPhase(t, c, corev1.PodPending)
}

func setRestartTestPodPhase(t *testing.T, c client.Client, phase corev1.PodPhase) {
	t.Helper()

	pod := &corev1.Pod{}
	require.NoError(t, c.Get(t.Context(), types.NamespacedName{Namespace: restartTestNamespace, Name: restartTestPod}, pod))

	pod.Status.Phase = phase
	require.NoError(t, c.Status().Update(t.Context(), pod))
}

func policyMatchAnnotation(t *testing.T, c client.Client) string {
	t.Helper()

	node := &corev1.Node{}
	require.NoError(t, c.Get(t.Context(), types.NamespacedName{Name: restartTestNode}, node))

	return node.Annotations[annotations.AnnotationKey]
}

// lockedPublisher records events from concurrent reconciles.
type lockedPublisher struct {
	mu     sync.Mutex
	events []recordedEvent
}

func (p *lockedPublisher) PublishHealthEvent(
	_ context.Context,
	_ *config.Policy,
	nodeName string,
	isHealthy bool,
	resourceInfo *config.ResourceInfo,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.events = append(p.events, recordedEvent{
		nodeName:     nodeName,
		isHealthy:    isHealthy,
		resourceInfo: resourceInfo,
	})

	return nil
}

func (p *lockedPublisher) count(isHealthy bool) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	count := 0

	for _, e := range p.events {
		if e.isHealthy == isHealthy {
			count++
		}
	}

	return count
}
