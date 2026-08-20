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

package informers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/nvidia/nvsentinel/commons/pkg/kubeclient"
)

// TestInformersSendEvictionRequestForPod_RateLimitScenarios_HigherQPSIncreasesThroughput
// exercises Node Drainer's real pod eviction request against envtest.
func TestInformersSendEvictionRequestForPod_RateLimitScenarios_HigherQPSIncreasesThroughput(t *testing.T) {
	testEnvironment := &envtest.Environment{}
	testConfig, err := testEnvironment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testEnvironment.Stop()) })

	adminClient, err := kubernetes.NewForConfig(testConfig)
	require.NoError(t, err)

	namespace := fmt.Sprintf("rate-limit-%d", time.Now().UnixNano())
	_, err = adminClient.CoreV1().Namespaces().Create(t.Context(), &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	const (
		podCount = 10
		burst    = 1
	)

	tests := []struct {
		name   string
		prefix string
		qps    float64
	}{
		{name: "low QPS", prefix: "low-qps", qps: 4},
		{name: "high QPS", prefix: "high-qps", qps: 40},
	}

	durations := make(map[string]time.Duration, len(tests))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			durations[test.name] = measureEvictionThroughput(
				t, adminClient, testConfig, namespace, test.prefix, podCount, test.qps, burst,
			)
		})
	}

	lowRate := float64(podCount) / durations["low QPS"].Seconds()
	highRate := float64(podCount) / durations["high QPS"].Seconds()
	throughputRatio := highRate / lowRate
	t.Logf("eviction throughput: low QPS=%.2f pods/s, high QPS=%.2f pods/s, ratio=%.2fx",
		lowRate, highRate, throughputRatio)

	assert.GreaterOrEqual(t, throughputRatio, 8.0)
	assert.LessOrEqual(t, throughputRatio, 11.0)
}

// measureEvictionThroughput creates pods with an unrestricted setup client,
// then measures only eviction requests made by the rate-limited client.
func measureEvictionThroughput(t *testing.T, adminClient kubernetes.Interface, testConfig *rest.Config,
	namespace, prefix string, podCount int, qps float64, burst int) time.Duration {
	t.Helper()

	config := rest.CopyConfig(testConfig)
	require.NoError(t, (kubeclient.RateLimitConfig{QPS: qps, Burst: burst}).Apply(config))

	clientset, err := kubernetes.NewForConfig(config)
	require.NoError(t, err)
	nodeDrainer := &Informers{clientset: clientset}

	pods := make([]*v1.Pod, podCount)
	for idx := range podCount {
		pods[idx], err = adminClient.CoreV1().Pods(namespace).Create(t.Context(), &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", prefix, idx)},
			Spec: v1.PodSpec{
				NodeName:   fmt.Sprintf("node-%d", idx),
				Containers: []v1.Container{{Name: "workload", Image: "example.invalid/workload"}},
			},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	start := time.Now()
	for _, pod := range pods {
		require.NoError(t, nodeDrainer.sendEvictionRequestForPod(ctx, namespace, 0, pod))
	}

	return time.Since(start)
}
