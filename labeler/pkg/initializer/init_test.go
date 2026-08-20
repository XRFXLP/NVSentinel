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

package initializer

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/nvidia/nvsentinel/commons/pkg/kubeclient"
)

func TestInitializeKubernetesClient_ConfiguredRateLimits_AppliesToClientset(t *testing.T) {
	kubeconfigPath := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, clientcmd.WriteToFile(clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"test": {Server: "https://example.invalid"},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"test": {},
		},
		Contexts: map[string]*clientcmdapi.Context{
			"test": {Cluster: "test", AuthInfo: "test"},
		},
		CurrentContext: "test",
	}, kubeconfigPath))

	tests := []struct {
		name  string
		qps   float64
		burst int
	}{
		{name: "low QPS", qps: 4, burst: 1},
		{name: "high QPS", qps: 40, burst: 10},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clientset, err := initializeKubernetesClient(InitializationParams{
				KubeconfigPath: kubeconfigPath,
				KubernetesClientRateLimits: kubeclient.RateLimitConfig{
					QPS:   test.qps,
					Burst: test.burst,
				},
			})
			require.NoError(t, err)

			concreteClientset, ok := clientset.(*kubernetes.Clientset)
			require.True(t, ok)
			limiter := concreteClientset.CoreV1().RESTClient().GetRateLimiter()
			require.NotNil(t, limiter)
			assert.InDelta(t, test.qps, limiter.QPS(), 0.0001)
		})
	}
}
