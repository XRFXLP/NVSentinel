// Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	janitorv1alpha1 "github.com/nvidia/nvsentinel/janitor/api/v1alpha1"
)

func TestMaintenanceCRD_UpdateImmutableFields_RejectsMutation(t *testing.T) {
	t.Parallel()

	testScheme := runtime.NewScheme()
	require.NoError(t, janitorv1alpha1.AddToScheme(testScheme))

	testEnvironment := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "..", "distros", "kubernetes", "nvsentinel", "charts", "janitor", "crds"),
		},
		ErrorIfCRDPathMissing: true,
	}
	if binaryDir := getFirstFoundEnvTestBinaryDir(); binaryDir != "" {
		testEnvironment.BinaryAssetsDirectory = binaryDir
	}

	restConfig, err := testEnvironment.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testEnvironment.Stop())
	})

	kubeClient, err := client.New(restConfig, client.Options{Scheme: testScheme})
	require.NoError(t, err)

	tests := []struct {
		name        string
		resource    client.Object
		mutate      func(client.Object)
		errorString string
	}{
		{
			name: "RebootNodeNodeName",
			resource: &janitorv1alpha1.RebootNode{
				ObjectMeta: metav1.ObjectMeta{Name: "immutable-reboot"},
				Spec:       janitorv1alpha1.RebootNodeSpec{NodeName: "node-a"},
			},
			mutate: func(resource client.Object) {
				resource.(*janitorv1alpha1.RebootNode).Spec.NodeName = "node-b"
			},
			errorString: "nodeName cannot be changed after creation",
		},
		{
			name: "TerminateNodeNodeName",
			resource: &janitorv1alpha1.TerminateNode{
				ObjectMeta: metav1.ObjectMeta{Name: "immutable-terminate"},
				Spec:       janitorv1alpha1.TerminateNodeSpec{NodeName: "node-a"},
			},
			mutate: func(resource client.Object) {
				resource.(*janitorv1alpha1.TerminateNode).Spec.NodeName = "node-b"
			},
			errorString: "nodeName cannot be changed after creation",
		},
		{
			name: "GPUResetSelector",
			resource: &janitorv1alpha1.GPUReset{
				ObjectMeta: metav1.ObjectMeta{Name: "immutable-gpu-reset"},
				Spec: janitorv1alpha1.GPUResetSpec{
					NodeName: "node-a",
					Selector: &janitorv1alpha1.GPUSelector{
						UUIDs: []string{"GPU-11111111-1111-1111-1111-111111111111"},
					},
				},
			},
			mutate: func(resource client.Object) {
				resource.(*janitorv1alpha1.GPUReset).Spec.Selector.UUIDs =
					[]string{"GPU-22222222-2222-2222-2222-222222222222"}
			},
			errorString: "selector cannot be changed after creation",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, kubeClient.Create(context.Background(), test.resource))

			test.mutate(test.resource)
			err := kubeClient.Update(context.Background(), test.resource)
			require.Error(t, err)
			assert.ErrorContains(t, err, test.errorString)
		})
	}
}
