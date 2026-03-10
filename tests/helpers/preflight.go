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

package helpers

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
)

const (
	PreflightNamespaceLabel    = "nvsentinel.nvidia.com/preflight"
	PreflightNamespaceLabelVal = "enabled"
	PreflightDCGMDiagName      = "preflight-dcgm-diag"
	PreflightConfigMapName     = "nvsentinel-preflight-config"
	PreflightConfigKey         = "config.yaml"

	GangConfigMapLabelManagedBy = "nvsentinel.nvidia.com/managed-by"
	GangConfigMapManagedByVal   = "preflight"
	GangDataKeyExpectedCount    = "expected_count"
	GangDataKeyPeers            = "peers"
	GangDataKeyMasterAddr       = "master_addr"
	GangDataKeyMasterPort       = "master_port"
	GangDataKeyGangID           = "gang_id"

	VolcanoPodGroupAnnotation = "scheduling.k8s.io/group-name"
)

// PreflightTestContext holds state for preflight E2E tests.
type PreflightTestContext struct {
	TestNamespace string
	NodeNames     []string
	PodNames      []string
	PodGroupName  string
}

// SetupPreflightTest sets up the full preflight E2E scenario:
//   - Checks preflight is deployed and waits for rollout
//   - Gets N real worker nodes (skips if insufficient)
//   - Creates and labels a test namespace for preflight
//   - Verifies the preflight config ConfigMap exists
//   - Creates a Volcano PodGroup and GPU pods (one per node)
func SetupPreflightTest(
	ctx context.Context, t *testing.T, c *envconf.Config,
	testNamespace, podGroupName string, nodeCount int,
) (context.Context, *PreflightTestContext) {
	t.Helper()

	client, err := c.NewClient()
	require.NoError(t, err, "create kubernetes client")

	var deployList appsv1.DeploymentList

	err = client.Resources(NVSentinelNamespace).List(ctx, &deployList,
		resources.WithLabelSelector("app.kubernetes.io/name=preflight"))
	require.NoError(t, err, "list preflight deployments")

	if len(deployList.Items) == 0 {
		t.Skipf("Preflight not deployed in %s", NVSentinelNamespace)
	}

	preflightDeployName := deployList.Items[0].Name
	WaitForDeploymentRollout(ctx, t, client, preflightDeployName, NVSentinelNamespace)

	nodeNames, err := GetRealNodeNames(ctx, client, nodeCount)
	if err != nil {
		t.Skipf("Need %d real worker nodes: %v", nodeCount, err)
	}

	t.Logf("Using worker nodes: %v", nodeNames)

	err = CreateNamespace(ctx, client, testNamespace)
	require.NoError(t, err, "create test namespace")

	var ns v1.Namespace

	err = client.Resources().Get(ctx, testNamespace, "", &ns)
	require.NoError(t, err)

	if ns.Labels == nil {
		ns.Labels = make(map[string]string)
	}

	ns.Labels[PreflightNamespaceLabel] = PreflightNamespaceLabelVal

	err = client.Resources().Update(ctx, &ns)
	require.NoError(t, err, "label namespace for preflight")

	var cm v1.ConfigMap

	err = client.Resources(NVSentinelNamespace).Get(
		ctx, PreflightConfigMapName, NVSentinelNamespace, &cm,
	)
	require.NoError(t, err,
		"preflight config ConfigMap %s should exist", PreflightConfigMapName)
	require.Contains(t, cm.Data, PreflightConfigKey)

	CreateVolcanoPodGroup(ctx, t, client, testNamespace, podGroupName, nodeCount)
	t.Logf("Created PodGroup %s/%s with minMember=%d",
		testNamespace, podGroupName, nodeCount)

	var podNames []string

	for i, node := range nodeNames {
		name := CreateGPUPodInGang(ctx, t, client, testNamespace, node, podGroupName)
		podNames = append(podNames, name)
		t.Logf("Created gang pod %d: %s on node %s", i, name, node)
	}

	testCtx := &PreflightTestContext{
		TestNamespace: testNamespace,
		NodeNames:     nodeNames,
		PodNames:      podNames,
		PodGroupName:  podGroupName,
	}

	return ctx, testCtx
}

// TeardownPreflightTest cleans up pods, PodGroup, and namespace.
func TeardownPreflightTest(
	ctx context.Context, t *testing.T, c *envconf.Config,
	testCtx *PreflightTestContext,
) context.Context {
	t.Helper()

	if testCtx == nil {
		return ctx
	}

	client, err := c.NewClient()
	if err != nil {
		return ctx
	}

	for _, podName := range testCtx.PodNames {
		_ = DeletePod(ctx, t, client, testCtx.TestNamespace, podName, false)
	}

	if testCtx.PodGroupName != "" {
		DeleteVolcanoPodGroup(ctx, client, testCtx.TestNamespace, testCtx.PodGroupName)
	}

	if testCtx.TestNamespace != "" {
		_ = client.Resources().Delete(ctx, &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: testCtx.TestNamespace},
		})
	}

	return ctx
}

// WaitForPodInitContainerStatuses waits until at least one preflight init
// container has terminated (Completed or Error).
func WaitForPodInitContainerStatuses(
	ctx context.Context, t *testing.T, client klient.Client,
	namespace, podName string,
) *v1.Pod {
	t.Helper()

	var pod v1.Pod

	require.Eventually(t, func() bool {
		err := client.Resources().Get(ctx, podName, namespace, &pod)
		if err != nil {
			return false
		}

		return PreflightInitContainerTerminated(&pod)
	}, EventuallyWaitTimeout, WaitInterval,
		"pod %s: at least one preflight-* init container should terminate",
		podName)

	return &pod
}

// PreflightInitContainerTerminated returns true if at least one preflight
// init container has reached Terminated state.
func PreflightInitContainerTerminated(pod *v1.Pod) bool {
	for _, st := range pod.Status.InitContainerStatuses {
		if strings.HasPrefix(st.Name, "preflight-") && st.State.Terminated != nil {
			return true
		}
	}

	return false
}

// ListGangConfigMaps lists ConfigMaps with the preflight gang label
// in the given namespaces.
func ListGangConfigMaps(
	ctx context.Context, client klient.Client, namespaces []string,
) ([]v1.ConfigMap, error) {
	selector := GangConfigMapLabelManagedBy + "=" + GangConfigMapManagedByVal

	var all []v1.ConfigMap

	for _, ns := range namespaces {
		var list v1.ConfigMapList

		err := client.Resources(ns).List(
			ctx, &list, resources.WithLabelSelector(selector),
		)
		if err != nil {
			continue
		}

		all = append(all, list.Items...)
	}

	return all, nil
}

// AssertGangConfigMap waits for the gang ConfigMap to be fully populated
// (gang_id, expected_count, peers, master_addr, master_port) and asserts
// correct values. Polls until all fields are present and match expectations
// to avoid flaky one-shot assertions on a partially-populated ConfigMap.
func AssertGangConfigMap(
	ctx context.Context, t *testing.T, client klient.Client,
	testCtx *PreflightTestContext, expectedGangID string,
	expectedPeerCount int,
) {
	t.Helper()

	namespaces := []string{testCtx.TestNamespace, NVSentinelNamespace}
	expectedCountStr := fmt.Sprintf("%d", expectedPeerCount)

	var found *v1.ConfigMap

	require.Eventually(t, func() bool {
		all, err := ListGangConfigMaps(ctx, client, namespaces)
		if err != nil {
			return false
		}

		for i := range all {
			cm := &all[i]
			if cm.Data[GangDataKeyGangID] != expectedGangID {
				continue
			}

			if cm.Data[GangDataKeyExpectedCount] != expectedCountStr {
				continue
			}

			peers := strings.TrimSpace(cm.Data[GangDataKeyPeers])
			if peers == "" || len(strings.Split(peers, "\n")) != expectedPeerCount {
				continue
			}

			if cm.Data[GangDataKeyMasterAddr] == "" {
				continue
			}

			if cm.Data[GangDataKeyMasterPort] == "" {
				continue
			}

			found = cm

			return true
		}

		return false
	}, EventuallyWaitTimeout, WaitInterval,
		"gang ConfigMap with gang_id=%s should be fully populated "+
			"(expected_count=%s, %d peers, master_addr, master_port)",
		expectedGangID, expectedCountStr, expectedPeerCount)

	t.Logf("Gang ConfigMap %s/%s: gang_id=%s expected_count=%s peers=\n%s",
		found.Namespace, found.Name,
		found.Data[GangDataKeyGangID],
		found.Data[GangDataKeyExpectedCount],
		found.Data[GangDataKeyPeers])
}

// CreateVolcanoPodGroup creates a Volcano PodGroup with the given
// minMember in the namespace.
func CreateVolcanoPodGroup(
	ctx context.Context, t *testing.T, client klient.Client,
	namespace, name string, minMember int,
) {
	t.Helper()

	pg := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "scheduling.volcano.sh/v1beta1",
			"kind":       "PodGroup",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"minMember": int64(minMember),
			},
		},
	}

	err := client.Resources(namespace).Create(ctx, pg)
	require.NoError(t, err,
		"create Volcano PodGroup %s/%s", namespace, name)
}

// DeleteVolcanoPodGroup deletes a Volcano PodGroup (best-effort).
func DeleteVolcanoPodGroup(
	ctx context.Context, client klient.Client, namespace, name string,
) {
	pg := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "scheduling.volcano.sh/v1beta1",
			"kind":       "PodGroup",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
		},
	}

	_ = client.Resources(namespace).Delete(ctx, pg)
}

// CreateGPUPodInGang creates a GPU pod annotated with the Volcano PodGroup
// name and pinned to nodeName. Returns the pod name.
func CreateGPUPodInGang(
	ctx context.Context, t *testing.T, client klient.Client,
	namespace, nodeName, podGroupName string,
) string {
	t.Helper()

	pod := NewGPUPodSpec(namespace, 1)
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}

	pod.Annotations[VolcanoPodGroupAnnotation] = podGroupName

	if nodeName != "" {
		pod.Spec.NodeName = nodeName
	}

	err := client.Resources().Create(ctx, pod)
	require.NoError(t, err, "create GPU pod in gang %s", podGroupName)
	require.NotEmpty(t, pod.Name)

	return pod.Name
}

// ExpectedVolcanoGangID returns the gang ID the preflight webhook generates
// for a Volcano PodGroup: volcano-{namespace}-{podGroupName}.
func ExpectedVolcanoGangID(namespace, podGroupName string) string {
	return fmt.Sprintf("volcano-%s-%s", namespace, podGroupName)
}
