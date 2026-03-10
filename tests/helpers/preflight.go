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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
)

const (
	// PreflightNamespaceLabel is the namespace label required for preflight webhook to mutate pods.
	PreflightNamespaceLabel    = "nvsentinel.nvidia.com/preflight"
	PreflightNamespaceLabelVal = "enabled"
	// PreflightDCGMDiagName is the init container name injected by preflight for DCGM diagnostic.
	PreflightDCGMDiagName = "preflight-dcgm-diag"
	// PreflightConfigMapName is the name of the preflight config ConfigMap.
	PreflightConfigMapName = "nvsentinel-preflight-config"
	PreflightConfigKey     = "config.yaml"
	// Gang ConfigMap keys (must match preflight/pkg/gang/coordinator).
	GangConfigMapLabelManagedBy = "nvsentinel.nvidia.com/managed-by"
	GangConfigMapManagedByVal   = "preflight"
	GangDataKeyExpectedCount    = "expected_count"
	GangDataKeyPeers            = "peers"
	GangDataKeyMasterAddr       = "master_addr"
	GangDataKeyMasterPort       = "master_port"
	GangDataKeyGangID           = "gang_id"

	// VolcanoPodGroupAnnotation is the annotation key the preflight webhook reads.
	VolcanoPodGroupAnnotation = "scheduling.k8s.io/group-name"
)

// VolcanoPodGroupGVR is the GVR for Volcano PodGroups.
var VolcanoPodGroupGVR = schema.GroupVersionResource{
	Group:    "scheduling.volcano.sh",
	Version:  "v1beta1",
	Resource: "podgroups",
}

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
	if err != nil || len(deployList.Items) == 0 {
		t.Skipf("Preflight not deployed in %s: %v", NVSentinelNamespace, err)
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

// AssertPreflightConfigConfigMapExists checks that the preflight config
// ConfigMap exists and has config.yaml.
func AssertPreflightConfigConfigMapExists(
	ctx context.Context, t *testing.T, client klient.Client,
) {
	t.Helper()

	var cm v1.ConfigMap

	err := client.Resources(NVSentinelNamespace).Get(
		ctx, PreflightConfigMapName, NVSentinelNamespace, &cm,
	)
	require.NoError(t, err,
		"preflight config ConfigMap should exist when preflight is deployed")
	require.Contains(t, cm.Data, PreflightConfigKey,
		"ConfigMap %s/%s should contain %s",
		cm.Namespace, cm.Name, PreflightConfigKey)
}

// CreateGPUPodWithPreflight creates a GPU pod in the given namespace
// and schedules it on nodeName. Returns the pod name.
func CreateGPUPodWithPreflight(
	ctx context.Context, t *testing.T, client klient.Client,
	namespace, nodeName string,
) string {
	t.Helper()

	pod := NewGPUPodSpec(namespace, 1)
	if nodeName != "" {
		pod.Spec.NodeName = nodeName
	}

	err := client.Resources().Create(ctx, pod)
	require.NoError(t, err, "create GPU pod")
	require.NotEmpty(t, pod.Name, "server should set pod name after create")

	return pod.Name
}

// GetPodAfterMutation fetches the pod from the server (after webhook mutation).
func GetPodAfterMutation(
	ctx context.Context, client klient.Client, namespace, podName string,
) (*v1.Pod, error) {
	var pod v1.Pod

	err := client.Resources().Get(ctx, podName, namespace, &pod)
	if err != nil {
		return nil, err
	}

	return &pod, nil
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

// AssertGangConfigMap waits for the gang ConfigMap and asserts gang_id,
// expected_count, peer count, master_addr, and master_port.
func AssertGangConfigMap(
	ctx context.Context, t *testing.T, client klient.Client,
	testCtx *PreflightTestContext, expectedGangID string,
	expectedPeerCount int,
) {
	t.Helper()

	cm := WaitForGangConfigMap(
		ctx, t, client,
		[]string{testCtx.TestNamespace, NVSentinelNamespace},
		expectedGangID,
	)

	require.Equal(t, expectedGangID,
		cm.Data[GangDataKeyGangID])
	require.Equal(t, fmt.Sprintf("%d", expectedPeerCount),
		cm.Data[GangDataKeyExpectedCount])

	peers := cm.Data[GangDataKeyPeers]
	peerLines := strings.Split(strings.TrimSpace(peers), "\n")

	require.Len(t, peerLines, expectedPeerCount,
		"peers should have %d entries, got: %v",
		expectedPeerCount, peerLines)

	require.Contains(t, cm.Data, GangDataKeyMasterPort,
		"ConfigMap %s/%s missing master_port",
		cm.Namespace, cm.Name)

	_, hasMaster := cm.Data[GangDataKeyMasterAddr]
	require.True(t, hasMaster,
		"ConfigMap %s/%s missing master_addr",
		cm.Namespace, cm.Name)

	t.Logf("Gang ConfigMap %s/%s: gang_id=%s expected_count=%s peers=\n%s",
		cm.Namespace, cm.Name,
		cm.Data[GangDataKeyGangID],
		cm.Data[GangDataKeyExpectedCount], peers)
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

// WaitForGangConfigMap polls until a gang ConfigMap with matching gangID
// appears. Returns the ConfigMap once found.
func WaitForGangConfigMap(
	ctx context.Context, t *testing.T, client klient.Client,
	namespaces []string, gangID string,
) *v1.ConfigMap {
	t.Helper()

	var found *v1.ConfigMap

	require.Eventually(t, func() bool {
		all, err := ListGangConfigMaps(ctx, client, namespaces)
		if err != nil {
			return false
		}

		for i := range all {
			if all[i].Data[GangDataKeyGangID] == gangID {
				found = &all[i]

				return true
			}
		}

		return false
	}, EventuallyWaitTimeout, WaitInterval,
		"gang ConfigMap with gang_id=%s should appear", gangID)

	return found
}

// ExpectedVolcanoGangID returns the gang ID the preflight webhook generates
// for a Volcano PodGroup: volcano-{namespace}-{podGroupName}.
func ExpectedVolcanoGangID(namespace, podGroupName string) string {
	return fmt.Sprintf("volcano-%s-%s", namespace, podGroupName)
}
