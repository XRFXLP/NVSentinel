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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

const (
	testGPUCountCurrentLabel  = "nvsentinel.dgxc.nvidia.com/gpu.count.current"
	testGPUCountExpectedLabel = "nvsentinel.dgxc.nvidia.com/gpu.count.expected"
	testNICCountCurrentLabel  = "nvsentinel.dgxc.nvidia.com/nic.count.current"
	testNICCountExpectedLabel = "nvsentinel.dgxc.nvidia.com/nic.count.expected"
)

func TestExpectedDeviceCountsLearnFromPeerNodeLabels(t *testing.T) {
	config := ExpectedDeviceCountsConfig{
		Enabled: true,
		Classes: []DeviceCountClassConfig{
			{
				Name:    "gpu",
				Enabled: true,
				Labels: DeviceCountLabels{
					Current:  testGPUCountCurrentLabel,
					Expected: testGPUCountExpectedLabel,
				},
				GroupingLabels:    []string{"node.kubernetes.io/instance-type"},
				CurrentExpression: "int(node.metadata.labels['nvidia.com/gpu.count'])",
			},
		},
	}

	nodeA := testNode("node-a", map[string]string{
		"node.kubernetes.io/instance-type": "p5",
		"nvidia.com/gpu.count":             "8",
	})
	nodeB := testNode("node-b", map[string]string{
		"node.kubernetes.io/instance-type": "p5",
		"nvidia.com/gpu.count":             "4",
	})

	labeler := newTestLabelerWithDeviceCounts(t, config, nodeA, nodeB)

	require.NoError(t, labeler.updateNodeLabels("node-b"))

	updatedNode, err := labeler.clientset.CoreV1().Nodes().Get(context.Background(), "node-b", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "4", updatedNode.Labels[testGPUCountCurrentLabel])
	require.Equal(t, "8", updatedNode.Labels[testGPUCountExpectedLabel])
}

func TestExpectedDeviceCountsOverridePrecedence(t *testing.T) {
	config := ExpectedDeviceCountsConfig{
		Enabled: true,
		Classes: []DeviceCountClassConfig{
			{
				Name:    "gpu",
				Enabled: true,
				Labels: DeviceCountLabels{
					Current:  testGPUCountCurrentLabel,
					Expected: testGPUCountExpectedLabel,
				},
				ExpectedCountOverrides: []ExpectedDeviceCountOverride{
					{
						MatchLabels: map[string]string{"nvidia.com/gpu.product": "NVIDIA-GB200"},
						Count:       8,
					},
				},
				CurrentExpression: "int(node.metadata.labels['nvidia.com/gpu.count'])",
			},
		},
	}

	node := testNode("node-a", map[string]string{
		"nvidia.com/gpu.product": "NVIDIA-GB200",
		"nvidia.com/gpu.count":   "4",
	})

	labeler := newTestLabelerWithDeviceCounts(t, config, node)

	require.NoError(t, labeler.updateNodeLabels("node-a"))

	updatedNode, err := labeler.clientset.CoreV1().Nodes().Get(context.Background(), "node-a", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "4", updatedNode.Labels[testGPUCountCurrentLabel])
	require.Equal(t, "8", updatedNode.Labels[testGPUCountExpectedLabel])
}

func TestExpectedDeviceCountsCountResourceSlices(t *testing.T) {
	config := ExpectedDeviceCountsConfig{
		Enabled: true,
		Classes: []DeviceCountClassConfig{
			{
				Name:    "nic",
				Enabled: true,
				Labels: DeviceCountLabels{
					Current:  testNICCountCurrentLabel,
					Expected: testNICCountExpectedLabel,
				},
				CurrentExpression: `
sum(resourceSlices
  .filter(rs,
    has(rs.spec.driver) &&
    rs.spec.driver == 'dra.networking.k8s.aws' &&
    has(rs.spec.devices)
  )
  .map(rs, rs.spec.devices
    .filter(d,
      has(d.attributes) &&
      'dra.vpc.amazonaws.com/deviceType' in d.attributes &&
      has(d.attributes['dra.vpc.amazonaws.com/deviceType'].string) &&
      d.attributes['dra.vpc.amazonaws.com/deviceType'].string == 'roce'
    )
    .size()
  ))`,
			},
		},
	}

	node := testNode("node-a", map[string]string{})
	labeler := newTestLabelerWithDeviceCounts(t, config, node)

	require.NoError(t, labeler.resourceSliceInformer.GetStore().Add(testResourceSlice("slice-a", "node-a",
		testDevice("roce-a", stringAttribute("roce")),
		testDevice("ethernet-a", stringAttribute("ethernet")),
		testDevice("missing-attribute", nil),
		testDevice("wrong-attribute-type", boolAttribute(true)),
	)))
	require.NoError(t, labeler.resourceSliceInformer.GetStore().Add(testResourceSlice("slice-b", "node-a",
		testDevice("roce-b", stringAttribute("roce")),
	)))
	require.NoError(t, labeler.resourceSliceInformer.GetStore().Add(testResourceSlice("slice-without-devices", "node-a")))
	require.NoError(t, labeler.resourceSliceInformer.GetStore().Add(testResourceSlice("other-node-slice", "node-b",
		testDevice("roce-c", stringAttribute("roce")),
	)))

	require.NoError(t, labeler.updateNodeLabels("node-a"))

	updatedNode, err := labeler.clientset.CoreV1().Nodes().Get(context.Background(), "node-a", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "2", updatedNode.Labels[testNICCountCurrentLabel])
	require.Equal(t, "2", updatedNode.Labels[testNICCountExpectedLabel])
}

func newTestLabelerWithDeviceCounts(
	t *testing.T,
	config ExpectedDeviceCountsConfig,
	nodes ...*corev1.Node,
) *Labeler {
	t.Helper()

	objects := make([]runtime.Object, 0, len(nodes))
	for _, node := range nodes {
		objects = append(objects, node.DeepCopy())
	}

	labeler, err := NewLabelerWithDeviceCounts(
		fake.NewSimpleClientset(objects...),
		time.Minute,
		"nvidia-dcgm",
		"nvidia-driver-daemonset",
		"nvidia-driver-installer",
		"",
		false,
		config,
	)
	require.NoError(t, err)

	labeler.ctx = context.Background()
	labeler.informersSynced = []cache.InformerSynced{func() bool { return true }}
	for _, node := range nodes {
		require.NoError(t, labeler.nodeInformer.GetStore().Add(node.DeepCopy()))
	}

	return labeler
}

func testNode(name string, nodeLabels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: nodeLabels,
		},
	}
}

func testResourceSlice(name, nodeName string, devices ...resourcev1.Device) *resourcev1.ResourceSlice {
	return &resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: resourcev1.ResourceSliceSpec{
			Driver: "dra.networking.k8s.aws",
			Pool: resourcev1.ResourcePool{
				Name:               nodeName,
				Generation:         1,
				ResourceSliceCount: 1,
			},
			NodeName: &nodeName,
			Devices:  devices,
		},
	}
}

func testDevice(name string, attribute *resourcev1.DeviceAttribute) resourcev1.Device {
	device := resourcev1.Device{
		Name: name,
	}
	if attribute != nil {
		device.Attributes = map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
			"dra.vpc.amazonaws.com/deviceType": *attribute,
		}
	}

	return device
}

func stringAttribute(value string) *resourcev1.DeviceAttribute {
	return &resourcev1.DeviceAttribute{StringValue: &value}
}

func boolAttribute(value bool) *resourcev1.DeviceAttribute {
	return &resourcev1.DeviceAttribute{BoolValue: &value}
}
