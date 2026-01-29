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

package webhook

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/nvidia/nvsentinel/preflight/pkg/config"
)

type Injector struct {
	initContainers   []corev1.Container
	gpuResourceNames []string
}

func NewInjector(cfg *config.Config) *Injector {
	return &Injector{
		initContainers:   cfg.InitContainers,
		gpuResourceNames: cfg.GPUResourceNames,
	}
}

type JSONPatchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

func (i *Injector) CreatePatch(pod *corev1.Pod) ([]JSONPatchOp, error) {
	gpuCount := i.getMaxGPUCount(pod)
	if gpuCount == 0 {
		return nil, nil
	}

	initContainers := i.buildInitContainers(gpuCount)
	if len(initContainers) == 0 {
		return nil, nil
	}

	var patch []JSONPatchOp

	if len(pod.Spec.InitContainers) == 0 {
		patch = append(patch, JSONPatchOp{
			Op:    "add",
			Path:  "/spec/initContainers",
			Value: initContainers,
		})
	} else {
		for idx := len(initContainers) - 1; idx >= 0; idx-- {
			patch = append(patch, JSONPatchOp{
				Op:    "add",
				Path:  "/spec/initContainers/0",
				Value: initContainers[idx],
			})
		}
	}

	return patch, nil
}

func (i *Injector) getMaxGPUCount(pod *corev1.Pod) int64 {
	var maxGPU int64

	for _, container := range pod.Spec.Containers {
		for _, resourceName := range i.gpuResourceNames {
			if qty, ok := container.Resources.Limits[corev1.ResourceName(resourceName)]; ok {
				if count := qty.Value(); count > maxGPU {
					maxGPU = count
				}
			}
			if qty, ok := container.Resources.Requests[corev1.ResourceName(resourceName)]; ok {
				if count := qty.Value(); count > maxGPU {
					maxGPU = count
				}
			}
		}
	}

	return maxGPU
}

func (i *Injector) buildInitContainers(gpuCount int64) []corev1.Container {
	containers := make([]corev1.Container, len(i.initContainers))

	for idx, ic := range i.initContainers {
		container := ic.DeepCopy()

		if container.Resources.Limits == nil {
			container.Resources.Limits = corev1.ResourceList{}
		}
		if container.Resources.Requests == nil {
			container.Resources.Requests = corev1.ResourceList{}
		}

		gpuQty := resource.NewQuantity(gpuCount, resource.DecimalSI)
		for _, resourceName := range i.gpuResourceNames {
			container.Resources.Limits[corev1.ResourceName(resourceName)] = *gpuQty
			container.Resources.Requests[corev1.ResourceName(resourceName)] = *gpuQty
		}

		if container.SecurityContext == nil {
			container.SecurityContext = &corev1.SecurityContext{
				AllowPrivilegeEscalation: boolPtr(false),
				ReadOnlyRootFilesystem:   boolPtr(true),
				RunAsNonRoot:             boolPtr(true),
				Capabilities: &corev1.Capabilities{
					Drop: []corev1.Capability{"ALL"},
				},
			}
		}

		containers[idx] = *container
	}

	return containers
}

func boolPtr(b bool) *bool {
	return &b
}
