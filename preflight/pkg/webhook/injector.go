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
	"log/slog"

	"github.com/nvidia/nvsentinel/preflight/pkg/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type PatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

type Injector struct {
	cfg *config.Config
}

func NewInjector(cfg *config.Config) *Injector {
	return &Injector{cfg: cfg}
}

func (i *Injector) InjectInitContainers(pod *corev1.Pod) ([]PatchOperation, error) {
	if !i.hasGPUResources(pod) {
		slog.Debug("Pod does not request GPU resources, skipping injection")
		return nil, nil
	}

	gpuContainer := i.findGPUContainer(pod)
	if gpuContainer == nil {
		slog.Debug("No GPU container found, skipping injection")
		return nil, nil
	}

	initContainers := i.buildInitContainers(gpuContainer)
	if len(initContainers) == 0 {
		return nil, nil
	}

	var patches []PatchOperation

	if len(pod.Spec.InitContainers) == 0 {
		patches = append(patches, PatchOperation{
			Op:    "add",
			Path:  "/spec/initContainers",
			Value: initContainers,
		})
	} else {
		for idx, container := range initContainers {
			patches = append(patches, PatchOperation{
				Op:    "add",
				Path:  "/spec/initContainers/" + string(rune('0'+idx)),
				Value: container,
			})
		}
	}

	return patches, nil
}

func (i *Injector) hasGPUResources(pod *corev1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		for _, resourceName := range i.cfg.GPUResourceNames {
			if quantity, ok := container.Resources.Requests[corev1.ResourceName(resourceName)]; ok {
				if !quantity.IsZero() {
					return true
				}
			}

			if quantity, ok := container.Resources.Limits[corev1.ResourceName(resourceName)]; ok {
				if !quantity.IsZero() {
					return true
				}
			}
		}
	}

	return false
}

func (i *Injector) findGPUContainer(pod *corev1.Pod) *corev1.Container {
	for idx := range pod.Spec.Containers {
		container := &pod.Spec.Containers[idx]
		for _, resourceName := range i.cfg.GPUResourceNames {
			if quantity, ok := container.Resources.Requests[corev1.ResourceName(resourceName)]; ok {
				if !quantity.IsZero() {
					return container
				}
			}

			if quantity, ok := container.Resources.Limits[corev1.ResourceName(resourceName)]; ok {
				if !quantity.IsZero() {
					return container
				}
			}
		}
	}

	return nil
}

func (i *Injector) buildInitContainers(gpuContainer *corev1.Container) []corev1.Container {
	var initContainers []corev1.Container

	for _, tmpl := range i.cfg.InitContainers {
		container := tmpl.DeepCopy()

		if container.Resources.Requests == nil {
			container.Resources.Requests = make(corev1.ResourceList)
		}

		if container.Resources.Limits == nil {
			container.Resources.Limits = make(corev1.ResourceList)
		}

		copyResources(&container.Resources, &gpuContainer.Resources, i.cfg.GPUResourceNames)
		copyResources(&container.Resources, &gpuContainer.Resources, i.cfg.NetworkResourceNames)

		if _, ok := container.Resources.Requests[corev1.ResourceCPU]; !ok {
			container.Resources.Requests[corev1.ResourceCPU] = resource.MustParse("100m")
		}

		if _, ok := container.Resources.Requests[corev1.ResourceMemory]; !ok {
			container.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("500Mi")
		}

		initContainers = append(initContainers, *container)
	}

	return initContainers
}

func copyResources(dst, src *corev1.ResourceRequirements, resourceNames []string) {
	for _, name := range resourceNames {
		resName := corev1.ResourceName(name)
		if quantity, ok := src.Requests[resName]; ok {
			dst.Requests[resName] = quantity
		}

		if quantity, ok := src.Limits[resName]; ok {
			dst.Limits[resName] = quantity
		}
	}
}
