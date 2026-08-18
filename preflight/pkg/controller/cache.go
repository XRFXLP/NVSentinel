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

package controller

import (
	"fmt"

	gangtypes "github.com/nvidia/nvsentinel/preflight/pkg/gang/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ManagerCacheOptions reduces the memory used by the manager's cluster-wide Pod
// informer while retaining every field used by the gang controller and gang
// discoverers.
func ManagerCacheOptions() cache.Options {
	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Pod{}: {
				// An empty map explicitly keeps this cache cluster-wide instead
				// of inheriting any future DefaultNamespaces configuration.
				Namespaces: map[string]cache.Config{},
				Transform:  transformPodForCache,
			},
		},
	}
}

func transformPodForCache(obj any) (any, error) {
	switch pod := obj.(type) {
	case *corev1.Pod:
		return transformTypedPod(pod), nil
	case *unstructured.Unstructured:
		// Kubernetes 1.35 Pods are read as unstructured objects so their
		// spec.workloadRef field survives decoding by the Kubernetes 1.36 client.
		return transformUnstructuredPod(pod), nil
	default:
		return nil, fmt.Errorf("expected Pod cache object, got %T", obj)
	}
}

func transformTypedPod(pod *corev1.Pod) *corev1.Pod {
	objectMeta := pod.ObjectMeta
	spec := pod.Spec
	status := pod.Status

	pod.TypeMeta = metav1.TypeMeta{}
	pod.ObjectMeta = metav1.ObjectMeta{
		Name:              objectMeta.Name,
		Namespace:         objectMeta.Namespace,
		UID:               objectMeta.UID,
		ResourceVersion:   objectMeta.ResourceVersion,
		DeletionTimestamp: objectMeta.DeletionTimestamp,
		Annotations:       objectMeta.Annotations,
		Labels:            objectMeta.Labels,
	}
	pod.Spec = corev1.PodSpec{
		NodeName:        spec.NodeName,
		Volumes:         gangConfigVolumesForCache(spec.Volumes),
		SchedulingGroup: spec.SchedulingGroup,
	}
	pod.Status = corev1.PodStatus{
		Phase: status.Phase,
		PodIP: status.PodIP,
	}

	return pod
}

// gangConfigVolumesForCache keeps the injected gang volume identity and the
// ConfigMap name used by reconciliation.
func gangConfigVolumesForCache(volumes []corev1.Volume) []corev1.Volume {
	for _, volume := range volumes {
		if volume.Name != gangtypes.GangConfigVolumeName {
			continue
		}

		cached := corev1.Volume{Name: volume.Name}
		if volume.ConfigMap != nil {
			cached.ConfigMap = &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: volume.ConfigMap.Name},
			}
		}

		return []corev1.Volume{cached}
	}

	return nil
}

func transformUnstructuredPod(pod *unstructured.Unstructured) *unstructured.Unstructured {
	transformed := &unstructured.Unstructured{}
	transformed.SetGroupVersionKind(pod.GroupVersionKind())
	transformed.SetName(pod.GetName())
	transformed.SetNamespace(pod.GetNamespace())
	transformed.SetUID(pod.GetUID())
	transformed.SetResourceVersion(pod.GetResourceVersion())
	transformed.SetDeletionTimestamp(pod.GetDeletionTimestamp())
	transformed.SetAnnotations(pod.GetAnnotations())
	transformed.SetLabels(pod.GetLabels())

	copyNestedField(pod, transformed, "spec", "nodeName")
	copyUnstructuredGangConfigVolume(pod, transformed)
	copyNestedField(pod, transformed, "spec", "schedulingGroup")
	// Kubernetes 1.35 workloadRef is unstructured because the field was
	// replaced by schedulingGroup in the Kubernetes 1.36 Go API.
	copyNestedField(pod, transformed, "spec", "workloadRef")
	copyNestedField(pod, transformed, "status", "podIP")
	copyNestedField(pod, transformed, "status", "phase")

	pod.Object = transformed.Object

	return pod
}

func copyUnstructuredGangConfigVolume(from, to *unstructured.Unstructured) {
	volumes, found, err := unstructured.NestedSlice(from.Object, "spec", "volumes")
	if err != nil || !found {
		return
	}

	for _, value := range volumes {
		volume, ok := value.(map[string]any)
		if !ok || volume["name"] != gangtypes.GangConfigVolumeName {
			continue
		}

		cached := map[string]any{"name": gangtypes.GangConfigVolumeName}
		if name, exists, _ := unstructured.NestedString(volume, "configMap", "name"); exists {
			cached["configMap"] = map[string]any{"name": name}
		}

		_ = unstructured.SetNestedSlice(to.Object, []any{cached}, "spec", "volumes")

		return
	}
}

// copyNestedField copies one field without retaining its surrounding object.
func copyNestedField(from, to *unstructured.Unstructured, fields ...string) {
	value, found, err := unstructured.NestedFieldCopy(from.Object, fields...)
	if err != nil || !found {
		return
	}

	_ = unstructured.SetNestedField(to.Object, value, fields...)
}
