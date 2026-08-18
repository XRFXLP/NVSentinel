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
				// Namespaces is intentionally not set. PreflightConfig resources
				// can add namespace-specific discoverers after the manager starts,
				// while controller-runtime cache namespace scope is immutable.
				Transform: transformPodForCache,
			},
		},
	}
}

func transformPodForCache(obj any) (any, error) {
	switch pod := obj.(type) {
	case *corev1.Pod:
		return transformTypedPod(pod), nil
	case *unstructured.Unstructured:
		return transformUnstructuredPod(pod), nil
	default:
		return nil, fmt.Errorf("expected Pod cache object, got %T", obj)
	}
}

func transformTypedPod(pod *corev1.Pod) *corev1.Pod {
	objectMeta := pod.ObjectMeta
	nodeName := pod.Spec.NodeName
	volumes := gangConfigVolumesForCache(pod.Spec.Volumes)
	schedulingGroup := schedulingGroupForCache(pod.Spec.SchedulingGroup)
	phase := pod.Status.Phase
	podIP := pod.Status.PodIP

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
		NodeName:        nodeName,
		Volumes:         volumes,
		SchedulingGroup: schedulingGroup,
	}
	pod.Status = corev1.PodStatus{
		Phase: phase,
		PodIP: podIP,
	}

	return pod
}

// gangConfigVolumesForCache keeps only the injected gang ConfigMap reference.
// Other volume sources are not used by gang reconciliation or discovery.
func gangConfigVolumesForCache(volumes []corev1.Volume) []corev1.Volume {
	var cachedVolumes []corev1.Volume

	for _, volume := range volumes {
		if volume.Name != gangtypes.GangConfigVolumeName {
			continue
		}

		cachedVolume := corev1.Volume{Name: volume.Name}
		if volume.ConfigMap != nil {
			cachedVolume.ConfigMap = &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: volume.ConfigMap.Name,
				},
			}
		}

		cachedVolumes = append(cachedVolumes, cachedVolume)
	}

	return cachedVolumes
}

// schedulingGroupForCache keeps the PodGroup name used by native Kubernetes
// gang discovery.
func schedulingGroupForCache(group *corev1.PodSchedulingGroup) *corev1.PodSchedulingGroup {
	if group == nil || group.PodGroupName == nil {
		return nil
	}

	podGroupName := *group.PodGroupName

	return &corev1.PodSchedulingGroup{PodGroupName: &podGroupName}
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
	copyUnstructuredSchedulingGroup(pod, transformed)
	// Kubernetes 1.35 workloadRef is unstructured because the field was
	// replaced by schedulingGroup in the Kubernetes 1.36 Go API.
	copyNestedField(pod, transformed, "spec", "workloadRef")
	copyNestedField(pod, transformed, "status", "podIP")
	copyNestedField(pod, transformed, "status", "phase")

	pod.Object = transformed.Object

	return pod
}

// copyUnstructuredGangConfigVolume applies the typed volume projection to the
// unstructured Pods used by Kubernetes 1.35 workloadRef discovery.
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

		cachedVolume := map[string]any{"name": gangtypes.GangConfigVolumeName}
		if configMapName, exists, _ := unstructured.NestedString(volume, "configMap", "name"); exists {
			cachedVolume["configMap"] = map[string]any{"name": configMapName}
		}

		_ = unstructured.SetNestedSlice(to.Object, []any{cachedVolume}, "spec", "volumes")

		return
	}
}

// copyUnstructuredSchedulingGroup keeps only the PodGroup name when an
// unstructured Pod is read through the manager cache.
func copyUnstructuredSchedulingGroup(from, to *unstructured.Unstructured) {
	podGroupName, found, err := unstructured.NestedString(
		from.Object, "spec", "schedulingGroup", "podGroupName")
	if err != nil || !found {
		return
	}

	_ = unstructured.SetNestedField(
		to.Object, podGroupName, "spec", "schedulingGroup", "podGroupName")
}

// copyNestedField copies one field without retaining its surrounding object.
func copyNestedField(from, to *unstructured.Unstructured, fields ...string) {
	value, found, err := unstructured.NestedFieldCopy(from.Object, fields...)
	if err != nil || !found {
		return
	}

	_ = unstructured.SetNestedField(to.Object, value, fields...)
}
