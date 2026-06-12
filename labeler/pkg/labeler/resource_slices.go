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
	"fmt"
	"log/slog"
	"reflect"
	"slices"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/tools/cache"
)

func (l *Labeler) resourceSlicesForNode(node *corev1.Node) []*resourcev1.ResourceSlice {
	if l.resourceSliceInformer == nil {
		return nil
	}

	// ResourceSlices are cluster-scoped and may target nodes directly, through a
	// selector, all nodes, or per-device node selection, so filter in-process.
	resourceSlices := []*resourcev1.ResourceSlice{}

	for _, obj := range l.resourceSliceInformer.GetStore().List() {
		resourceSlice, ok := obj.(*resourcev1.ResourceSlice)
		if !ok {
			continue
		}

		if resourceSliceAppliesToNode(resourceSlice, node) {
			resourceSlices = append(resourceSlices, resourceSlice)
		}
	}

	return resourceSlices
}

func resourceSliceAppliesToNode(resourceSlice *resourcev1.ResourceSlice, node *corev1.Node) bool {
	spec := resourceSlice.Spec

	// ResourceSlice has mutually exclusive node selection modes. Mirror those
	// modes here so expressions only see slices relevant to the reconciled node.
	if spec.NodeName != nil {
		return *spec.NodeName == node.Name
	}

	if spec.AllNodes != nil {
		return *spec.AllNodes
	}

	if spec.NodeSelector != nil {
		return nodeSelectorMatches(spec.NodeSelector, node)
	}

	if spec.PerDeviceNodeSelection != nil && *spec.PerDeviceNodeSelection {
		return slices.ContainsFunc(spec.Devices, func(device resourcev1.Device) bool {
			return deviceAppliesToNode(device, node)
		})
	}

	return false
}

func deviceAppliesToNode(device resourcev1.Device, node *corev1.Node) bool {
	if device.NodeName != nil {
		return *device.NodeName == node.Name
	}

	if device.AllNodes != nil {
		return *device.AllNodes
	}

	if device.NodeSelector != nil {
		return nodeSelectorMatches(device.NodeSelector, node)
	}

	return false
}

func nodeSelectorMatches(nodeSelector *corev1.NodeSelector, node *corev1.Node) bool {
	for _, term := range nodeSelector.NodeSelectorTerms {
		if nodeSelectorTermMatches(term, node) {
			return true
		}
	}

	return false
}

func nodeSelectorTermMatches(term corev1.NodeSelectorTerm, node *corev1.Node) bool {
	for _, requirement := range term.MatchExpressions {
		operator, ok := nodeSelectorOperator(requirement.Operator)
		if !ok {
			return false
		}

		selectorRequirement, err := labels.NewRequirement(
			requirement.Key,
			operator,
			requirement.Values,
		)
		if err != nil || !selectorRequirement.Matches(labels.Set(node.Labels)) {
			return false
		}
	}

	for _, requirement := range term.MatchFields {
		if !nodeFieldRequirementMatches(requirement, node) {
			return false
		}
	}

	return true
}

func nodeSelectorOperator(operator corev1.NodeSelectorOperator) (selection.Operator, bool) {
	switch operator {
	case corev1.NodeSelectorOpIn:
		return selection.In, true
	case corev1.NodeSelectorOpNotIn:
		return selection.NotIn, true
	case corev1.NodeSelectorOpExists:
		return selection.Exists, true
	case corev1.NodeSelectorOpDoesNotExist:
		return selection.DoesNotExist, true
	case corev1.NodeSelectorOpGt:
		return selection.GreaterThan, true
	case corev1.NodeSelectorOpLt:
		return selection.LessThan, true
	default:
		return "", false
	}
}

func nodeFieldRequirementMatches(requirement corev1.NodeSelectorRequirement, node *corev1.Node) bool {
	// Kubernetes supports a small set of node fields in selectors. The only one
	// needed here is metadata.name; unsupported fields simply do not match.
	fields := map[string]string{
		"metadata.name": node.Name,
	}

	value, exists := fields[requirement.Key]
	switch requirement.Operator {
	case corev1.NodeSelectorOpIn:
		return exists && slices.Contains(requirement.Values, value)
	case corev1.NodeSelectorOpNotIn:
		return !exists || !slices.Contains(requirement.Values, value)
	case corev1.NodeSelectorOpExists:
		return exists
	case corev1.NodeSelectorOpDoesNotExist:
		return !exists
	case corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt:
		return false
	default:
		return false
	}
}

func (l *Labeler) handleResourceSliceEvent(resourceSlices ...*resourcev1.ResourceSlice) {
	if !l.allInformersSynced() {
		return
	}

	if len(resourceSlices) == 0 {
		// Kept as a defensive fallback for callers that cannot identify a slice.
		l.reconcileAllNodes()
		return
	}

	for nodeName := range l.nodeNamesForResourceSlices(resourceSlices...) {
		if err := l.updateNodeLabels(nodeName); err != nil {
			slog.Error("Failed to reconcile node labels after ResourceSlice event",
				"node", nodeName, "error", err)
		}
	}
}

func (l *Labeler) nodeNamesForResourceSlices(resourceSlices ...*resourcev1.ResourceSlice) map[string]struct{} {
	nodeNames := map[string]struct{}{}

	// Selector-based ResourceSlices require comparing against cached nodes; for
	// updates, callers pass old and new slices to include nodes that lost a slice.
	for _, obj := range l.nodeInformer.GetStore().List() {
		node, ok := obj.(*corev1.Node)
		if !ok {
			continue
		}

		for _, resourceSlice := range resourceSlices {
			if resourceSlice != nil && resourceSliceAppliesToNode(resourceSlice, node) {
				nodeNames[node.Name] = struct{}{}
				break
			}
		}
	}

	return nodeNames
}

func resourceSliceFromEventObject(obj any) (*resourcev1.ResourceSlice, bool) {
	resourceSlice, ok := obj.(*resourcev1.ResourceSlice)
	if ok {
		return resourceSlice, true
	}

	// Delete events can arrive as tombstones when the informer misses the final object state.
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}

	resourceSlice, ok = tombstone.Obj.(*resourcev1.ResourceSlice)

	return resourceSlice, ok
}

func newResourceSliceEventHandlers(l *Labeler) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			resourceSlice, ok := resourceSliceFromEventObject(obj)
			if !ok {
				slog.Warn("Skipping ResourceSlice add event with unexpected object type",
					"type", fmt.Sprintf("%T", obj))

				return
			}

			l.handleResourceSliceEvent(resourceSlice)
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldResourceSlice, oldOk := resourceSliceFromEventObject(oldObj)
			newResourceSlice, newOk := resourceSliceFromEventObject(newObj)

			if !oldOk || !newOk {
				slog.Warn("Skipping ResourceSlice update event with unexpected object type",
					"oldType", fmt.Sprintf("%T", oldObj), "newType", fmt.Sprintf("%T", newObj))

				return
			}

			// Device-count expressions only read ResourceSlice spec; ignore metadata-only churn.
			if reflect.DeepEqual(oldResourceSlice.Spec, newResourceSlice.Spec) {
				return
			}

			// Reconcile nodes matched by both old and new specs in case node selection changed.
			l.handleResourceSliceEvent(oldResourceSlice, newResourceSlice)
		},
		DeleteFunc: func(obj any) {
			resourceSlice, ok := resourceSliceFromEventObject(obj)
			if !ok {
				slog.Warn("Skipping ResourceSlice delete event with unexpected object type",
					"type", fmt.Sprintf("%T", obj))

				return
			}

			l.handleResourceSliceEvent(resourceSlice)
		},
	}
}
