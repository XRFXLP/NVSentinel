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
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/yaml"

	"github.com/nvidia/nvsentinel/labeler/pkg/metrics"
)

type ExpectedDeviceCountsConfig struct {
	Enabled bool                     `json:"enabled" yaml:"enabled"`
	Classes []DeviceCountClassConfig `json:"classes" yaml:"classes"`
}

type DeviceCountClassConfig struct {
	Name                   string                        `json:"name" yaml:"name"`
	Enabled                bool                          `json:"enabled" yaml:"enabled"`
	Labels                 DeviceCountLabels             `json:"labels" yaml:"labels"`
	GroupingLabels         []string                      `json:"groupingLabels" yaml:"groupingLabels"`
	ExpectedCountOverrides []ExpectedDeviceCountOverride `json:"expectedCountOverrides" yaml:"expectedCountOverrides"`
	CurrentExpression      string                        `json:"currentExpression" yaml:"currentExpression"`
}

type DeviceCountLabels struct {
	Current  string `json:"current" yaml:"current"`
	Expected string `json:"expected" yaml:"expected"`
}

type ExpectedDeviceCountOverride struct {
	MatchLabels map[string]string `json:"matchLabels" yaml:"matchLabels"`
	Count       int               `json:"count" yaml:"count"`
}

type deviceCountManager struct {
	classes          []compiledDeviceCountClass
	managedLabelKeys map[string]struct{}
}

type compiledDeviceCountClass struct {
	DeviceCountClassConfig
	program cel.Program
}

func ParseExpectedDeviceCountsConfig(raw string) (ExpectedDeviceCountsConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return ExpectedDeviceCountsConfig{}, nil
	}

	var config ExpectedDeviceCountsConfig
	if err := yaml.Unmarshal([]byte(raw), &config); err != nil {
		return ExpectedDeviceCountsConfig{}, fmt.Errorf("parse expected device counts config: %w", err)
	}

	return config, nil
}

func newDeviceCountManager(config ExpectedDeviceCountsConfig) (*deviceCountManager, error) {
	if !config.Enabled {
		return nil, nil
	}

	// Keep the CEL surface intentionally small: expressions can only inspect
	// the reconciled node, that node's associated ResourceSlices, and sum lists.
	env, err := cel.NewEnv(
		cel.Variable("node", cel.DynType),
		cel.Variable("resourceSlices", cel.ListType(cel.DynType)),
		cel.Function("sum",
			cel.Overload("sum_list_int",
				[]*cel.Type{cel.ListType(cel.IntType)},
				cel.IntType,
				cel.FunctionBinding(sumIntList),
			),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create device-count CEL environment: %w", err)
	}

	manager := &deviceCountManager{
		managedLabelKeys: map[string]struct{}{},
	}

	for i, classConfig := range config.Classes {
		if !classConfig.Enabled {
			continue
		}

		if classConfig.Name == "" {
			return nil, fmt.Errorf("expectedDeviceCounts.classes[%d]: name is required", i)
		}
		if classConfig.Labels.Current == "" || classConfig.Labels.Expected == "" {
			return nil, fmt.Errorf("expectedDeviceCounts.classes[%d] (%s): current and expected labels are required",
				i, classConfig.Name)
		}
		if strings.TrimSpace(classConfig.CurrentExpression) == "" {
			return nil, fmt.Errorf("expectedDeviceCounts.classes[%d] (%s): currentExpression is required",
				i, classConfig.Name)
		}
		for j, override := range classConfig.ExpectedCountOverrides {
			if override.Count < 0 {
				return nil, fmt.Errorf("expectedDeviceCounts.classes[%d] (%s).expectedCountOverrides[%d]: count must be non-negative",
					i, classConfig.Name, j)
			}
		}

		ast, issues := env.Compile(classConfig.CurrentExpression)
		if issues != nil && issues.Err() != nil {
			return nil, fmt.Errorf("expectedDeviceCounts.classes[%d] (%s): compile currentExpression: %w",
				i, classConfig.Name, issues.Err())
		}
		if ast.OutputType() != cel.IntType && ast.OutputType() != cel.DynType {
			return nil, fmt.Errorf("expectedDeviceCounts.classes[%d] (%s): currentExpression must return int, got %s",
				i, classConfig.Name, ast.OutputType())
		}

		program, err := env.Program(ast)
		if err != nil {
			return nil, fmt.Errorf("expectedDeviceCounts.classes[%d] (%s): create CEL program: %w",
				i, classConfig.Name, err)
		}

		manager.classes = append(manager.classes, compiledDeviceCountClass{
			DeviceCountClassConfig: classConfig,
			program:                program,
		})

		// Node label updates caused by these derived labels should not trigger
		// another device-count reconciliation loop.
		manager.managedLabelKeys[classConfig.Labels.Current] = struct{}{}
		manager.managedLabelKeys[classConfig.Labels.Expected] = struct{}{}
	}

	if len(manager.classes) == 0 {
		return nil, nil
	}

	return manager, nil
}

func sumIntList(args ...ref.Val) ref.Val {
	if len(args) != 1 {
		return types.NewErr("sum requires exactly one argument")
	}

	list, ok := args[0].(traits.Lister)
	if !ok {
		return types.NewErr("sum argument must be list<int>")
	}

	size, ok := list.Size().(types.Int)
	if !ok {
		return types.NewErr("sum argument size is not an int")
	}

	var total int64
	for i := int64(0); i < int64(size); i++ {
		value := list.Get(types.Int(i))
		intValue, ok := value.(types.Int)
		if !ok {
			return types.NewErr("sum argument must contain only ints")
		}
		total += int64(intValue)
	}

	return types.Int(total)
}

func (m *deviceCountManager) enabled() bool {
	return m != nil && len(m.classes) > 0
}

func (m *deviceCountManager) ownsLabel(key string) bool {
	if m == nil {
		return false
	}
	_, ok := m.managedLabelKeys[key]
	return ok
}

func (m *deviceCountManager) evaluateCurrent(
	ctx context.Context,
	class compiledDeviceCountClass,
	node *corev1.Node,
	resourceSlices []*resourcev1.ResourceSlice,
) (int, error) {
	nodeMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(node)
	if err != nil {
		return 0, fmt.Errorf("convert node to CEL input: %w", err)
	}

	// Kubernetes labels are strings, but the documented GPU expression uses
	// int(node.metadata.labels['nvidia.com/gpu.count']). Normalizing numeric
	// strings avoids CEL runtime conversion gaps for dynamic map values.
	normalizeNumericNodeLabels(nodeMap)

	resourceSliceMaps := make([]map[string]any, 0, len(resourceSlices))
	for _, resourceSlice := range resourceSlices {
		resourceSliceMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resourceSlice)
		if err != nil {
			return 0, fmt.Errorf("convert ResourceSlice %s to CEL input: %w", resourceSlice.Name, err)
		}
		resourceSliceMaps = append(resourceSliceMaps, resourceSliceMap)
	}

	result, _, err := class.program.ContextEval(ctx, map[string]any{
		"node":           nodeMap,
		"resourceSlices": resourceSliceMaps,
	})
	if err != nil {
		return 0, fmt.Errorf("evaluate currentExpression: %w", err)
	}
	if types.IsUnknownOrError(result) {
		return 0, fmt.Errorf("currentExpression returned unknown/error: %v", result)
	}

	count, ok := celResultToInt(result)
	if !ok {
		return 0, fmt.Errorf("currentExpression returned non-integer: %T", result.Value())
	}
	if count < 0 {
		return 0, fmt.Errorf("currentExpression returned negative count: %d", count)
	}

	return count, nil
}

func celResultToInt(result ref.Val) (int, bool) {
	if intValue, ok := result.(types.Int); ok {
		return int(int64(intValue)), true
	}

	switch value := result.Value().(type) {
	case int:
		return value, true
	case int32:
		return int(value), true
	case int64:
		return int(value), true
	default:
		return 0, false
	}
}

func normalizeNumericNodeLabels(nodeMap map[string]any) {
	metadata, ok := nodeMap["metadata"].(map[string]any)
	if !ok {
		return
	}

	nodeLabels, ok := metadata["labels"].(map[string]any)
	if !ok {
		return
	}

	for key, value := range nodeLabels {
		raw, ok := value.(string)
		if !ok {
			continue
		}

		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err == nil {
			nodeLabels[key] = parsed
		}
	}
}

func (l *Labeler) reconcileDeviceCountLabelsInPlace(ctx context.Context, node *corev1.Node) bool {
	if !l.deviceCounts.enabled() {
		return false
	}

	needsUpdate := false
	resourceSlices := l.resourceSlicesForNode(node)

	for _, class := range l.deviceCounts.classes {
		// Do not turn a missing DRA source into current=0. A ResourceSlice-based
		// expression should wait until at least one associated slice exists.
		if class.referencesResourceSlices() && len(resourceSlices) == 0 {
			metrics.DeviceCountSkippedUpdates.WithLabelValues(class.Name, metrics.SkipReasonMissingSource).Inc()
			slog.Warn("Skipping device count label update because no ResourceSlices are associated with the node",
				"node", node.Name, "class", class.Name)
			continue
		}

		current, err := l.deviceCounts.evaluateCurrent(ctx, class, node, resourceSlices)
		if err != nil {
			metrics.DeviceCountSkippedUpdates.WithLabelValues(class.Name, metrics.SkipReasonEvaluationError).Inc()
			slog.Warn("Skipping device count label update after current count evaluation failed",
				"node", node.Name, "class", class.Name, "error", err)
			continue
		}

		expected := l.expectedDeviceCount(ctx, class, node, current)
		currentValue := strconv.Itoa(current)
		expectedValue := strconv.Itoa(expected)
		partitionKey := class.partitionKey(node)

		metrics.CurrentDeviceCount.WithLabelValues(node.Name, class.Name).Set(float64(current))
		metrics.ExpectedDeviceCount.WithLabelValues(class.Name, partitionKey).Set(float64(expected))

		if node.Labels[class.Labels.Current] != currentValue {
			node.Labels[class.Labels.Current] = currentValue
			needsUpdate = true
		}
		if node.Labels[class.Labels.Expected] != expectedValue {
			node.Labels[class.Labels.Expected] = expectedValue
			needsUpdate = true
		}
	}

	return needsUpdate
}

func (l *Labeler) expectedDeviceCount(
	ctx context.Context,
	class compiledDeviceCountClass,
	node *corev1.Node,
	current int,
) int {
	if override, ok := class.expectedOverride(node); ok {
		return override
	}

	// Learned expected counts can rise from current observations or previously
	// written labels, but they must not fall automatically when devices vanish.
	expected := current
	if existing, ok := parseCountLabel(node.Labels[class.Labels.Expected]); ok && existing > expected {
		expected = existing
	}

	partitionKey := class.partitionKey(node)
	for _, obj := range l.nodeInformer.GetStore().List() {
		peer, ok := obj.(*corev1.Node)
		if !ok || class.partitionKey(peer) != partitionKey {
			continue
		}

		if existing, ok := parseCountLabel(peer.Labels[class.Labels.Expected]); ok && existing > expected {
			expected = existing
		}

		peerResourceSlices := l.resourceSlicesForNode(peer)
		// A peer with no DRA source should not lower or initialize the baseline
		// for ResourceSlice-backed classes.
		if class.referencesResourceSlices() && len(peerResourceSlices) == 0 {
			metrics.DeviceCountSkippedUpdates.WithLabelValues(class.Name, metrics.SkipReasonMissingSource).Inc()
			continue
		}

		peerCurrent, err := l.deviceCounts.evaluateCurrent(ctx, class, peer, peerResourceSlices)
		if err != nil {
			metrics.DeviceCountSkippedUpdates.WithLabelValues(class.Name, metrics.SkipReasonEvaluationError).Inc()
			slog.Warn("Skipping peer device count during expected count learning",
				"node", node.Name, "peer", peer.Name, "class", class.Name, "error", err)
			continue
		}
		if peerCurrent > expected {
			expected = peerCurrent
		}
	}

	return expected
}

func parseCountLabel(raw string) (int, bool) {
	if raw == "" {
		return 0, false
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, false
	}

	return value, true
}

func (class compiledDeviceCountClass) expectedOverride(node *corev1.Node) (int, bool) {
	for _, override := range class.ExpectedCountOverrides {
		if matchLabels(node.Labels, override.MatchLabels) {
			return override.Count, true
		}
	}

	return 0, false
}

func (class compiledDeviceCountClass) referencesResourceSlices() bool {
	// This cheap check is only used to distinguish "missing DRA source" from a
	// legitimate zero count. Expressions that do not reference ResourceSlices can
	// still evaluate from node labels alone.
	return strings.Contains(class.CurrentExpression, "resourceSlices")
}

func matchLabels(actual, expected map[string]string) bool {
	for key, expectedValue := range expected {
		if actual[key] != expectedValue {
			return false
		}
	}

	return true
}

func (class compiledDeviceCountClass) partitionKey(node *corev1.Node) string {
	if len(class.GroupingLabels) == 0 {
		return "default"
	}

	// The class name is not included here because callers already track class
	// separately; the partition is just the configured hardware grouping.
	parts := make([]string, 0, len(class.GroupingLabels))
	for _, label := range class.GroupingLabels {
		parts = append(parts, label+"="+node.Labels[label])
	}

	return strings.Join(parts, "|")
}

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
	default:
		return false
	}
}

func (l *Labeler) nodeLabelsAffectDeviceCounts(oldLabels, newLabels map[string]string) bool {
	if !l.deviceCounts.enabled() {
		return false
	}

	oldInputLabels := maps.Clone(oldLabels)
	newInputLabels := maps.Clone(newLabels)
	for key := range l.deviceCounts.managedLabelKeys {
		// Ignore labels owned by this feature to avoid self-triggered updates.
		delete(oldInputLabels, key)
		delete(newInputLabels, key)
	}

	return !maps.Equal(oldInputLabels, newInputLabels)
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
				slog.Warn("Skipping ResourceSlice add event with unexpected object type", "type", fmt.Sprintf("%T", obj))
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
				slog.Warn("Skipping ResourceSlice delete event with unexpected object type", "type", fmt.Sprintf("%T", obj))
				return
			}

			l.handleResourceSliceEvent(resourceSlice)
		},
	}
}
