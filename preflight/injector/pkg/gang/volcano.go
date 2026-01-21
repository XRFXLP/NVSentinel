package gang

import (
	"context"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// VolcanoPodGroupAnnotation is used by Volcano to identify pods in a PodGroup.
	// When a Volcano Job is created, it creates a PodGroup and annotates all
	// pods with this annotation containing the PodGroup name.
	VolcanoPodGroupAnnotation = "scheduling.volcano.sh/pod-group"

	// VolcanoPodGroupAnnotationLegacy is the legacy annotation used by older Volcano versions.
	VolcanoPodGroupAnnotationLegacy = "volcano.sh/pod-group"

	// SchedulingGroupNameAnnotation is the standard Kubernetes scheduling annotation
	// used by Volcano and other gang schedulers.
	SchedulingGroupNameAnnotation = "scheduling.k8s.io/group-name"

	// VolcanoQueueAnnotation indicates which Volcano queue the pod belongs to.
	VolcanoQueueAnnotation = "volcano.sh/queue-name"

	// VolcanoTaskSpecKey is used to identify which task spec within a Volcano Job
	// the pod belongs to.
	VolcanoTaskSpecKey = "volcano.sh/task-spec"
)

// VolcanoGangDiscoverer discovers gang members using Volcano's PodGroup annotation.
// Volcano is a batch scheduler for Kubernetes that supports gang scheduling.
// When pods are created via a Volcano Job, they are annotated with the PodGroup name.
type VolcanoGangDiscoverer struct {
	client client.Client
}

// NewVolcanoGangDiscoverer creates a new Volcano gang discoverer.
func NewVolcanoGangDiscoverer(c client.Client) *VolcanoGangDiscoverer {
	return &VolcanoGangDiscoverer{client: c}
}

// Name returns "volcano".
func (v *VolcanoGangDiscoverer) Name() string {
	return "volcano"
}

// CanHandle returns true if the pod has a Volcano PodGroup annotation.
func (v *VolcanoGangDiscoverer) CanHandle(pod *corev1.Pod) bool {
	if pod == nil || pod.Annotations == nil {
		return false
	}
	// Check all known annotation names for gang scheduling
	for _, ann := range []string{
		VolcanoPodGroupAnnotation,
		VolcanoPodGroupAnnotationLegacy,
		SchedulingGroupNameAnnotation,
	} {
		if _, exists := pod.Annotations[ann]; exists {
			return true
		}
	}
	return false
}

// ExtractGangID returns the PodGroup name from the Volcano annotation.
// The gang ID format is: namespace/podgroup-name
func (v *VolcanoGangDiscoverer) ExtractGangID(pod *corev1.Pod) string {
	if pod == nil || pod.Annotations == nil {
		return ""
	}

	// Try all known annotation names
	pgName := pod.Annotations[VolcanoPodGroupAnnotation]
	if pgName == "" {
		pgName = pod.Annotations[VolcanoPodGroupAnnotationLegacy]
	}
	if pgName == "" {
		pgName = pod.Annotations[SchedulingGroupNameAnnotation]
	}
	if pgName == "" {
		return ""
	}

	// Include namespace to make the gang ID globally unique
	return pod.Namespace + "/" + pgName
}

// DiscoverPeers lists all pods with the same Volcano PodGroup annotation.
func (v *VolcanoGangDiscoverer) DiscoverPeers(ctx context.Context, pod *corev1.Pod) (*GangInfo, error) {
	if pod == nil {
		return nil, fmt.Errorf("pod is nil")
	}

	gangID := v.ExtractGangID(pod)
	if gangID == "" {
		return nil, nil // Not a gang pod
	}

	// Get the PodGroup name (without namespace prefix)
	pgName := getPodGroupName(pod)

	// List all pods in the same namespace
	podList := &corev1.PodList{}
	if err := v.client.List(ctx, podList, client.InNamespace(pod.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list pods in namespace %s: %w", pod.Namespace, err)
	}

	// Filter pods that belong to the same PodGroup
	var peers []PeerInfo
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Annotations == nil {
			continue
		}

		// Check if pod belongs to the same PodGroup
		pPgName := getPodGroupName(p)
		if pPgName != pgName {
			continue
		}

		// Skip pods that are being deleted
		if p.DeletionTimestamp != nil {
			continue
		}

		peers = append(peers, PeerInfo{
			PodName:   p.Name,
			PodIP:     p.Status.PodIP,
			NodeName:  p.Spec.NodeName,
			Namespace: p.Namespace,
		})
	}

	// Get expected count from the PodGroup CRD (minMember)
	expectedCount := len(peers)
	if minMember, err := v.getPodGroupMinMember(ctx, pod.Namespace, pgName); err == nil && minMember > 0 {
		expectedCount = minMember
	}

	return &GangInfo{
		GangID:        gangID,
		ExpectedCount: expectedCount,
		Peers:         peers,
	}, nil
}

// getPodGroupName extracts the pod group name from any of the known annotations.
func getPodGroupName(pod *corev1.Pod) string {
	if pod == nil || pod.Annotations == nil {
		return ""
	}
	pgName := pod.Annotations[VolcanoPodGroupAnnotation]
	if pgName == "" {
		pgName = pod.Annotations[VolcanoPodGroupAnnotationLegacy]
	}
	if pgName == "" {
		pgName = pod.Annotations[SchedulingGroupNameAnnotation]
	}
	return pgName
}

// getPodGroupMinMember attempts to get the minMember from a Volcano PodGroup.
// This queries the PodGroup CRD directly using an unstructured client.
// Returns -1 if the PodGroup doesn't exist or minMember is not set.
func (v *VolcanoGangDiscoverer) getPodGroupMinMember(ctx context.Context, namespace, pgName string) (int, error) {
	// Volcano PodGroup is a custom resource: scheduling.volcano.sh/v1beta1 PodGroup
	// Use unstructured client to avoid importing Volcano types

	podGroup := &unstructured.Unstructured{}
	podGroup.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "scheduling.volcano.sh",
		Version: "v1beta1",
		Kind:    "PodGroup",
	})

	if err := v.client.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      pgName,
	}, podGroup); err != nil {
		return -1, err
	}

	// Extract spec.minMember
	spec, found, err := unstructured.NestedMap(podGroup.Object, "spec")
	if err != nil || !found {
		return -1, fmt.Errorf("spec not found in PodGroup")
	}

	minMember, found, err := unstructured.NestedInt64(spec, "minMember")
	if err != nil || !found {
		return -1, fmt.Errorf("minMember not found in PodGroup spec")
	}

	return int(minMember), nil
}

// GetPodGroupMinMember is the public API for getting minMember.
// Deprecated: Use getPodGroupMinMember internally.
func (v *VolcanoGangDiscoverer) GetPodGroupMinMember(ctx context.Context, namespace, pgName string) (int, error) {
	return v.getPodGroupMinMember(ctx, namespace, pgName)
}

// podGroupNameFromAnnotation extracts just the PodGroup name (not the full gang ID).
// Deprecated: use getPodGroupName instead.
func podGroupNameFromAnnotation(pod *corev1.Pod) string {
	return getPodGroupName(pod)
}

// parseIntAnnotation safely parses an integer from a pod annotation.
func parseIntAnnotation(pod *corev1.Pod, key string) (int, bool) {
	if pod == nil || pod.Annotations == nil {
		return 0, false
	}
	val, exists := pod.Annotations[key]
	if !exists {
		return 0, false
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, false
	}
	return i, true
}
