package gang

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkloadRefGangDiscoverer discovers gang members using Kubernetes 1.35+ native workloadRef.
// The workloadRef field provides a standard way to identify pods that belong to the same
// workload, including gang/pod-group information.
type WorkloadRefGangDiscoverer struct {
	client client.Client
}

// NewWorkloadRefGangDiscoverer creates a new workloadRef-based gang discoverer.
func NewWorkloadRefGangDiscoverer(c client.Client) *WorkloadRefGangDiscoverer {
	return &WorkloadRefGangDiscoverer{client: c}
}

// Name returns "workloadref".
func (w *WorkloadRefGangDiscoverer) Name() string {
	return "workloadref"
}

// CanHandle returns true if the pod has a workloadRef with a PodGroup set.
func (w *WorkloadRefGangDiscoverer) CanHandle(pod *corev1.Pod) bool {
	if pod == nil || pod.Spec.WorkloadRef == nil {
		return false
	}
	// Only handle if PodGroup is set (indicates gang scheduling)
	return pod.Spec.WorkloadRef.PodGroup != ""
}

// ExtractGangID returns the gang identifier from the workloadRef.
// Format: workload-name/pod-group[/replica-key]
func (w *WorkloadRefGangDiscoverer) ExtractGangID(pod *corev1.Pod) string {
	if pod == nil || pod.Spec.WorkloadRef == nil {
		return ""
	}

	wr := pod.Spec.WorkloadRef
	if wr.PodGroup == "" {
		return ""
	}

	// Include namespace for global uniqueness
	base := pod.Namespace + "/" + wr.Name + "/" + wr.PodGroup
	if wr.PodGroupReplicaKey != "" {
		return base + "/" + wr.PodGroupReplicaKey
	}
	return base
}

// DiscoverPeers finds all pods with the same workloadRef.
func (w *WorkloadRefGangDiscoverer) DiscoverPeers(ctx context.Context, pod *corev1.Pod) (*GangInfo, error) {
	if pod == nil {
		return nil, fmt.Errorf("pod is nil")
	}

	gangID := w.ExtractGangID(pod)
	if gangID == "" {
		return nil, nil // Not a gang pod
	}

	wr := pod.Spec.WorkloadRef

	// List all pods in the same namespace
	podList := &corev1.PodList{}
	if err := w.client.List(ctx, podList, client.InNamespace(pod.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list pods in namespace %s: %w", pod.Namespace, err)
	}

	// Filter pods that belong to the same workload and pod group
	var peers []PeerInfo
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Spec.WorkloadRef == nil {
			continue
		}

		pwr := p.Spec.WorkloadRef
		// Match on workload name and pod group
		if pwr.Name != wr.Name || pwr.PodGroup != wr.PodGroup {
			continue
		}

		// If replica key is set, it must also match
		if wr.PodGroupReplicaKey != "" && pwr.PodGroupReplicaKey != wr.PodGroupReplicaKey {
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

	return &GangInfo{
		GangID:        gangID,
		ExpectedCount: len(peers), // Could potentially get from workload resource
		Peers:         peers,
	}, nil
}
