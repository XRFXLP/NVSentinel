// Package gang provides pluggable gang discovery for preflight checks.
// Gang discovery identifies all pods that belong to the same workload group
// (e.g., for distributed training jobs that need coordinated preflight checks).
package gang

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// PeerInfo contains information about a gang member pod.
type PeerInfo struct {
	PodName   string
	PodIP     string
	NodeName  string
	Namespace string
}

// GangInfo contains the full gang information.
type GangInfo struct {
	// GangID is the unique identifier for the gang.
	GangID string

	// ExpectedCount is the total number of pods expected in the gang.
	// This may be known from scheduler annotations (e.g., Volcano's minMember).
	ExpectedCount int

	// Peers contains information about all discovered gang members.
	Peers []PeerInfo
}

// GangDiscoverer discovers all pods belonging to the same gang.
// Different schedulers (Volcano, Kueue, native K8s workloadRef) have different
// mechanisms for identifying gang members.
type GangDiscoverer interface {
	// Name returns the discoverer name (for logging/metrics).
	Name() string

	// CanHandle returns true if this discoverer can handle the given pod.
	// This is used to select the appropriate discoverer in a chain.
	CanHandle(pod *corev1.Pod) bool

	// ExtractGangID extracts the gang identifier from a pod.
	// Returns empty string if the pod doesn't belong to a gang.
	// This is a lightweight operation that doesn't require API calls.
	ExtractGangID(pod *corev1.Pod) string

	// DiscoverPeers finds all pods in the same gang.
	// This typically requires listing pods via the Kubernetes API.
	// Returns nil GangInfo if the pod doesn't belong to a gang.
	DiscoverPeers(ctx context.Context, pod *corev1.Pod) (*GangInfo, error)
}
