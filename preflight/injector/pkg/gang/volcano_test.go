package gang

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestVolcanoGangDiscoverer_CanHandle(t *testing.T) {
	discoverer := &VolcanoGangDiscoverer{}

	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected bool
	}{
		{
			name:     "nil pod",
			pod:      nil,
			expected: false,
		},
		{
			name: "pod without annotations",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
			},
			expected: false,
		},
		{
			name: "pod with current volcano annotation",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						VolcanoPodGroupAnnotation: "my-podgroup",
					},
				},
			},
			expected: true,
		},
		{
			name: "pod with legacy volcano annotation",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						VolcanoPodGroupAnnotationLegacy: "my-podgroup",
					},
				},
			},
			expected: true,
		},
		{
			name: "pod with unrelated annotations",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"other-annotation": "value",
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := discoverer.CanHandle(tt.pod)
			if result != tt.expected {
				t.Errorf("CanHandle() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestVolcanoGangDiscoverer_ExtractGangID(t *testing.T) {
	discoverer := &VolcanoGangDiscoverer{}

	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected string
	}{
		{
			name:     "nil pod",
			pod:      nil,
			expected: "",
		},
		{
			name: "pod with current annotation",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "training",
					Annotations: map[string]string{
						VolcanoPodGroupAnnotation: "training-job-pg",
					},
				},
			},
			expected: "training/training-job-pg",
		},
		{
			name: "pod with legacy annotation",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "ml-jobs",
					Annotations: map[string]string{
						VolcanoPodGroupAnnotationLegacy: "distributed-training",
					},
				},
			},
			expected: "ml-jobs/distributed-training",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := discoverer.ExtractGangID(tt.pod)
			if result != tt.expected {
				t.Errorf("ExtractGangID() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestVolcanoGangDiscoverer_DiscoverPeers(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create test pods that belong to the same PodGroup
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "training-job-worker-0",
			Namespace: "training",
			Annotations: map[string]string{
				VolcanoPodGroupAnnotation: "training-job-pg",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}

	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "training-job-worker-1",
			Namespace: "training",
			Annotations: map[string]string{
				VolcanoPodGroupAnnotation: "training-job-pg",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-2",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.2",
		},
	}

	// Pod in different PodGroup
	pod3 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-job-worker-0",
			Namespace: "training",
			Annotations: map[string]string{
				VolcanoPodGroupAnnotation: "other-job-pg",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-3",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.3",
		},
	}

	// Create fake client with test pods
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod1, pod2, pod3).
		Build()

	discoverer := NewVolcanoGangDiscoverer(fakeClient)

	t.Run("discover peers in same PodGroup", func(t *testing.T) {
		gangInfo, err := discoverer.DiscoverPeers(context.Background(), pod1)
		if err != nil {
			t.Fatalf("DiscoverPeers() error = %v", err)
		}

		if gangInfo == nil {
			t.Fatal("DiscoverPeers() returned nil GangInfo")
		}

		if gangInfo.GangID != "training/training-job-pg" {
			t.Errorf("GangID = %v, want %v", gangInfo.GangID, "training/training-job-pg")
		}

		if len(gangInfo.Peers) != 2 {
			t.Errorf("len(Peers) = %v, want 2", len(gangInfo.Peers))
		}

		// Verify peer details
		peerNames := make(map[string]bool)
		for _, peer := range gangInfo.Peers {
			peerNames[peer.PodName] = true
		}

		if !peerNames["training-job-worker-0"] {
			t.Error("Expected training-job-worker-0 in peers")
		}
		if !peerNames["training-job-worker-1"] {
			t.Error("Expected training-job-worker-1 in peers")
		}
		if peerNames["other-job-worker-0"] {
			t.Error("other-job-worker-0 should not be in peers")
		}
	})

	t.Run("pod without annotation returns nil", func(t *testing.T) {
		podWithoutAnnotation := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "standalone-pod",
				Namespace: "training",
			},
		}

		gangInfo, err := discoverer.DiscoverPeers(context.Background(), podWithoutAnnotation)
		if err != nil {
			t.Fatalf("DiscoverPeers() error = %v", err)
		}

		if gangInfo != nil {
			t.Error("Expected nil GangInfo for pod without annotation")
		}
	})
}
