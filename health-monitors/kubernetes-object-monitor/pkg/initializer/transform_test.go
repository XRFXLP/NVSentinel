package initializer

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// realisticNode mirrors a production GPU worker: ~180 labels, 38 conditions,
// 50 cached images and a large managedFields block.
func realisticNode() *unstructured.Unstructured {
	labels := map[string]any{}
	for i := 0; i < 180; i++ {
		labels[string(rune('a'+i%26))+string(rune('a'+i/26))+".nvidia.com/l"] = "0123456789012345678901234567890123456789012345678"
	}

	conds := []any{}
	for i := 0; i < 38; i++ {
		conds = append(conds, map[string]any{
			"type": "C" + string(rune('a'+i%26)), "status": "False",
			"lastHeartbeatTime": "2026-09-01T00:00:00Z", "lastTransitionTime": "2026-09-01T00:00:00Z",
			"reason": "SomeReason", "message": "some longer message describing the condition state",
		})
	}

	conds = append(conds, map[string]any{"type": "Ready", "status": "False"})

	images := []any{}
	for i := 0; i < 50; i++ {
		images = append(images, map[string]any{
			"names":     []any{"registry.example.com/some/fairly-long-image-name@sha256:deadbeef" + string(rune('a'+i%26))},
			"sizeBytes": int64(123456789),
		})
	}

	mf := []any{}
	for i := 0; i < 8; i++ {
		mf = append(mf, map[string]any{
			"manager": "kubelet", "operation": "Update", "apiVersion": "v1",
			"fieldsV1": map[string]any{"f:status": map[string]any{"f:conditions": map[string]any{}}},
			"blob":     string(make([]byte, 2000)),
		})
	}

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Node",
		"metadata": map[string]any{
			"name": "pb-860000", "uid": "abc", "resourceVersion": "12345",
			"labels": labels, "annotations": map[string]any{"pad": string(make([]byte, 9000))},
			"managedFields": mf,
		},
		"spec":   map[string]any{"taints": []any{map[string]any{"key": "k", "effect": "NoSchedule"}}},
		"status": map[string]any{"conditions": conds, "images": images, "allocatable": map[string]any{"cpu": "32"}},
	}}
}

func size(t *testing.T, u *unstructured.Unstructured) int {
	t.Helper()

	b, err := json.Marshal(u.Object)
	if err != nil {
		t.Fatal(err)
	}

	return len(b)
}

func TestPruningTransformKeepsPolicyFieldsAndShrinksObject(t *testing.T) {
	n := realisticNode()
	before := size(t, n)

	// what the NodeNotReady policy reads
	tf := pruningTransform([][]string{{"status", "conditions"}})

	out, err := tf(n)
	if err != nil {
		t.Fatal(err)
	}

	got := out.(*unstructured.Unstructured)
	after := size(t, got)

	// the predicate must still be answerable
	conds, found, err := unstructured.NestedSlice(got.Object, "status", "conditions")
	if err != nil || !found || len(conds) != 39 {
		t.Fatalf("conditions not retained: found=%v err=%v n=%d", found, err, len(conds))
	}

	if got.GetName() != "pb-860000" || got.GetUID() != "abc" || got.GetResourceVersion() != "12345" {
		t.Fatalf("informer-critical metadata lost: %v", got.Object["metadata"])
	}

	// the expensive fields must be gone
	for _, p := range [][]string{{"metadata", "managedFields"}, {"metadata", "labels"}, {"status", "images"}} {
		if _, found, _ := unstructured.NestedFieldNoCopy(got.Object, p...); found {
			t.Errorf("%v should have been pruned", p)
		}
	}

	t.Logf("wire bytes %d -> %d  (%.1f%% retained, %.1fx smaller)",
		before, after, float64(after)/float64(before)*100, float64(before)/float64(after))

	if after > before/3 {
		t.Errorf("expected at least a 3x reduction, got %d -> %d", before, after)
	}
}
