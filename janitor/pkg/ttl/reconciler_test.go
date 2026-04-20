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

package ttl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	janitorv1alpha1 "github.com/nvidia/nvsentinel/janitor/api/v1alpha1"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	require.NoError(t, janitorv1alpha1.AddToScheme(s))

	return s
}

func newFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(objs...).
		Build()
}

func TestReconciler_AppliesDefaultTTLOnFirstReconcile(t *testing.T) {
	rn := newRebootNode("foo")
	c := newFakeClient(t, rn)
	clk := newClock()

	r := NewReconciler[*janitorv1alpha1.RebootNode](c,
		WithDefaultTTL[*janitorv1alpha1.RebootNode](14*24*time.Hour),
		WithClock[*janitorv1alpha1.RebootNode](clk),
	)

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "foo"},
	})
	require.NoError(t, err)
	assert.Greater(t, res.RequeueAfter, time.Duration(0))

	var got janitorv1alpha1.RebootNode
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "foo"}, &got))
	assert.Equal(t, "336h0m0s", got.Annotations[TTLAnnotation])
	assert.NotEmpty(t, got.Annotations[ExpiryAnnotation])
}

func TestReconciler_DeletesExpiredResource(t *testing.T) {
	clk := newClock()
	rn := newRebootNode("foo")
	rn.Annotations = map[string]string{
		TTLAnnotation:    "1h",
		ExpiryAnnotation: clk.Now().Add(-5 * time.Minute).Format(time.RFC3339),
	}

	c := newFakeClient(t, rn)

	var deletedKinds []string

	r := NewReconciler[*janitorv1alpha1.RebootNode](c,
		WithClock[*janitorv1alpha1.RebootNode](clk),
		WithMetrics[*janitorv1alpha1.RebootNode](func(kind string) {
			deletedKinds = append(deletedKinds, kind)
		}),
	)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "foo"},
	})
	require.NoError(t, err)

	var got janitorv1alpha1.RebootNode
	err = c.Get(context.Background(), types.NamespacedName{Name: "foo"}, &got)
	assert.True(t, apierrors.IsNotFound(err), "expected CR to be deleted, got err=%v", err)
	assert.Equal(t, []string{"RebootNode"}, deletedKinds)
}

func TestReconciler_IgnoresNotFound(t *testing.T) {
	c := newFakeClient(t) // no objects

	r := NewReconciler[*janitorv1alpha1.RebootNode](c,
		WithDefaultTTL[*janitorv1alpha1.RebootNode](time.Hour),
		WithClock[*janitorv1alpha1.RebootNode](newClock()),
	)

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gone"},
	})

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)
}

func TestReconciler_PreserveAnnotationBlocksDeletion(t *testing.T) {
	clk := newClock()
	rn := newRebootNode("foo")
	rn.Annotations = map[string]string{
		TTLAnnotation:      "1h",
		ExpiryAnnotation:   clk.Now().Add(-1 * time.Hour).Format(time.RFC3339),
		PreserveAnnotation: "true",
	}

	c := newFakeClient(t, rn)
	r := NewReconciler[*janitorv1alpha1.RebootNode](c,
		WithClock[*janitorv1alpha1.RebootNode](clk),
	)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "foo"},
	})
	require.NoError(t, err)

	var got janitorv1alpha1.RebootNode
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "foo"}, &got))
	assert.Equal(t, "true", got.Annotations[PreserveAnnotation])
}

func TestReconciler_IdempotentWhenNotYetExpired(t *testing.T) {
	rn := newRebootNode("foo")
	rn.Annotations = map[string]string{TTLAnnotation: "1h"}
	c := newFakeClient(t, rn)
	clk := newClock()

	r := NewReconciler[*janitorv1alpha1.RebootNode](c,
		WithClock[*janitorv1alpha1.RebootNode](clk),
	)

	// First reconcile: writes expiry, returns requeue.
	res1, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "foo"},
	})
	require.NoError(t, err)
	require.Greater(t, res1.RequeueAfter, time.Duration(0))

	var afterFirst janitorv1alpha1.RebootNode
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "foo"}, &afterFirst))
	firstExpiry := afterFirst.Annotations[ExpiryAnnotation]
	firstRV := afterFirst.ResourceVersion

	// Second reconcile: no changes, same expiry, still requeued.
	res2, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "foo"},
	})
	require.NoError(t, err)
	require.Greater(t, res2.RequeueAfter, time.Duration(0))

	var afterSecond janitorv1alpha1.RebootNode
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "foo"}, &afterSecond))
	assert.Equal(t, firstExpiry, afterSecond.Annotations[ExpiryAnnotation])
	assert.Equal(t, firstRV, afterSecond.ResourceVersion, "expected no Update on second reconcile")
}

func TestReconciler_IgnoresObjectBeingDeleted(t *testing.T) {
	clk := newClock()
	deletionTime := metav1.NewTime(clk.Now())
	rn := newRebootNode("foo")
	rn.DeletionTimestamp = &deletionTime
	rn.Finalizers = []string{"test"} // required to make the fake client accept DeletionTimestamp
	rn.Annotations = map[string]string{
		TTLAnnotation:    "1h",
		ExpiryAnnotation: clk.Now().Add(-1 * time.Hour).Format(time.RFC3339),
	}

	c := newFakeClient(t, rn)

	var deletedKinds []string

	r := NewReconciler[*janitorv1alpha1.RebootNode](c,
		WithClock[*janitorv1alpha1.RebootNode](clk),
		WithMetrics[*janitorv1alpha1.RebootNode](func(kind string) {
			deletedKinds = append(deletedKinds, kind)
		}),
	)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "foo"},
	})
	require.NoError(t, err)
	assert.Empty(t, deletedKinds, "should not trigger delete on an object already being deleted")
}
