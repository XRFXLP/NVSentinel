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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	janitorv1alpha1 "github.com/nvidia/nvsentinel/janitor/api/v1alpha1"
)

// fakeClock is a deterministic Clock for tests.
type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time { return f.now }

func (f *fakeClock) advance(d time.Duration) { f.now = f.now.Add(d) }

// fixedNow is an arbitrary reference time used across tests.
var fixedNow = time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)

func newClock() *fakeClock { return &fakeClock{now: fixedNow} }

func newRebootNode(name string) *janitorv1alpha1.RebootNode {
	return &janitorv1alpha1.RebootNode{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
}

func TestProcess_BeingDeleted_ReturnsNoop(t *testing.T) {
	rn := newRebootNode("foo")
	deletionTime := metav1.NewTime(fixedNow)
	rn.DeletionTimestamp = &deletionTime

	result, err := Process(context.Background(), rn, time.Hour, newClock())

	require.NoError(t, err)
	assert.Equal(t, ActionNoop, result.Action)
	assert.False(t, result.Changed)
}

func TestProcess_PreserveAnnotation_ReturnsNoop(t *testing.T) {
	rn := newRebootNode("foo")
	rn.Annotations = map[string]string{PreserveAnnotation: "true"}

	result, err := Process(context.Background(), rn, time.Hour, newClock())

	require.NoError(t, err)
	assert.Equal(t, ActionNoop, result.Action)
	assert.False(t, result.Changed)
}

func TestProcess_NoTTLAndNoDefault_ReturnsNoop(t *testing.T) {
	rn := newRebootNode("foo")

	result, err := Process(context.Background(), rn, 0, newClock())

	require.NoError(t, err)
	assert.Equal(t, ActionNoop, result.Action)
	assert.False(t, result.Changed)
	assert.NotContains(t, rn.Annotations, TTLAnnotation)
}

func TestProcess_AppliesDefaultTTLWhenMissing(t *testing.T) {
	rn := newRebootNode("foo")
	clk := newClock()

	result, err := Process(context.Background(), rn, 14*24*time.Hour, clk)

	require.NoError(t, err)
	assert.Equal(t, ActionRequeue, result.Action)
	assert.True(t, result.Changed)
	assert.Equal(t, "336h0m0s", rn.Annotations[TTLAnnotation])
	assert.Equal(t, fixedNow.Add(14*24*time.Hour).Format(time.RFC3339), rn.Annotations[ExpiryAnnotation])
}

func TestProcess_ExistingTTLAnnotationNotOverwritten(t *testing.T) {
	rn := newRebootNode("foo")
	rn.Annotations = map[string]string{TTLAnnotation: "1h"}

	result, err := Process(context.Background(), rn, 336*time.Hour, newClock())

	require.NoError(t, err)
	assert.Equal(t, ActionRequeue, result.Action)
	assert.Equal(t, "1h", rn.Annotations[TTLAnnotation])
}

func TestProcess_InvalidTTLAnnotation_ReturnsNoop(t *testing.T) {
	rn := newRebootNode("foo")
	rn.Annotations = map[string]string{TTLAnnotation: "not-a-duration"}

	result, err := Process(context.Background(), rn, 0, newClock())

	require.NoError(t, err)
	assert.Equal(t, ActionNoop, result.Action)
}

func TestProcess_ZeroTTLAnnotation_ReturnsNoop(t *testing.T) {
	rn := newRebootNode("foo")
	rn.Annotations = map[string]string{TTLAnnotation: "0"}

	result, err := Process(context.Background(), rn, 0, newClock())

	require.NoError(t, err)
	assert.Equal(t, ActionNoop, result.Action)
}

func TestProcess_ExpiryPersistedAndReused(t *testing.T) {
	rn := newRebootNode("foo")
	clk := newClock()

	// First pass: expiry should be written.
	result, err := Process(context.Background(), rn, time.Hour, clk)
	require.NoError(t, err)
	require.Equal(t, ActionRequeue, result.Action)

	firstExpiry := rn.Annotations[ExpiryAnnotation]
	require.NotEmpty(t, firstExpiry)

	// Advance the clock by a bit (still within TTL) and reprocess.
	clk.advance(10 * time.Minute)

	result, err = Process(context.Background(), rn, time.Hour, clk)
	require.NoError(t, err)
	assert.Equal(t, ActionRequeue, result.Action)
	assert.False(t, result.Changed, "expiry should not be recomputed on subsequent calls")
	assert.Equal(t, firstExpiry, rn.Annotations[ExpiryAnnotation])
}

func TestProcess_InvalidExpiryAnnotation_Recomputed(t *testing.T) {
	rn := newRebootNode("foo")
	rn.Annotations = map[string]string{
		TTLAnnotation:    "1h",
		ExpiryAnnotation: "not-a-time",
	}

	result, err := Process(context.Background(), rn, 0, newClock())

	require.NoError(t, err)
	assert.Equal(t, ActionRequeue, result.Action)
	assert.True(t, result.Changed)
	assert.Equal(t, fixedNow.Add(time.Hour).Format(time.RFC3339), rn.Annotations[ExpiryAnnotation])
}

func TestProcess_ExpiredReturnsActionExpired(t *testing.T) {
	rn := newRebootNode("foo")
	clk := newClock()

	result, err := Process(context.Background(), rn, time.Hour, clk)
	require.NoError(t, err)
	require.Equal(t, ActionRequeue, result.Action)

	// Jump past the TTL.
	clk.advance(2 * time.Hour)

	result, err = Process(context.Background(), rn, time.Hour, clk)
	require.NoError(t, err)
	assert.Equal(t, ActionExpired, result.Action)
}

func TestProcess_ExpiryAtExactNow_ReturnsExpired(t *testing.T) {
	// Boundary: when clock.Now() == expiry, the CR should be deleted, not requeued.
	rn := newRebootNode("foo")
	clk := newClock()
	rn.Annotations = map[string]string{
		TTLAnnotation:    "1h",
		ExpiryAnnotation: fixedNow.Format(time.RFC3339),
	}

	result, err := Process(context.Background(), rn, 0, clk)

	require.NoError(t, err)
	assert.Equal(t, ActionExpired, result.Action)
}

func TestProcess_CapsRequeueAtMaxInterval(t *testing.T) {
	// A TTL of many days should still cause a requeue no later than maxRequeueInterval.
	rn := newRebootNode("foo")

	result, err := Process(context.Background(), rn, 30*24*time.Hour, newClock())

	require.NoError(t, err)
	require.Equal(t, ActionRequeue, result.Action)
	assert.Equal(t, maxRequeueInterval, result.RequeueAfter)
}

func TestProcess_InitializesNilAnnotationMap(t *testing.T) {
	rn := newRebootNode("foo")
	assert.Nil(t, rn.Annotations)

	result, err := Process(context.Background(), rn, time.Hour, newClock())

	require.NoError(t, err)
	assert.Equal(t, ActionRequeue, result.Action)
	assert.NotNil(t, rn.Annotations)
}

func TestKindFromType_RebootNode(t *testing.T) {
	assert.Equal(t, "RebootNode", kindFromType[*janitorv1alpha1.RebootNode]())
}

func TestKindFromType_ConfigMap(t *testing.T) {
	// Sanity-check the helper against a standard core/v1 type, to confirm
	// the extraction works beyond janitor CRDs.
	assert.Equal(t, "ConfigMap", kindFromType[*corev1.ConfigMap]())
}

func TestNewZeroRef_ReturnsNonNilPointer(t *testing.T) {
	got := newZeroRef[*janitorv1alpha1.RebootNode]()
	require.NotNil(t, got)
	assert.Empty(t, got.Name)
}
