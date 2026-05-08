// Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package healthpub

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

// fakePCClient is a minimal stand-in for pb.PlatformConnectorClient.
// Each test wires its own behaviour via responseFn (call number is
// 1-based) and the publisher invokes it via the ordinary gRPC client
// interface.
type fakePCClient struct {
	calls      atomic.Int64
	responseFn func(call int) error
}

func (f *fakePCClient) HealthEventOccurredV1(
	_ context.Context, _ *pb.HealthEvents, _ ...grpc.CallOption,
) (*emptypb.Empty, error) {
	n := int(f.calls.Add(1))
	if f.responseFn != nil {
		if err := f.responseFn(n); err != nil {
			return nil, err
		}
	}

	return &emptypb.Empty{}, nil
}

func sampleEvents() *pb.HealthEvents {
	return &pb.HealthEvents{
		Version: 1,
		Events: []*pb.HealthEvent{{
			Version:            1,
			Agent:              "test",
			CheckName:          "TestCheck",
			ComponentClass:     "Test",
			GeneratedTimestamp: timestamppb.New(time.Unix(0, 0)),
			NodeName:           "test-node",
		}},
	}
}

// fastRetryOpt configures a publisher whose retry budget is small
// enough that "all retries exhausted" tests complete quickly without
// changing the semantics under test.
func fastRetryOpt() Option {
	return WithRetryPolicy(3, 5*time.Millisecond, 1.0, 0)
}

// touchSocket creates an empty file to simulate a present Unix socket.
// We don't bind a real listener; the publisher only stat()s the path.
func touchSocket(t *testing.T, path string) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// TestPublish_SkipsWhenSocketMissing reproduces the dominant production
// scenario from the w-0366 incident: platform-connector restart removes
// the socket file, and the producer must skip rather than queue the
// event with a stale GeneratedTimestamp.
func TestPublish_SkipsWhenSocketMissing(t *testing.T) {
	tmp := t.TempDir()
	target := "unix://" + filepath.Join(tmp, "nvsentinel.sock") // path does NOT exist

	monitor := "test-skip-when-missing"

	skippedBefore := testutil.ToFloat64(sendsSkippedPCUnavailable.WithLabelValues(monitor))
	successBefore := testutil.ToFloat64(sendsSuccess.WithLabelValues(monitor))

	fc := &fakePCClient{}

	p := New(fc, target, monitor, fastRetryOpt())

	start := time.Now()
	err := p.Publish(context.Background(), sampleEvents())
	elapsed := time.Since(start)

	require.ErrorIs(t, err, ErrPlatformConnectorUnavailable,
		"expected ErrPlatformConnectorUnavailable when socket file is absent")
	assert.Equal(t, int64(0), fc.calls.Load(),
		"gRPC must NOT be invoked when socket is missing — no buffering, no retries")
	assert.Less(t, elapsed, 100*time.Millisecond,
		"skip must be fast; if elapsed approaches the retry budget the gate isn't firing")

	skippedAfter := testutil.ToFloat64(sendsSkippedPCUnavailable.WithLabelValues(monitor))
	successAfter := testutil.ToFloat64(sendsSuccess.WithLabelValues(monitor))
	assert.Equal(t, skippedBefore+1, skippedAfter,
		"skip counter must increment exactly once per skipped Publish")
	assert.Equal(t, successBefore, successAfter,
		"success counter must not move when the call was skipped")
}

// TestPublish_SuccessWhenSocketPresent covers the happy path: socket
// is there, gRPC accepts on first attempt, no skip recorded.
func TestPublish_SuccessWhenSocketPresent(t *testing.T) {
	tmp := t.TempDir()
	socket := filepath.Join(tmp, "nvsentinel.sock")
	touchSocket(t, socket)

	monitor := "test-success-when-present"

	skippedBefore := testutil.ToFloat64(sendsSkippedPCUnavailable.WithLabelValues(monitor))
	successBefore := testutil.ToFloat64(sendsSuccess.WithLabelValues(monitor))

	fc := &fakePCClient{}

	p := New(fc, "unix://"+socket, monitor)

	require.NoError(t, p.Publish(context.Background(), sampleEvents()))
	assert.Equal(t, int64(1), fc.calls.Load(),
		"gRPC must be invoked exactly once on a clean send")

	skippedAfter := testutil.ToFloat64(sendsSkippedPCUnavailable.WithLabelValues(monitor))
	successAfter := testutil.ToFloat64(sendsSuccess.WithLabelValues(monitor))
	assert.Equal(t, skippedBefore, skippedAfter,
		"skip counter must NOT move on a successful send")
	assert.Equal(t, successBefore+1, successAfter,
		"success counter must increment on a successful send")
}

// TestPublish_RetriesOnTransientErrorThenSucceeds verifies that genuine
// transient gRPC errors (Unavailable returned despite the socket file
// being present) still benefit from the retry loop.
func TestPublish_RetriesOnTransientErrorThenSucceeds(t *testing.T) {
	tmp := t.TempDir()
	socket := filepath.Join(tmp, "nvsentinel.sock")
	touchSocket(t, socket)

	monitor := "test-retry-transient"

	fc := &fakePCClient{
		responseFn: func(call int) error {
			if call < 2 {
				return status.Error(codes.Unavailable, "transient")
			}

			return nil
		},
	}

	p := New(fc, "unix://"+socket, monitor, fastRetryOpt())

	require.NoError(t, p.Publish(context.Background(), sampleEvents()))
	assert.Equal(t, int64(2), fc.calls.Load(),
		"first call should fail Unavailable, second should succeed")
}

// TestPublish_AbortsMidRetryWhenSocketDisappears covers the rare but
// real case where platform-connector exits between the entry probe and
// a subsequent retry attempt. We must detect the disappearance and stop
// burning retries against a moving target.
func TestPublish_AbortsMidRetryWhenSocketDisappears(t *testing.T) {
	tmp := t.TempDir()
	socket := filepath.Join(tmp, "nvsentinel.sock")
	touchSocket(t, socket)

	monitor := "test-abort-mid-retry"

	fc := &fakePCClient{
		responseFn: func(call int) error {
			// First attempt: fail transiently AND remove the socket
			// file, so the next iteration's pre-call probe sees it
			// gone.
			if call == 1 {
				_ = os.Remove(socket)
				return status.Error(codes.Unavailable, "transient")
			}

			return nil
		},
	}

	p := New(fc, "unix://"+socket, monitor,
		WithRetryPolicy(5, 5*time.Millisecond, 1.0, 0))

	skippedBefore := testutil.ToFloat64(sendsSkippedPCUnavailable.WithLabelValues(monitor))

	err := p.Publish(context.Background(), sampleEvents())
	require.ErrorIs(t, err, ErrPlatformConnectorUnavailable)
	assert.Equal(t, int64(1), fc.calls.Load(),
		"gRPC must be invoked exactly once before the in-loop probe catches the disappearance")

	skippedAfter := testutil.ToFloat64(sendsSkippedPCUnavailable.WithLabelValues(monitor))
	assert.Equal(t, skippedBefore+1, skippedAfter,
		"in-loop disappearance must charge the skip counter (not the error counter)")
}

// TestPublish_NonRetryableErrorReturnsImmediately verifies we don't
// burn the retry budget on permanent errors like InvalidArgument.
func TestPublish_NonRetryableErrorReturnsImmediately(t *testing.T) {
	tmp := t.TempDir()
	socket := filepath.Join(tmp, "nvsentinel.sock")
	touchSocket(t, socket)

	monitor := "test-non-retryable"

	fc := &fakePCClient{
		responseFn: func(_ int) error {
			return status.Error(codes.InvalidArgument, "bad request")
		},
	}

	p := New(fc, "unix://"+socket, monitor, fastRetryOpt())

	err := p.Publish(context.Background(), sampleEvents())
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrPlatformConnectorUnavailable)
	assert.Equal(t, int64(1), fc.calls.Load(),
		"non-retryable errors must NOT trigger retries")
}

// TestUnixSocketPathFromTarget covers the URI variants gRPC accepts for
// Unix-socket targets so each monitor's preferred form actually triggers
// the gate. csp-health-monitor uses "unix:%s" (single colon), the others
// use "unix:///path" (double slash).
func TestUnixSocketPathFromTarget(t *testing.T) {
	cases := []struct {
		target string
		want   string
	}{
		{"unix:///var/run/nvsentinel.sock", "/var/run/nvsentinel.sock"},
		{"unix:/var/run/nvsentinel.sock", "/var/run/nvsentinel.sock"},
		{"unix:relative/path", ""}, // relative paths intentionally skipped
		{"127.0.0.1:5555", ""},
		{"dns:///host:5555", ""},
		{"", ""},
	}
	for _, tc := range cases {
		t.Run(tc.target, func(t *testing.T) {
			assert.Equal(t, tc.want, unixSocketPathFromTarget(tc.target))
		})
	}
}

// TestPublish_TCPTargetSkipsGate verifies that callers using a non-unix
// gRPC target (TCP, etc.) bypass the socket-existence probe entirely
// and fall straight through to the retry loop.
func TestPublish_TCPTargetSkipsGate(t *testing.T) {
	monitor := "test-tcp-target"

	fc := &fakePCClient{}

	p := New(fc, "127.0.0.1:5555", monitor)

	assert.Empty(t, p.SocketPath(),
		"non-unix:// targets must not derive a socketPath")
	require.NoError(t, p.Publish(context.Background(), sampleEvents()))
	assert.Equal(t, int64(1), fc.calls.Load(),
		"TCP target must reach the gRPC call without gating on a file")
}

// TestPublish_NilOrEmptyEventsIsNoOp asserts the no-op contract for
// empty batches.
func TestPublish_NilOrEmptyEventsIsNoOp(t *testing.T) {
	monitor := "test-noop"

	fc := &fakePCClient{}

	p := New(fc, "unix:///nonexistent.sock", monitor)

	assert.NoError(t, p.Publish(context.Background(), nil),
		"nil HealthEvents must be a no-op error-free")
	assert.NoError(t, p.Publish(context.Background(), &pb.HealthEvents{}),
		"empty HealthEvents must be a no-op error-free")
	assert.Equal(t, int64(0), fc.calls.Load(),
		"empty input must short-circuit before any gRPC call")
}

// TestNew_EmptyMonitorPanics ensures library users get a loud failure
// for what would otherwise be a silent dashboarding bug (events
// skipped against an empty `monitor` label).
func TestNew_EmptyMonitorPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("New must panic on empty monitor name")
		}
	}()

	_ = New(&fakePCClient{}, "unix:///x", "")
}

// TestPublish_ContextCancellationStopsRetries verifies that a cancelled
// context causes the retry loop to abort, instead of exhausting the
// full retry budget against a long-running outage.
func TestPublish_ContextCancellationStopsRetries(t *testing.T) {
	tmp := t.TempDir()
	socket := filepath.Join(tmp, "nvsentinel.sock")
	touchSocket(t, socket)

	monitor := "test-ctx-cancel"

	fc := &fakePCClient{
		responseFn: func(_ int) error {
			return status.Error(codes.Unavailable, "transient")
		},
	}

	p := New(fc, "unix://"+socket, monitor,
		WithRetryPolicy(20, 50*time.Millisecond, 1.0, 0))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	err := p.Publish(ctx, sampleEvents())
	require.Error(t, err)
	// The error may be `context.DeadlineExceeded` itself, or
	// `wait.ErrWaitTimeout` — both indicate the loop terminated due to
	// context cancellation rather than retries succeeding.
	assert.True(t,
		errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, context.Canceled) ||
			err.Error() == "timed out waiting for the condition" ||
			err.Error() == "context deadline exceeded",
		"expected a context-related error, got %v", err)
	assert.Less(t, fc.calls.Load(), int64(20),
		"retries must stop when the context is cancelled, not exhaust the full budget")
}
