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

// Package healthpub is the shared health-event publisher used by every
// nvsentinel health monitor. It exists to centralise the gRPC retry policy
// against the platform-connector and — most importantly — to gate every
// send on the platform-connector Unix socket actually being present.
//
// Background. Each monitor stamps `HealthEvent.GeneratedTimestamp` with
// `time.Now()` at the moment it decides a condition exists, then hands the
// event to a `wait.ExponentialBackoff` retry loop calling
// `HealthEventOccurredV1`. When platform-connector restarts on the local
// node (graceful redeploy, OOM, helm upgrade, …) its Unix socket file
// vanishes from disk for the duration of the outage. Without a producer-
// side gate, the retry loop holds the *original* timestamp through that
// outage and eventually delivers a stale event whose `GeneratedTimestamp`
// is many seconds behind wall-clock. fault-quarantine then attributes the
// gap to "time-to-cordon" even though it spent <100 ms cordoning. We
// observed multi-minute outages bake into the histogram this way (see the
// w-0366 incident logs and the corresponding ADR-039 context).
//
// Fix. Before every gRPC call we probe the socket file's existence. If
// absent, we skip the send entirely (no buffering, no cache mutation,
// counter incremented) and return ErrPlatformConnectorUnavailable so the
// caller can refrain from advancing its local "have I sent this state?"
// cache. The producer's natural poll cadence then re-evaluates the
// underlying condition next cycle and re-emits with a fresh
// `GeneratedTimestamp`. The metric observation is therefore bounded by
// the polling cadence regardless of how long platform-connector is down.
//
// platform-connector explicitly removes the socket on shutdown and on
// startup before binding (see `platform-connectors/main.go`'s
// `os.Remove(socket)` calls), so file-presence is a faithful proxy for
// "PC is up" on this node.
package healthpub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

// ErrPlatformConnectorUnavailable is returned by Publisher.Publish when
// the platform-connector Unix socket file is absent at send time. Callers
// should treat this distinctly from a generic send error: do NOT advance
// any local "have I sent this state?" cache, so the next poll naturally
// re-emits the same condition with a fresh `GeneratedTimestamp` rather
// than letting an older timestamp sit in a retry buffer.
var ErrPlatformConnectorUnavailable = errors.New("platform-connector unix socket missing; send skipped")

// gRPC accepts multiple URI forms for Unix-socket targets:
//
//   - "unix:///absolute/path"     (authority empty, path absolute) — used
//     by most monitors via `unix:///var/run/nvsentinel.sock`.
//   - "unix:/absolute/path"       (one slash) — used by csp-health-monitor
//     via `fmt.Sprintf("unix:%s", udsPath)` with an absolute udsPath.
//   - "unix:relative/path"        (relative) — uncommon but legal.
//
// We accept all three so the gate works regardless of which scheme form
// the caller used to dial the connection.
const (
	unixSchemeDoubleSlash = "unix://"
	unixSchemeSingleColon = "unix:"
)

// Defaults intentionally mirror the values previously baked into each
// monitor's local `sendWithRetry` so migration is behaviour-preserving for
// the happy path.
const (
	defaultMaxRetries     = 5
	defaultInitialBackoff = 2 * time.Second
	defaultBackoffFactor  = 1.5
	defaultBackoffJitter  = 0.1
)

// Publisher publishes health events to the platform-connector. It is safe
// for concurrent use by multiple goroutines: it holds no per-call mutable
// state.
type Publisher struct {
	client pb.PlatformConnectorClient

	// monitor is the value used as the `monitor` Prometheus label on the
	// shared counters in metrics.go. Pass the agent name (e.g.
	// "syslog-health-monitor"). Not used outside metrics.
	monitor string

	// socketPath is the filesystem path of the Unix socket, derived from
	// target by stripping a leading "unix://". Empty when target is not a
	// Unix-socket URI; in that case the existence gate is skipped and
	// only the gRPC retry path is exercised. We retain this field rather
	// than re-parsing target on every call.
	socketPath string

	maxRetries     int
	initialBackoff time.Duration
	backoffFactor  float64
	backoffJitter  float64
}

// Option configures a Publisher.
type Option func(*Publisher)

// WithRetryPolicy overrides the default exponential-backoff parameters.
//
// Defaults: maxRetries=5, initialBackoff=2s, factor=1.5, jitter=0.1.
//
// Note: with the pre-send gate in place, the retry loop is now exercised
// only for genuinely transient gRPC errors (e.g., platform-connector
// crashed mid-call after the file existed at probe time). Most monitors
// can leave the defaults alone.
func WithRetryPolicy(maxRetries int, initialBackoff time.Duration, factor, jitter float64) Option {
	return func(p *Publisher) {
		if maxRetries > 0 {
			p.maxRetries = maxRetries
		}

		if initialBackoff > 0 {
			p.initialBackoff = initialBackoff
		}

		if factor > 0 {
			p.backoffFactor = factor
		}

		if jitter >= 0 {
			p.backoffJitter = jitter
		}
	}
}

// New constructs a Publisher.
//
// client is a fully constructed gRPC client; the publisher does NOT dial.
//
// target is the gRPC target string the caller used when dialing
// (e.g. "unix:///var/run/nvsentinel.sock"). It is used only to derive the
// Unix-socket path for the existence gate. If target does not start with
// "unix://", the gate is skipped — TCP targets fall straight through to
// the retry loop with no behaviour change.
//
// monitor is the agent name used as a Prometheus label. Pass the same
// constant the monitor uses for `HealthEvent.Agent` so dashboards line
// up. Required (panics on empty).
func New(client pb.PlatformConnectorClient, target, monitor string, opts ...Option) *Publisher {
	if monitor == "" {
		panic("healthpub.New: monitor name must be non-empty")
	}

	p := &Publisher{
		client:         client,
		monitor:        monitor,
		maxRetries:     defaultMaxRetries,
		initialBackoff: defaultInitialBackoff,
		backoffFactor:  defaultBackoffFactor,
		backoffJitter:  defaultBackoffJitter,
	}

	p.socketPath = unixSocketPathFromTarget(target)

	for _, opt := range opts {
		opt(p)
	}

	return p
}

// SocketPath returns the resolved Unix-socket path, or empty string if
// the publisher's target was not a unix:// URI. Exposed primarily for
// tests; callers shouldn't need this in production code.
func (p *Publisher) SocketPath() string { return p.socketPath }

// Publish sends a batch of health events. Returns:
//
//   - nil on success
//   - ErrPlatformConnectorUnavailable if the unix socket is absent at
//     send time (skip; no buffering, no cache mutation, no retries
//     attempted). Callers must treat this case specially: leave any
//     local "have I sent this?" state untouched so the next poll cycle
//     re-emits with a fresh `GeneratedTimestamp`.
//   - any other error if the gRPC call fails after retries are
//     exhausted, or a non-retryable gRPC error is encountered.
//
// Publish is safe to call from multiple goroutines.
func (p *Publisher) Publish(ctx context.Context, events *pb.HealthEvents) error {
	if events == nil || len(events.GetEvents()) == 0 {
		return nil
	}

	// Pre-send gate. The cheapest possible check that catches the
	// dominant failure mode (platform-connector pod down on this node).
	if p.socketPath != "" {
		if _, err := os.Stat(p.socketPath); err != nil {
			sendsSkippedPCUnavailable.WithLabelValues(p.monitor).Inc()
			slog.Warn("Platform-connector socket missing; skipping send. "+
				"Next poll will re-evaluate and re-stamp the event.",
				"monitor", p.monitor,
				"socket", p.socketPath,
				"stat_error", err,
			)

			return ErrPlatformConnectorUnavailable
		}
	}

	backoff := wait.Backoff{
		Steps:    p.maxRetries,
		Duration: p.initialBackoff,
		Factor:   p.backoffFactor,
		Jitter:   p.backoffJitter,
	}

	var lastErr error

	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		// Re-check the socket between retries. If platform-connector
		// went away after the initial probe (e.g. crashed during a
		// retry sleep), abort early instead of burning the rest of the
		// budget against a moving target. The next poll will retry
		// with a fresh timestamp.
		if p.socketPath != "" {
			if _, statErr := os.Stat(p.socketPath); statErr != nil {
				sendsSkippedPCUnavailable.WithLabelValues(p.monitor).Inc()
				slog.Warn("Platform-connector socket disappeared mid-retry; aborting send.",
					"monitor", p.monitor,
					"socket", p.socketPath,
					"stat_error", statErr,
				)

				lastErr = ErrPlatformConnectorUnavailable

				return false, ErrPlatformConnectorUnavailable
			}
		}

		_, sendErr := p.client.HealthEventOccurredV1(ctx, events)
		if sendErr == nil {
			sendsSuccess.WithLabelValues(p.monitor).Inc()
			slog.Info("Successfully sent health events",
				"monitor", p.monitor,
				"count", len(events.GetEvents()))

			return true, nil
		}

		lastErr = sendErr

		if isRetryable(sendErr) {
			slog.Warn("Retryable error sending health events; will retry.",
				"monitor", p.monitor,
				"error", sendErr,
			)

			return false, nil
		}

		slog.Error("Non-retryable error sending health events.",
			"monitor", p.monitor,
			"error", sendErr,
		)

		return false, fmt.Errorf("non-retryable error sending health events: %w", sendErr)
	})

	if errors.Is(err, ErrPlatformConnectorUnavailable) || errors.Is(lastErr, ErrPlatformConnectorUnavailable) {
		// Distinct from a generic retry-exhausted error so callers can
		// branch on it via errors.Is.
		return ErrPlatformConnectorUnavailable
	}

	if err == nil {
		return nil
	}

	// Charge the error counter once, with a code label, to avoid
	// inflating the count by the retry budget.
	sendsError.WithLabelValues(p.monitor, errorCodeLabel(lastErr)).Inc()

	if lastErr != nil && !errors.Is(err, lastErr) {
		return fmt.Errorf("%w: last error: %w", err, lastErr)
	}

	return err
}

// isRetryable reports whether a gRPC error is transient and worth
// retrying. Mirrors the union of the per-monitor `isRetryable` helpers
// that previously lived in each publisher.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}

	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.Unavailable, codes.DeadlineExceeded:
			return true
		}
	}

	if errors.Is(err, io.EOF) {
		return true
	}

	msg := err.Error()
	if strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") {
		return true
	}

	return false
}

// unixSocketPathFromTarget extracts the absolute filesystem path from a
// gRPC unix-scheme target, returning "" for non-unix targets. The two
// scheme variants we honor are "unix://" (typical) and "unix:"
// (csp-health-monitor's `unix:%s` form). Relative paths under the
// "unix:" form are intentionally not supported by the gate — they would
// require resolving a working directory the publisher does not own; in
// that case we return "" and let the gate fall through.
func unixSocketPathFromTarget(target string) string {
	if rest, ok := strings.CutPrefix(target, unixSchemeDoubleSlash); ok {
		return rest
	}

	if rest, ok := strings.CutPrefix(target, unixSchemeSingleColon); ok {
		// Only treat as a Unix path if the remainder is absolute. A
		// relative path would resolve against a working directory the
		// publisher cannot reliably know — better to skip the gate
		// than guess.
		if strings.HasPrefix(rest, "/") {
			return rest
		}
	}

	return ""
}

// errorCodeLabel produces a low-cardinality label value for the
// sendsError counter. We prefer the gRPC status code (e.g.
// "Unavailable") over the raw error string so the counter has bounded
// cardinality in Prometheus.
func errorCodeLabel(err error) string {
	if err == nil {
		return ""
	}

	if s, ok := status.FromError(err); ok {
		return s.Code().String()
	}

	return "Unknown"
}
