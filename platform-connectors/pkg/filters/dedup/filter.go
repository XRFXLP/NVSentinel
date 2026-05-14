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

package dedup

import (
	"context"
	"log/slog"
	"time"

	commondedup "github.com/nvidia/nvsentinel/commons/pkg/dedup"
	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

// Name is the pipeline registry name for the deduplication filter.
const Name = "Deduplicator"

// Deduplicator suppresses repeated health events within a tracker burst window.
type Deduplicator struct {
	tracker *commondedup.Tracker
	skip    map[string]bool
	cancel  context.CancelFunc
}

// NewDeduplicator creates a filter backed by tracker and a check-name skip list.
func NewDeduplicator(
	tracker *commondedup.Tracker,
	skipChecks []string,
	cancel ...context.CancelFunc,
) *Deduplicator {
	skip := make(map[string]bool, len(skipChecks))
	for _, check := range skipChecks {
		skip[check] = true
	}

	d := &Deduplicator{
		tracker: tracker,
		skip:    skip,
	}
	if len(cancel) > 0 {
		d.cancel = cancel[0]
	}

	return d
}

// Close stops the background eviction loop, if this filter owns one.
func (d *Deduplicator) Close() error {
	if d.cancel != nil {
		d.cancel()
	}

	return nil
}

// Name returns the pipeline stage name.
func (d *Deduplicator) Name() string {
	return Name
}

// Filter returns false for duplicate events and true for events that should continue downstream.
func (d *Deduplicator) Filter(ctx context.Context, event *pb.HealthEvent) (bool, error) {
	if d.skip[event.GetCheckName()] {
		return true, nil
	}

	clearedUnhealthy := false
	if event.GetIsHealthy() {
		clearedUnhealthy = d.tracker.ClearUnhealthyCounterpart(event)
	}

	if !clearedUnhealthy && d.tracker.IsDuplicate(event) {
		dedupSuppressedCounter.WithLabelValues(
			event.GetCheckName(),
			event.GetNodeName(),
			errCodeLabel(event),
		).Inc()
		slog.InfoContext(ctx, "Health event suppressed by deduplication",
			"node", event.GetNodeName(),
			"check", event.GetCheckName(),
			"err_code", errCodeLabel(event))

		return false, nil
	}

	d.tracker.Mark(event)

	return true, nil
}

func startEvictExpired(ctx context.Context, tracker *commondedup.Tracker, interval time.Duration) {
	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tracker.EvictExpired()
			}
		}
	}()
}
