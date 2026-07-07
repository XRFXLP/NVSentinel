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

package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
)

func TestCleanupExpiredCancellationCutoffsLocked(t *testing.T) {
	now := time.Date(2026, time.July, 7, 0, 0, 0, 0, time.UTC)
	expired := now.Add(-cancellationCutoffTTL - time.Second)
	fresh := now.Add(-cancellationCutoffTTL + time.Second)

	r := &Reconciler{
		nodeEventsMap: map[string]eventStatusMap{
			"tracked-expired": {"event": model.StatusInProgress},
		},
		cancelledNodes: map[string]cancellationCutoff{
			"expired":         {createdAt: expired},
			"fresh":           {createdAt: fresh},
			"tracked-expired": {createdAt: expired},
		},
	}

	r.cleanupExpiredCancellationCutoffsLocked(now)

	if _, exists := r.cancelledNodes["expired"]; exists {
		t.Fatal("expired untracked cutoff should be removed")
	}

	if _, exists := r.cancelledNodes["fresh"]; !exists {
		t.Fatal("fresh cutoff should be retained")
	}

	if _, exists := r.cancelledNodes["tracked-expired"]; !exists {
		t.Fatal("expired cutoff with tracked events should be retained")
	}
}

func TestIsTimeoutEvictionCancelledUsesCutoff(t *testing.T) {
	cutoff := time.Date(2026, time.July, 7, 0, 0, 0, 0, time.UTC)
	r := &Reconciler{
		nodeEventsMap:  make(map[string]eventStatusMap),
		cancelledNodes: map[string]cancellationCutoff{"node-a": {createdAt: cutoff}},
	}

	if !r.isTimeoutEvictionCancelled(context.Background(), "old-event", "node-a", cutoff.Add(-time.Second)) {
		t.Fatal("pre-cutoff timeout eviction should be cancelled")
	}

	if r.isTimeoutEvictionCancelled(context.Background(), "fresh-event", "node-a", cutoff.Add(time.Second)) {
		t.Fatal("post-cutoff timeout eviction should not be cancelled")
	}
}
