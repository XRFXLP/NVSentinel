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

package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nvidia/nvsentinel/commons/pkg/eventutil"
	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/node-drainer/pkg/initializer"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client/pkg/query"
	"github.com/nvidia/nvsentinel/store-client/pkg/utils"
)

const coldStartBatchSize = 1000

// dataStoreAdapter adapts client.DatabaseClient to queue.DataStore
type dataStoreAdapter struct {
	client.DatabaseClient
}

func (d *dataStoreAdapter) FindDocument(ctx context.Context, filter interface{},
	options *client.FindOneOptions) (client.SingleResult, error) {
	return d.FindOne(ctx, filter, options)
}

func (d *dataStoreAdapter) FindDocuments(ctx context.Context, filter interface{},
	options *client.FindOptions) (client.Cursor, error) {
	return d.Find(ctx, filter, options)
}

// coldStartQuery returns the query for events that may still require draining after a
// restart: in-progress drains and quarantined/already-quarantined events that were
// never processed. Note that this intentionally matches records regardless of whether
// their quarantine session has since ended; that filtering happens per-event so we can
// also tombstone stale records (see handleColdStart).
func coldStartQuery() *query.Builder {
	return query.New().Build(
		query.Or(
			// Events that were in-progress
			query.Eq("healtheventstatus.userpodsevictionstatus.status", string(model.StatusInProgress)),

			// Quarantined events that haven't been processed yet
			query.And(
				query.Eq("healtheventstatus.nodequarantined", string(model.Quarantined)),
				query.In("healtheventstatus.userpodsevictionstatus.status", []interface{}{"", string(model.StatusNotStarted)}),
			),

			// AlreadyQuarantined events that haven't been processed yet
			query.And(
				query.Eq("healtheventstatus.nodequarantined", string(model.AlreadyQuarantined)),
				query.In("healtheventstatus.userpodsevictionstatus.status", []interface{}{"", string(model.StatusNotStarted)}),
			),
		),
	)
}

// handleColdStart re-processes events that were in-progress or quarantined during a restart.
// Events are fetched in bounded batches via FindHealthEventsByQueryBatched to prevent
// unbounded memory usage. All matching events are loaded (not just latest per node)
// because a single node can have multiple concurrent partial drains.
//
// Cold start deliberately does NOT trust the datastore match alone. Node-drainer's
// live cancellation state (cancelledNodes) is in-memory and is lost on restart, so a
// quarantine record with an empty drain status can linger even after fault-quarantine
// has already unquarantined the node. Replaying those records marks a healthy,
// uncordoned node drain-succeeded, which fault-remediation then turns into an orphaned
// remediation-failed label. To avoid that, each quarantine record is checked against
// later UnQuarantined/Cancelled events for the same node; stale records are tombstoned
// instead of re-queued.
func handleColdStart(ctx context.Context, components *initializer.Components) error {
	slog.InfoContext(ctx, "Querying for events requiring processing")

	q := coldStartQuery()

	healthStore := components.DataStore.HealthEventStore()
	dbAdapter := &dataStoreAdapter{DatabaseClient: components.DatabaseClient}
	resolver := newQuarantineSessionResolver(healthStore)

	err := healthStore.FindHealthEventsByQueryBatched(ctx, q, coldStartBatchSize,
		func(batch []datastore.HealthEventWithStatus) error {
			slog.Info("Processing cold start batch", "count", len(batch))

			for i := range batch {
				processColdStartEvent(ctx, components, healthStore, dbAdapter, resolver, batch[i])
			}

			return nil
		})
	if err != nil {
		return fmt.Errorf("failed to process cold start events: %w", err)
	}

	slog.InfoContext(ctx, "Cold start processing completed")

	return nil
}

// processColdStartEvent evaluates a single cold-start candidate: it either tombstones a
// stale quarantine record whose session already ended, or re-queues the event for the
// normal drain pipeline.
func processColdStartEvent(
	ctx context.Context,
	components *initializer.Components,
	healthStore datastore.HealthEventStore,
	dbAdapter *dataStoreAdapter,
	resolver *quarantineSessionResolver,
	he datastore.HealthEventWithStatus,
) {
	parsed, nodeName, documentID, ok := parseColdStartEvent(ctx, he.RawEvent)
	if !ok {
		return
	}

	if skipStaleColdStartEvent(ctx, resolver, dbAdapter, parsed, nodeName, documentID, he.CreatedAt) {
		return
	}

	if enqueueErr := components.QueueManager.EnqueueEventGeneric(
		ctx, nodeName, he.RawEvent, dbAdapter, healthStore, documentID); enqueueErr != nil {
		slog.Error("Failed to enqueue cold start event", "error", enqueueErr, "nodeName", nodeName)
	} else {
		slog.InfoContext(ctx, "Re-queued event from cold start", "nodeName", nodeName)
	}
}

// parseColdStartEvent validates and extracts the fields needed to process a cold-start
// candidate. It returns ok=false (after logging) when the event is malformed and should
// be skipped.
func parseColdStartEvent(
	ctx context.Context, event datastore.Event,
) (parsed model.HealthEventWithStatus, nodeName string, documentID interface{}, ok bool) {
	if len(event) == 0 {
		slog.ErrorContext(ctx, "RawEvent is empty, skipping cold start event")

		return parsed, "", nil, false
	}

	parsed, err := eventutil.ParseHealthEventFromEvent(event)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to parse health event from cold start event", "error", err)

		return parsed, "", nil, false
	}

	if parsed.HealthEvent == nil {
		slog.ErrorContext(ctx, "Health event is nil in cold start event")

		return parsed, "", nil, false
	}

	nodeName = parsed.HealthEvent.GetNodeName()
	if nodeName == "" {
		slog.ErrorContext(ctx, "Node name is empty in cold start event")

		return parsed, "", nil, false
	}

	documentID, err = utils.ExtractDocumentIDNative(event)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to extract document ID from cold start event", "error", err)

		return parsed, "", nil, false
	}

	return parsed, nodeName, documentID, true
}

// skipStaleColdStartEvent reports whether a quarantine record should be skipped because
// its quarantine session has already ended. When it decides to skip, it also tombstones
// the record so future restarts do not replay it. It returns false (do not skip) for
// non-quarantine records and when session state cannot be determined, so genuine drains
// are never silently dropped.
func skipStaleColdStartEvent(
	ctx context.Context,
	resolver *quarantineSessionResolver,
	dbAdapter *dataStoreAdapter,
	parsed model.HealthEventWithStatus,
	nodeName string,
	documentID interface{},
	eventCreatedAt time.Time,
) bool {
	// Only quarantine records can be orphaned by a lost in-memory cancellation.
	// UnQuarantined/Cancelled records must always be re-queued so they run to
	// completion and let fault-remediation clean up node state.
	if !isActiveQuarantineStatus(parsed.HealthEventStatus.NodeQuarantined) {
		return false
	}

	ended, err := resolver.quarantineSessionEnded(ctx, nodeName, eventCreatedAt)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to determine quarantine session state, re-queueing event",
			"error", err, "nodeName", nodeName)

		return false
	}

	if !ended {
		return false
	}

	slog.InfoContext(ctx, "Skipping stale cold start event from an ended quarantine session",
		"nodeName", nodeName, "eventCreatedAt", eventCreatedAt)

	if markErr := tombstoneStaleQuarantineEvent(ctx, dbAdapter, documentID); markErr != nil {
		slog.ErrorContext(ctx, "Failed to tombstone stale cold start event",
			"error", markErr, "nodeName", nodeName)
	}

	return true
}

// isActiveQuarantineStatus reports whether the node-quarantined status represents an
// active quarantine (as opposed to a session-ending UnQuarantined/Cancelled marker).
func isActiveQuarantineStatus(status string) bool {
	return status == string(model.Quarantined) || status == string(model.AlreadyQuarantined)
}

// tombstoneStaleQuarantineEvent marks a stale quarantine record's drain status as
// Cancelled. This removes it from future cold-start queries and, because its
// nodequarantined stays Quarantined/AlreadyQuarantined with a non-drained status, it
// does not match fault-remediation's change-stream pipeline, so no remediation label
// is applied.
func tombstoneStaleQuarantineEvent(ctx context.Context, dbClient client.DatabaseClient, documentID interface{}) error {
	filter := map[string]any{"_id": documentID}
	update := map[string]any{
		"$set": map[string]any{
			"healtheventstatus.userpodsevictionstatus.status": string(model.Cancelled),
		},
	}

	if _, err := dbClient.UpdateDocument(ctx, filter, update); err != nil {
		return fmt.Errorf("failed to tombstone stale quarantine event %v: %w", documentID, err)
	}

	return nil
}

// sessionEndFinder is the subset of datastore.HealthEventStore needed to look up
// quarantine session-ending events. It exists to keep quarantineSessionResolver testable.
type sessionEndFinder interface {
	FindHealthEventsByQuery(ctx context.Context, builder datastore.QueryBuilder) ([]datastore.HealthEventWithStatus, error)
}

// quarantineSessionResolver answers, per node, whether a quarantine record predates a
// later UnQuarantined/Cancelled event (i.e. its quarantine session has already ended).
// Results are cached per node because a cold-start batch commonly contains multiple
// records for the same node.
type quarantineSessionResolver struct {
	finder sessionEndFinder
	cache  map[string]sessionEndInfo
}

// sessionEndInfo captures the most recent quarantine session end observed for a node.
type sessionEndInfo struct {
	latest time.Time
	exists bool
}

func newQuarantineSessionResolver(finder sessionEndFinder) *quarantineSessionResolver {
	return &quarantineSessionResolver{
		finder: finder,
		cache:  make(map[string]sessionEndInfo),
	}
}

// quarantineSessionEnded reports whether an UnQuarantined/Cancelled event exists for the
// node that is strictly newer than eventCreatedAt. When the candidate has no usable
// timestamp, it returns false so the caller re-queues rather than dropping the event.
func (r *quarantineSessionResolver) quarantineSessionEnded(
	ctx context.Context, nodeName string, eventCreatedAt time.Time,
) (bool, error) {
	if eventCreatedAt.IsZero() {
		return false, nil
	}

	info, ok := r.cache[nodeName]
	if !ok {
		var err error

		info, err = r.lookupLatestSessionEnd(ctx, nodeName)
		if err != nil {
			return false, err
		}

		r.cache[nodeName] = info
	}

	if !info.exists {
		return false, nil
	}

	return info.latest.After(eventCreatedAt), nil
}

// lookupLatestSessionEnd finds the newest UnQuarantined/Cancelled event for a node.
func (r *quarantineSessionResolver) lookupLatestSessionEnd(
	ctx context.Context, nodeName string,
) (sessionEndInfo, error) {
	q := query.New().Build(
		query.And(
			query.Eq("healthevent.nodename", nodeName),
			query.In("healtheventstatus.nodequarantined",
				[]interface{}{string(model.UnQuarantined), string(model.Cancelled)}),
		),
	)

	events, err := r.finder.FindHealthEventsByQuery(ctx, q)
	if err != nil {
		return sessionEndInfo{}, fmt.Errorf("failed to look up quarantine session end for node %s: %w", nodeName, err)
	}

	var info sessionEndInfo

	for i := range events {
		created := events[i].CreatedAt
		if created.IsZero() {
			continue
		}

		if !info.exists || created.After(info.latest) {
			info = sessionEndInfo{latest: created, exists: true}
		}
	}

	return info, nil
}
