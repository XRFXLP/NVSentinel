// Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/fault-remediation/pkg/events"
	"github.com/nvidia/nvsentinel/fault-remediation/pkg/metrics"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
)

type reconcileRequest struct {
	event      *datastore.EventWithToken
	documentID string
}

type eventReference struct {
	documentID  string
	resumeToken []byte
}

type controllerReconciler struct {
	reconciler *FaultRemediationReconciler
}

func (c *controllerReconciler) Reconcile(
	ctx context.Context,
	request reconcileRequest,
) (ctrl.Result, error) {
	if request.event != nil {
		return c.reconciler.Reconcile(ctx, request.event)
	}

	return c.reconcileColdStartEvent(ctx, request.documentID)
}

func (c *controllerReconciler) reconcileColdStartEvent(
	ctx context.Context,
	documentID string,
) (result ctrl.Result, reconcileErr error) {
	if documentID == "" {
		return ctrl.Result{}, errors.New("cold-start reconcile request has no document ID")
	}

	if c.reconciler.coldStartReader == nil {
		return ctrl.Result{}, errors.New("health event store does not support cold-start reads")
	}

	healthEvent, err := c.reconciler.coldStartReader.FindHealthEventByID(ctx, documentID)
	if err != nil {
		metrics.ProcessingErrors.WithLabelValues("cold_start_fetch_error", "unknown").Inc()
		slog.ErrorContext(ctx, "Failed to fetch typed cold-start health event",
			"eventID", documentID,
			"error", err)

		return ctrl.Result{}, fmt.Errorf("fetch cold-start health event %s: %w", documentID, err)
	}

	if healthEvent == nil {
		metrics.ProcessingErrors.WithLabelValues("cold_start_event_unavailable", "unknown").Inc()
		slog.WarnContext(ctx, "Skipping deleted cold-start health event", "eventID", documentID)

		return ctrl.Result{}, nil
	}

	if healthEvent.HealthEvent == nil || healthEvent.HealthEventStatus == nil {
		metrics.ProcessingErrors.WithLabelValues("cold_start_event_unavailable", "unknown").Inc()
		slog.WarnContext(ctx, "Skipping invalid cold-start health event", "eventID", documentID)

		return ctrl.Result{}, nil
	}

	if healthEvent.HealthEventStatus.NodeQuarantined == "" {
		healthEvent.HealthEventStatus.NodeQuarantined = string(model.StatusNotStarted)
	}

	start := time.Now()

	slog.InfoContext(ctx, "Reconciling cold-start event", "eventID", documentID)

	defer func() {
		metrics.EventHandlingDuration.Observe(time.Since(start).Seconds())
	}()

	metrics.TotalEventsReceived.Inc()

	return c.reconciler.reconcileHealthEvent(ctx, &events.HealthEventDoc{
		ID:                    documentID,
		HealthEventWithStatus: *healthEvent,
	}, eventReference{documentID: documentID})
}
