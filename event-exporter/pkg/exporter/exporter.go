package exporter

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/config"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/metrics"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/sink"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/transformer"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"k8s.io/apimachinery/pkg/util/wait"
)

type HealthEventsExporter struct {
	cfg         *config.Config
	dbClient    client.DatabaseClient
	source      client.ChangeStreamWatcher
	transformer transformer.EventTransformer
	sink        sink.EventSink
}

func New(
	cfg *config.Config,
	dbClient client.DatabaseClient,
	source client.ChangeStreamWatcher,
	transformer transformer.EventTransformer,
	sink sink.EventSink,
) *HealthEventsExporter {
	return &HealthEventsExporter{
		cfg:         cfg,
		dbClient:    dbClient,
		source:      source,
		transformer: transformer,
		sink:        sink,
	}
}

func (e *HealthEventsExporter) Run(ctx context.Context) error {
	if e.cfg.Exporter.Backfill.Enabled {
		slog.Info("Backfill enabled, running backfill before streaming")

		if err := e.runBackfill(ctx); err != nil {
			slog.Error("Failed to run backfill", "error", err)
			return fmt.Errorf("backfill: %w", err)
		}
	}

	e.source.Start(ctx)

	return e.streamEvents(ctx)
}

func (e *HealthEventsExporter) runBackfill(ctx context.Context) error {
	startTime := time.Now().UTC()
	backfillStart := startTime.Add(-e.cfg.Exporter.Backfill.GetMaxAge())

	slog.Info("Starting backfill", "backfillStart", backfillStart, "maxAge", e.cfg.Exporter.Backfill.GetMaxAge())

	metrics.BackfillInProgress.Set(1)
	defer metrics.BackfillInProgress.Set(0)

	cursor, err := e.queryBackfillEvents(ctx, backfillStart, startTime)
	if err != nil {
		slog.Error("Failed to query backfill events", "error", err)
		return fmt.Errorf("query backfill events: %w", err)
	}
	defer cursor.Close(ctx)

	count, err := e.processBackfillCursor(ctx, cursor)
	if err != nil {
		slog.Error("Failed to process backfill cursor", "error", err)
		return fmt.Errorf("process backfill cursor: %w", err)
	}

	metrics.BackfillDuration.Observe(time.Since(startTime).Seconds())
	slog.Info("Backfill complete", "events", count)

	return nil
}

func (e *HealthEventsExporter) queryBackfillEvents(ctx context.Context, start, end time.Time) (client.Cursor, error) {
	filter := map[string]any{
		"createdAt": map[string]any{
			"$gte": start,
			"$lt":  end,
		},
	}

	limit := int64(e.cfg.Exporter.Backfill.MaxEvents)
	opts := &client.FindOptions{
		Limit: &limit,
	}

	cursor, err := e.dbClient.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}

	return cursor, nil
}

func (e *HealthEventsExporter) processBackfillCursor(ctx context.Context, cursor client.Cursor) (int, error) {
	rateLimiter := time.NewTicker(time.Second / time.Duration(e.cfg.Exporter.Backfill.RateLimit))
	defer rateLimiter.Stop()

	count := 0

	for cursor.Next(ctx) {
		if ctx.Err() != nil {
			return count, ctx.Err()
		}

		var healthEventWithStatus model.HealthEventWithStatus
		if err := cursor.Decode(&healthEventWithStatus); err != nil {
			slog.Warn("Failed to decode event", "error", err)
			continue
		}

		if healthEventWithStatus.HealthEvent == nil {
			slog.Debug("Skipping nil health event")
			continue
		}

		slog.Debug("Publishing backfill event", "nodeName", healthEventWithStatus.HealthEvent.NodeName)

		if err := e.publishWithRetry(ctx, healthEventWithStatus.HealthEvent); err != nil {
			return count, fmt.Errorf("publish event: %w", err)
		}

		count++

		metrics.BackfillEventsProcessed.Inc()

		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case <-rateLimiter.C:
		}
	}

	if err := cursor.Err(); err != nil {
		return count, fmt.Errorf("cursor error: %w", err)
	}

	return count, nil
}

func (e *HealthEventsExporter) streamEvents(ctx context.Context) error {
	slog.Info("Starting event stream")

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context done", "error", ctx.Err())
			return ctx.Err()
		case healthEvent, ok := <-e.source.Events():
			if !ok {
				return fmt.Errorf("event channel closed")
			}

			if err := e.processEvent(ctx, healthEvent); err != nil {
				return err
			}
		}
	}
}

func (e *HealthEventsExporter) processEvent(ctx context.Context, healthEvent client.Event) error {
	metrics.EventsReceived.Inc()

	event, err := unmarshalHealthEvent(healthEvent)
	if err != nil {
		slog.Warn("Failed to unmarshal event", "error", err)
		metrics.TransformErrors.Inc()

		return e.markProcessedOrFail(ctx, healthEvent.GetResumeToken())
	}

	if event == nil {
		slog.Debug("Skipping nil health event")
		return e.markProcessedOrFail(ctx, healthEvent.GetResumeToken())
	}

	slog.Debug("Publishing event", "nodeName", event.NodeName)

	if err := e.publishWithRetry(ctx, event); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	return e.markProcessedOrFail(ctx, healthEvent.GetResumeToken())
}

func (e *HealthEventsExporter) markProcessedOrFail(ctx context.Context, token []byte) error {
	if err := e.source.MarkProcessed(ctx, token); err != nil {
		slog.Error("Failed to mark processed", "error", err)
		return fmt.Errorf("mark processed: %w", err)
	}

	metrics.ResumeTokenUpdateTimestamp.SetToCurrentTime()

	return nil
}

func (e *HealthEventsExporter) publishWithRetry(ctx context.Context, event *pb.HealthEvent) error {
	cloudEvent, err := e.transformer.Transform(event)
	if err != nil {
		metrics.TransformErrors.Inc()
		return fmt.Errorf("transform event: %w", err)
	}

	startTime := time.Now()
	attempt := 0

	backoffConfig := wait.Backoff{
		Steps:    e.cfg.Exporter.FailureHandling.MaxRetries + 1,
		Duration: e.cfg.Exporter.FailureHandling.GetInitialBackoff(),
		Factor:   e.cfg.Exporter.FailureHandling.BackoffMultiplier,
		Jitter:   0.1,
		Cap:      e.cfg.Exporter.FailureHandling.GetMaxBackoff(),
	}

	err = wait.ExponentialBackoffWithContext(ctx, backoffConfig, func(ctx context.Context) (bool, error) {
		if attempt > 0 {
			metrics.PublishRetries.Inc()
		}

		publishErr := e.sink.Publish(ctx, cloudEvent)
		if publishErr == nil {
			if attempt > 0 {
				slog.Info("Publish succeeded after retry", "attempt", attempt)
			}

			metrics.PublishDuration.Observe(time.Since(startTime).Seconds())
			metrics.EventsPublished.WithLabelValues(metrics.StatusSuccess).Inc()

			return true, nil
		}

		attempt++

		slog.Warn("Publish failed, retrying",
			"attempt", attempt,
			"maxRetries", e.cfg.Exporter.FailureHandling.MaxRetries,
			"error", publishErr)

		return false, nil
	})
	if err != nil {
		metrics.PublishErrors.WithLabelValues("max_retries_exceeded").Inc()
		metrics.EventsPublished.WithLabelValues(metrics.StatusFailure).Inc()

		return fmt.Errorf("publish failed after %d retries: %w", e.cfg.Exporter.FailureHandling.MaxRetries, err)
	}

	return nil
}

func unmarshalHealthEvent(event client.Event) (*pb.HealthEvent, error) {
	var healthEventWithStatus model.HealthEventWithStatus
	if err := event.UnmarshalDocument(&healthEventWithStatus); err != nil {
		slog.Error("Failed to unmarshal document", "error", err)
		return nil, fmt.Errorf("unmarshal document: %w", err)
	}

	if healthEventWithStatus.HealthEvent == nil {
		slog.Debug("Skipping nil health event")
		return nil, nil
	}

	return healthEventWithStatus.HealthEvent, nil
}
