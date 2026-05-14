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

// Package pipeline provides a transformer pipeline for processing health events.
// It includes a registry-based factory for creating transformers from configuration.
package pipeline

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/nvidia/nvsentinel/commons/pkg/tracing"
	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

type Transformer interface {
	Transform(ctx context.Context, event *pb.HealthEvent) error
	Name() string
}

// Filter inspects a health event and returns whether it should continue downstream.
type Filter interface {
	Filter(ctx context.Context, event *pb.HealthEvent) (keep bool, err error)
	Name() string
}

// Pipeline runs configured transformers followed by filters for each event.
type Pipeline struct {
	transformers []Transformer
	filters      []Filter
}

func New(transformers ...Transformer) *Pipeline {
	return &Pipeline{transformers: transformers}
}

// NewWithFilters creates a pipeline with transformer and filter stages.
func NewWithFilters(transformers []Transformer, filters []Filter) *Pipeline {
	return &Pipeline{transformers: transformers, filters: filters}
}

// Close releases resources owned by filters that expose a Close method.
func (p *Pipeline) Close() {
	for _, f := range p.filters {
		closer, ok := f.(interface{ Close() error })
		if !ok {
			continue
		}

		if err := closer.Close(); err != nil {
			slog.Warn("Failed to close pipeline filter", "filter", f.Name(), "error", err)
		}
	}
}

// Process applies the pipeline and returns false when a filter drops the event.
func (p *Pipeline) Process(ctx context.Context, event *pb.HealthEvent) bool {
	ctx, span := tracing.StartSpan(ctx, "platform_connector.pipeline.process")
	defer span.End()

	var failedCount int

	for _, t := range p.transformers {
		if err := t.Transform(ctx, event); err != nil {
			failedCount++

			slog.WarnContext(ctx, "Transformer failed",
				"transformer", t.Name(),
				"node", event.NodeName,
				"error", err)
			tracing.RecordError(span, err)
			span.AddEvent("platform_connector.pipeline.transformer_failed", trace.WithAttributes(
				attribute.String("platform_connector.pipeline.failed_transformer", t.Name()),
				attribute.String("platform_connector.pipeline.error.type", "running_transformer_failed"),
				attribute.String("platform_connector.pipeline.error.message", err.Error()),
			))
		}
	}

	for _, f := range p.filters {
		keep, err := f.Filter(ctx, event)
		if err != nil {
			failedCount++

			slog.WarnContext(ctx, "Filter failed",
				"filter", f.Name(),
				"node", event.NodeName,
				"error", err)
			tracing.RecordError(span, err)
			span.AddEvent("platform_connector.pipeline.filter_failed", trace.WithAttributes(
				attribute.String("platform_connector.pipeline.failed_filter", f.Name()),
				attribute.String("platform_connector.pipeline.error.type", "running_filter_failed"),
				attribute.String("platform_connector.pipeline.error.message", err.Error()),
			))

			continue
		}

		if !keep {
			span.AddEvent("platform_connector.pipeline.event_dropped", trace.WithAttributes(
				attribute.String("platform_connector.pipeline.filter", f.Name()),
			))

			return false
		}
	}

	span.SetAttributes(attribute.Int("platform_connector.pipeline.failed_stage_count", failedCount))

	return true
}
