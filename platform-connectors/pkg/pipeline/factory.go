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
	"fmt"
	"log/slog"
)

type Options struct {
	KubeconfigPath string
}

type Factory func(cfg *Config, opts Options) (Transformer, error)

// FilterFactory creates a filter from pipeline configuration.
type FilterFactory func(cfg *Config) (Filter, error)

var (
	registry       = map[string]Factory{}
	filterRegistry = map[string]FilterFactory{}
)

func Register(name string, factory Factory) {
	registry[name] = factory
}

// RegisterFilter registers a filter factory by pipeline stage name.
func RegisterFilter(name string, factory FilterFactory) {
	filterRegistry[name] = factory
}

func Create(cfg *Config, opts Options) (Transformer, error) {
	factory, ok := registry[cfg.Name]
	if !ok {
		return nil, fmt.Errorf("unknown transformer: %s", cfg.Name)
	}

	return factory(cfg, opts)
}

// CreateFilter instantiates a registered filter from configuration.
func CreateFilter(cfg *Config) (Filter, error) {
	factory, ok := filterRegistry[cfg.Name]
	if !ok {
		return nil, fmt.Errorf("unknown filter: %s", cfg.Name)
	}

	return factory(cfg)
}

// NewFromConfigs creates a Pipeline from a slice of transformer configurations.
// Disabled stages are skipped. Returns an error if any enabled stage fails to initialize.
func NewFromConfigs(ctx context.Context, configs []Config, opts Options) (*Pipeline, error) {
	var transformers []Transformer
	var filters []Filter

	for _, cfg := range configs {
		if !cfg.Enabled {
			slog.InfoContext(ctx, "Pipeline stage disabled", "name", cfg.Name)
			continue
		}

		if factory, ok := registry[cfg.Name]; ok {
			t, err := factory(&cfg, opts)
			if err != nil {
				return nil, fmt.Errorf("failed to create transformer %s: %w", cfg.Name, err)
			}

			transformers = append(transformers, t)
			slog.InfoContext(ctx, "Transformer registered", "name", t.Name())
			continue
		}

		if factory, ok := filterRegistry[cfg.Name]; ok {
			f, err := factory(&cfg)
			if err != nil {
				return nil, fmt.Errorf("failed to create filter %s: %w", cfg.Name, err)
			}

			filters = append(filters, f)
			slog.InfoContext(ctx, "Filter registered", "name", f.Name())
			continue
		}

		return nil, fmt.Errorf("unknown pipeline stage: %s", cfg.Name)
	}

	return NewWithFilters(transformers, filters), nil
}
