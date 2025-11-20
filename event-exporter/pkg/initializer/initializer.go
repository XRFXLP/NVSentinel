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

package initializer

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/nvidia/nvsentinel/event-exporter/pkg/auth"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/config"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/exporter"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/sink"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/transformer"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	"github.com/nvidia/nvsentinel/store-client/pkg/helper"
)

type Params struct {
	ConfigPath string
}

type Components struct {
	Exporter        *exporter.HealthEventsExporter
	DatastoreBundle *helper.DatastoreClientBundle
}

func InitializeAll(ctx context.Context, params Params) (*Components, error) {
	cfg, err := loadConfig(params.ConfigPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	tokenProvider, err := initializeOIDC(cfg)
	if err != nil {
		slog.Error("Failed to initialize OIDC", "error", err)
		return nil, fmt.Errorf("failed to initialize OIDC: %w", err)
	}

	httpSink := sink.NewHTTPSink(
		cfg.Exporter.Sink.Endpoint,
		cfg.Exporter.Sink.GetTimeout(),
		tokenProvider,
	)

	cloudEventsTransformer := transformer.NewCloudEventsTransformer(cfg.Exporter.Metadata)

	datastoreBundle, err := initializeDatastore(ctx)
	if err != nil {
		slog.Error("Failed to initialize datastore", "error", err)
		return nil, fmt.Errorf("failed to initialize datastore: %w", err)
	}

	exp := exporter.New(
		cfg,
		datastoreBundle.DatabaseClient,
		datastoreBundle.ChangeStreamWatcher,
		cloudEventsTransformer,
		httpSink,
	)

	return &Components{
		Exporter:        exp,
		DatastoreBundle: datastoreBundle,
	}, nil
}

func loadConfig(configPath string) (*config.Config, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		return nil, fmt.Errorf("failed to load config from %s: %w", configPath, err)
	}

	slog.Info("Configuration loaded", "path", configPath)

	return cfg, nil
}

func initializeOIDC(cfg *config.Config) (*auth.TokenProvider, error) {
	clientSecret := os.Getenv("OIDC_CLIENT_SECRET")
	if clientSecret == "" {
		slog.Error("OIDC_CLIENT_SECRET environment variable not set")
		return nil, fmt.Errorf("OIDC_CLIENT_SECRET environment variable not set")
	}

	if cfg.Exporter.OIDC.TokenURL == "" {
		slog.Error("OIDC token URL not configured")
		return nil, fmt.Errorf("OIDC token URL not configured")
	}

	if cfg.Exporter.OIDC.ClientID == "" {
		slog.Error("OIDC client ID not configured")
		return nil, fmt.Errorf("OIDC client ID not configured")
	}

	scope := cfg.Exporter.OIDC.Scope
	if scope == "" {
		slog.Error("OIDC scope not configured")
		return nil, fmt.Errorf("OIDC scope not configured")
	}

	tokenProvider := auth.NewTokenProvider(
		cfg.Exporter.OIDC.TokenURL,
		cfg.Exporter.OIDC.ClientID,
		clientSecret,
		scope,
	)

	slog.Info("OIDC token provider initialized",
		"tokenURL", cfg.Exporter.OIDC.TokenURL,
		"clientID", cfg.Exporter.OIDC.ClientID,
		"scope", scope)

	return tokenProvider, nil
}

func initializeDatastore(ctx context.Context) (*helper.DatastoreClientBundle, error) {
	datastoreConfig, err := datastore.LoadDatastoreConfig()
	if err != nil {
		slog.Error("Failed to load datastore config", "error", err)
		return nil, fmt.Errorf("failed to load datastore config: %w", err)
	}

	pipeline := client.BuildAllHealthEventInsertsPipeline()

	bundle, err := helper.NewDatastoreClientFromConfig(ctx, "event-exporter", *datastoreConfig, pipeline)
	if err != nil {
		slog.Error("Failed to create datastore client", "error", err)
		return nil, fmt.Errorf("failed to create datastore client: %w", err)
	}

	slog.Info("Datastore client initialized", "provider", datastoreConfig.Provider)

	return bundle, nil
}
