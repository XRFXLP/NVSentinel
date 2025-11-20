package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nvidia/nvsentinel/event-exporter/pkg/auth"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/config"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/exporter"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/sink"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/transformer"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	_ "github.com/nvidia/nvsentinel/store-client/pkg/datastore/providers"
	"github.com/nvidia/nvsentinel/store-client/pkg/helper"
)

func main() {
	configPath := flag.String("config", "/etc/config/config.toml", "Path to configuration file")
	flag.Parse()

	slog.Info("Health Events Exporter starting")

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	clientSecret := os.Getenv("OIDC_CLIENT_SECRET")
	if clientSecret == "" {
		slog.Error("OIDC_CLIENT_SECRET environment variable not set")
		os.Exit(1)
	}

	scope := cfg.Exporter.OIDC.Scope
	if scope == "" {
		slog.Error("OIDC scope not set")
		os.Exit(1)
	}

	tokenProvider := auth.NewTokenProvider(
		cfg.Exporter.OIDC.TokenURL,
		cfg.Exporter.OIDC.ClientID,
		clientSecret,
		scope,
	)

	httpSink := sink.NewHTTPSink(
		cfg.Exporter.Sink.Endpoint,
		cfg.Exporter.Sink.GetTimeout(),
		tokenProvider,
	)

	cloudEventsTransformer := transformer.NewCloudEventsTransformer(cfg.Exporter.Metadata)

	datastoreConfig, err := datastore.LoadDatastoreConfig()
	if err != nil {
		slog.Error("Failed to load datastore config", "error", err)
		os.Exit(1)
	}

	pipeline := client.BuildAllHealthEventInsertsPipeline()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bundle, err := helper.NewDatastoreClientFromConfig(ctx, "event-exporter", *datastoreConfig, pipeline)
	if err != nil {
		slog.Error("Failed to create datastore bundle", "error", err)
		cancel()
		os.Exit(1) //nolint:gocritic // cancel() called explicitly before exit
	}
	defer bundle.Close(ctx)

	exp := exporter.New(cfg, bundle.DatabaseClient, bundle.ChangeStreamWatcher, cloudEventsTransformer, httpSink)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		slog.Info("Shutdown signal received")
		cancel()
	}()

	slog.Info("Starting event export")

	if err := exp.Run(ctx); err != nil {
		if ctx.Err() == context.Canceled {
			slog.Info("Exporter stopped gracefully")
			slog.Info("Closing watcher to save resume token")

			closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer closeCancel()

			if closeErr := bundle.Close(closeCtx); closeErr != nil {
				slog.Error("Failed to close bundle", "error", closeErr)
			}

			return
		}

		slog.Error("Exporter failed", "error", err)
		os.Exit(1)
	}
}
