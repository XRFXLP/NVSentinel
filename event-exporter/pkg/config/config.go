package config

import (
	"fmt"
	"time"

	"github.com/nvidia/nvsentinel/commons/pkg/configmanager"
)

type Config struct {
	Exporter ExporterConfig `toml:"exporter"`
}

type ExporterConfig struct {
	Metadata        MetadataConfig        `toml:"metadata"`
	Sink            SinkConfig            `toml:"sink"`
	OIDC            OIDCConfig            `toml:"oidc"`
	Backfill        BackfillConfig        `toml:"backfill"`
	ResumeToken     ResumeTokenConfig     `toml:"resume_token"`
	FailureHandling FailureHandlingConfig `toml:"failure_handling"`
}

type MetadataConfig map[string]string

type SinkConfig struct {
	Endpoint string `toml:"endpoint"`
	Timeout  string `toml:"timeout"`
}

type OIDCConfig struct {
	TokenURL string `toml:"token_url"`
	ClientID string `toml:"client_id"`
	Scope    string `toml:"scope"`
}

type BackfillConfig struct {
	Enabled   bool   `toml:"enabled"`
	MaxAge    string `toml:"max_age"`
	MaxEvents int    `toml:"max_events"`
	BatchSize int    `toml:"batch_size"`
	RateLimit int    `toml:"rate_limit"`
}

type ResumeTokenConfig struct {
	Collection string `toml:"collection"`
	Database   string `toml:"database"`
}

type FailureHandlingConfig struct {
	MaxRetries        int     `toml:"max_retries"`
	InitialBackoff    string  `toml:"initial_backoff"`
	MaxBackoff        string  `toml:"max_backoff"`
	BackoffMultiplier float64 `toml:"backoff_multiplier"`
}

func (c *SinkConfig) GetTimeout() time.Duration {
	if c.Timeout == "" {
		return 30 * time.Second
	}

	d, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return 30 * time.Second
	}

	return d
}

func (c *BackfillConfig) GetMaxAge() time.Duration {
	if c.MaxAge == "" {
		return 720 * time.Hour
	}

	d, err := time.ParseDuration(c.MaxAge)
	if err != nil {
		return 720 * time.Hour
	}

	return d
}

func (c *FailureHandlingConfig) GetInitialBackoff() time.Duration {
	if c.InitialBackoff == "" {
		return 1 * time.Second
	}

	d, err := time.ParseDuration(c.InitialBackoff)
	if err != nil {
		return 1 * time.Second
	}

	return d
}

func (c *FailureHandlingConfig) GetMaxBackoff() time.Duration {
	if c.MaxBackoff == "" {
		return 60 * time.Second
	}

	d, err := time.ParseDuration(c.MaxBackoff)
	if err != nil {
		return 60 * time.Second
	}

	return d
}

func Load(path string) (*Config, error) {
	var cfg Config

	if err := configmanager.LoadTOMLConfig(path, &cfg); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	return &cfg, nil
}
