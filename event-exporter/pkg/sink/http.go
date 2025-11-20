package sink

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/nvidia/nvsentinel/event-exporter/pkg/auth"
	"github.com/nvidia/nvsentinel/event-exporter/pkg/transformer"
)

type HTTPSink struct {
	endpoint      string
	timeout       time.Duration
	tokenProvider *auth.TokenProvider
	client        *http.Client
}

func NewHTTPSink(
	endpoint string,
	timeout time.Duration,
	tokenProvider *auth.TokenProvider,
) *HTTPSink {
	return &HTTPSink{
		endpoint:      endpoint,
		timeout:       timeout,
		tokenProvider: tokenProvider,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					MinVersion: tls.VersionTLS12,
				},
			},
		},
	}
}

func (s *HTTPSink) Publish(ctx context.Context, event *transformer.CloudEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	token, err := s.tokenProvider.GetToken(ctx)
	if err != nil {
		slog.Error("Failed to get token", "error", err)
		return fmt.Errorf("get token: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		slog.Error("Failed to create request", "error", err)
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/cloudevents+json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	resp, err := s.client.Do(req)
	if err != nil {
		slog.Error("Failed to execute request", "error", err)
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		slog.Warn("Failed to read response body", "error", readErr)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(respBody))
	}

	slog.Info("Event published successfully",
		"status", resp.StatusCode,
		"responseBody", string(respBody),
		"nodeName", event.Data["healthEvent"].(map[string]any)["nodeName"],
	)

	return nil
}

func (s *HTTPSink) Close(ctx context.Context) error {
	s.client.CloseIdleConnections()
	return nil
}
