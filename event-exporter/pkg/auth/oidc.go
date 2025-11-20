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

package auth

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/nvidia/nvsentinel/event-exporter/pkg/metrics"
)

type TokenProvider struct {
	tokenURL     string
	clientID     string
	clientSecret string
	scope        string

	mu             sync.RWMutex
	cachedToken    string
	tokenExpiresAt time.Time
}

func NewTokenProvider(tokenURL, clientID, clientSecret, scope string) *TokenProvider {
	return &TokenProvider{
		tokenURL:     tokenURL,
		clientID:     clientID,
		clientSecret: clientSecret,
		scope:        scope,
	}
}

func (p *TokenProvider) GetToken(ctx context.Context) (string, error) {
	if token := p.getCachedToken(); token != "" {
		return token, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if token := p.getCachedTokenUnsafe(); token != "" {
		return token, nil
	}

	return p.fetchNewToken(ctx)
}

func (p *TokenProvider) getCachedToken() string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.getCachedTokenUnsafe()
}

func (p *TokenProvider) getCachedTokenUnsafe() string {
	if p.cachedToken != "" && time.Now().Before(p.tokenExpiresAt.Add(-time.Minute)) {
		return p.cachedToken
	}

	return ""
}

func (p *TokenProvider) fetchNewToken(ctx context.Context) (string, error) {
	req, err := p.createTokenRequest(ctx)
	if err != nil {
		return "", err
	}

	tokenResp, err := p.executeTokenRequest(req)
	if err != nil {
		return "", err
	}

	p.cachedToken = tokenResp.AccessToken
	p.tokenExpiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)

	return p.cachedToken, nil
}

func (p *TokenProvider) createTokenRequest(ctx context.Context) (*http.Request, error) {
	formData := url.Values{
		"scope":      {p.scope},
		"grant_type": {"client_credentials"},
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		p.tokenURL,
		strings.NewReader(formData.Encode()),
	)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	authBytes := fmt.Appendf(nil, "%s:%s", p.clientID, p.clientSecret)
	auth := base64.StdEncoding.EncodeToString(authBytes)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", fmt.Sprintf("Basic %s", auth))

	return req, nil
}

func (p *TokenProvider) executeTokenRequest(req *http.Request) (*tokenResponse, error) {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
			},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		metrics.TokenRefreshErrors.Inc()
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		metrics.TokenRefreshErrors.Inc()
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		metrics.TokenRefreshErrors.Inc()
		return nil, fmt.Errorf("token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp tokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		metrics.TokenRefreshErrors.Inc()
		return nil, fmt.Errorf("parse response: %w", err)
	}

	return &tokenResp, nil
}

type tokenResponse struct {
	AccessToken string  `json:"access_token"`
	ExpiresIn   float64 `json:"expires_in"`
}
