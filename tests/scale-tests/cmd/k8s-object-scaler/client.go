// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
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

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	saTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token" //nolint:gosec
	saCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

// apiClient talks to the Kubernetes API directly over HTTP. client-go is
// deliberately avoided: its default rate limiter (QPS 5 / burst 10) is the very
// thing that makes object creation slow, and here we want the API server's own
// admission-control limits to be the only ceiling.
type apiClient struct {
	host  string
	token string
	http  *http.Client

	created   atomic.Int64
	deleted   atomic.Int64
	conflicts atomic.Int64
	throttled atomic.Int64
	failed    atomic.Int64
}

func newAPIClient(host string, concurrency int, insecure bool) (*apiClient, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12} //nolint:gosec

	if ca, err := os.ReadFile(saCAPath); err == nil {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		tlsCfg.RootCAs = pool
	} else if insecure {
		tlsCfg.InsecureSkipVerify = true
	} else {
		return nil, fmt.Errorf("read cluster CA %s: %w (use --insecure outside a pod)", saCAPath, err)
	}

	var token string

	if b, err := os.ReadFile(saTokenPath); err == nil {
		token = string(bytes.TrimSpace(b))
	} else {
		return nil, fmt.Errorf("read service account token: %w (this must run in-cluster)", err)
	}

	return &apiClient{
		host:  host,
		token: token,
		http: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig:     tlsCfg,
				MaxIdleConns:        concurrency * 2,
				MaxIdleConnsPerHost: concurrency * 2,
				MaxConnsPerHost:     concurrency * 2,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}, nil
}

// do issues a single request and classifies the outcome. 409 (already exists)
// and 404 (already gone) are treated as success for idempotency, so a rerun
// converges instead of erroring.
func (c *apiClient) do(ctx context.Context, method, path string, body []byte) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.host+path, reader)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+c.token)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		c.failed.Add(1)
		return err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode < 300:
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil

	case resp.StatusCode == http.StatusConflict:
		// Object already exists; that is the desired end state.
		c.conflicts.Add(1)
		_, _ = io.Copy(io.Discard, resp.Body)

		return nil

	case resp.StatusCode == http.StatusNotFound && method == http.MethodDelete:
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil

	case resp.StatusCode == http.StatusTooManyRequests:
		c.throttled.Add(1)
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		_, _ = io.Copy(io.Discard, resp.Body)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryAfter):
		}

		// One retry; sustained 429s mean the requested concurrency is above
		// what API Priority and Fairness will grant.
		return c.do(ctx, method, path, body)

	default:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
		c.failed.Add(1)

		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, bytes.TrimSpace(msg))
	}
}

func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 250 * time.Millisecond
	}

	if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}

	return 250 * time.Millisecond
}

// ensureOwner creates a placeholder controller object in the namespace and
// returns its UID, so generated pods can carry a real ownerReference.
//
// This is required rather than cosmetic: a pod whose ownerReferences point at a
// UID that does not exist is treated as an orphaned dependent and deleted by
// the garbage collector within seconds. The owner is given a nodeSelector that
// matches nothing, so its own controller never schedules pods of its own.
func (c *apiClient) ensureOwner(ctx context.Context, namespace, kind, name string) (string, error) {
	var path string

	switch kind {
	case "DaemonSet":
		path = fmt.Sprintf("/apis/apps/v1/namespaces/%s/daemonsets", namespace)
	case "ReplicaSet":
		path = fmt.Sprintf("/apis/apps/v1/namespaces/%s/replicasets", namespace)
	default:
		return "", fmt.Errorf("unsupported --owner-kind %q (want DaemonSet or ReplicaSet)", kind)
	}

	sel := map[string]any{"matchLabels": map[string]string{"bench-owner": name}}
	tmpl := map[string]any{
		"metadata": map[string]any{"labels": map[string]string{"bench-owner": name}},
		"spec": map[string]any{
			"nodeSelector": map[string]string{"bench.nvsentinel.io/never-schedule": "true"},
			"containers": []map[string]any{{
				"name": "pause", "image": "registry.k8s.io/pause:3.9",
			}},
		},
	}
	spec := map[string]any{"selector": sel, "template": tmpl}

	if kind == "ReplicaSet" {
		spec["replicas"] = 0
	}

	body, err := json.Marshal(map[string]any{
		"apiVersion": "apps/v1",
		"kind":       kind,
		"metadata":   map[string]any{"name": name, "namespace": namespace},
		"spec":       spec,
	})
	if err != nil {
		return "", err
	}

	// 409 from do() is treated as success, so a second run reuses the owner.
	if err := c.do(ctx, http.MethodPost, path, body); err != nil {
		return "", fmt.Errorf("create owner %s/%s: %w", kind, name, err)
	}

	return c.uidOf(ctx, path+"/"+name)
}

// uidOf reads metadata.uid from an object.
func (c *apiClient) uidOf(ctx context.Context, path string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.host+path, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var obj struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
		return "", err
	}

	if obj.Metadata.UID == "" {
		return "", fmt.Errorf("owner %s has no uid", path)
	}

	return obj.Metadata.UID, nil
}

// patch applies a strategic-merge patch. It is separate from do() only because
// PATCH needs its own Content-Type; the API server rejects a patch sent as
// application/json.
func (c *apiClient) patch(ctx context.Context, path string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.host+path, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")

	resp, err := c.http.Do(req)
	if err != nil {
		c.failed.Add(1)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		c.throttled.Add(1)
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		_, _ = io.Copy(io.Discard, resp.Body)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryAfter):
		}

		return c.patch(ctx, path, body)
	}

	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
	c.failed.Add(1)

	return fmt.Errorf("PATCH %s: %s: %s", path, resp.Status, bytes.TrimSpace(msg))
}
