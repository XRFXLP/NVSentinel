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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Node label written by the pipeline, from commons/pkg/statemanager.
const nodeStateLabelKey = "dgxc.nvidia.com/nvsentinel-state"

const (
	saTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token" //nolint:gosec
	saCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

// nodeStateFor maps a pipeline stage to the node label value the components
// expect to already be present. A quarantined event whose node carries no
// label makes node-drainer no-op, so keeping this in step with the event
// stage is what makes a scenario reproducible.
func nodeStateFor(stage string) (label string, cordon bool, ok bool) {
	switch stage {
	case stageQuarantined:
		return "quarantined", true, true
	case stageDrained:
		return "drain-succeeded", true, true
	case stageRemediated:
		return "remediating", true, true
	default:
		return "", false, false
	}
}

// nodePatcher patches Node objects over the in-cluster REST API. It uses the
// service account token directly rather than client-go to keep this module's
// dependency set small.
type nodePatcher struct {
	host    string
	token   string
	client  *http.Client
	patched atomic.Int64
	failed  atomic.Int64
}

func newNodePatcher(apiServer string) (*nodePatcher, error) {
	token, err := os.ReadFile(saTokenPath)
	if err != nil {
		return nil, fmt.Errorf("read service account token (is this running in-cluster?): %w", err)
	}

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if ca, err := os.ReadFile(saCAPath); err == nil {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		tlsCfg.RootCAs = pool
	}

	return &nodePatcher{
		host:  apiServer,
		token: string(bytes.TrimSpace(token)),
		client: &http.Client{
			Timeout:   15 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tlsCfg, MaxIdleConnsPerHost: 64},
		},
	}, nil
}

// patch applies the label and, when the stage implies it, marks the node
// unschedulable in a single strategic-merge patch.
func (p *nodePatcher) patch(node, labelValue string, cordon bool) error {
	body := map[string]any{
		"metadata": map[string]any{
			"labels": map[string]string{nodeStateLabelKey: labelValue},
		},
	}
	if cordon {
		body["spec"] = map[string]any{"unschedulable": true}
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/nodes/%s", p.host, node)

	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("patch node %s: %s: %s", node, resp.Status, string(msg))
	}

	_, _ = io.Copy(io.Discard, resp.Body)

	return nil
}

// syncNodes patches every node in the list, bounded by concurrency. Failures
// are counted and reported rather than aborting: on a KWOK fleet a handful of
// nodes may legitimately not exist.
func (p *nodePatcher) syncNodes(nodes []string, labelValue string, cordon bool, concurrency int) {
	jobs := make(chan string, concurrency*2)

	var wg sync.WaitGroup

	for range concurrency {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for node := range jobs {
				if err := p.patch(node, labelValue, cordon); err != nil {
					if p.failed.Add(1) <= 5 {
						fmt.Fprintf(os.Stderr, "node sync: %v\n", err)
					}

					continue
				}

				p.patched.Add(1)
			}
		}()
	}

	for _, n := range nodes {
		jobs <- n
	}

	close(jobs)
	wg.Wait()
}
