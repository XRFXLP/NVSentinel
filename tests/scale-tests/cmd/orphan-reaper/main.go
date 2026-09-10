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

// orphan-reaper deletes pods still bound to nodes that no longer exist.
//
// Kubernetes garbage collection reclaims these, but on a large fleet it does so
// far slower than they accumulate and can stop making progress entirely, which
// leaves kube-controller-manager and the scheduler degraded and holds etcd
// space. This does the same work directly.
//
// Pods are found through the spec.nodeName field index, one request per dead
// node, so nothing lists the full pod collection. It runs in-cluster against
// the API server directly: routing this volume through `kubectl proxy`
// saturates that single process and starves every other client of the cluster.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token" //nolint:gosec
	caPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

type client struct {
	host, token string
	http        *http.Client
	listed      atomic.Int64
	deleted     atomic.Int64
	failed      atomic.Int64
}

func (c *client) do(ctx context.Context, method, path string) ([]byte, int) {
	req, err := http.NewRequestWithContext(ctx, method, c.host+path, nil)
	if err != nil {
		return nil, 0
	}

	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)

	return buf.Bytes(), resp.StatusCode
}

type podList struct {
	Items []struct {
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
	} `json:"items"`
}

func main() {
	var (
		host        = flag.String("api-server", "https://kubernetes.default.svc", "API server URL")
		prefix      = flag.String("prefix", "pb-", "node name prefix")
		first       = flag.Int("first", 0, "first dead node index")
		last        = flag.Int("last", 0, "one past the last dead node index")
		concurrency = flag.Int("concurrency", 200, "parallel workers")
	)
	flag.Parse()

	tok, err := os.ReadFile(tokenPath)
	if err != nil {
		log.Fatalf("read service account token: %v (must run in-cluster)", err)
	}

	ca, err := os.ReadFile(caPath)
	if err != nil {
		log.Fatalf("read cluster CA: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)

	c := &client{
		host:  *host,
		token: string(bytes.TrimSpace(tok)),
		http: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig:     &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
				MaxIdleConns:        *concurrency * 2,
				MaxIdleConnsPerHost: *concurrency * 2,
				MaxConnsPerHost:     *concurrency * 2,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}

	ctx := context.Background()
	idx := make(chan int, *concurrency*2)

	var wg sync.WaitGroup

	for range *concurrency {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range idx {
				node := fmt.Sprintf("%s%06d", *prefix, i)

				body, st := c.do(ctx, http.MethodGet, "/api/v1/pods?fieldSelector=spec.nodeName%3D"+node)
				if st != http.StatusOK {
					c.failed.Add(1)
					continue
				}

				c.listed.Add(1)

				var pl podList
				if err := json.Unmarshal(body, &pl); err != nil {
					c.failed.Add(1)
					continue
				}

				for _, p := range pl.Items {
					// Grace period zero: the kubelet that owned these pods is
					// gone with its node, so nothing will ever confirm a
					// graceful shutdown.
					_, ds := c.do(ctx, http.MethodDelete,
						fmt.Sprintf("/api/v1/namespaces/%s/pods/%s?gracePeriodSeconds=0",
							p.Metadata.Namespace, p.Metadata.Name))
					if ds == http.StatusOK || ds == http.StatusAccepted || ds == http.StatusNotFound {
						c.deleted.Add(1)
					} else {
						c.failed.Add(1)
					}
				}
			}
		}()
	}

	start := time.Now()
	done := make(chan struct{})

	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()

		for {
			select {
			case <-done:
				return
			case <-t.C:
				el := time.Since(start).Seconds()
				log.Printf("nodes=%d deleted=%d failed=%d (%.0f pods/s)",
					c.listed.Load(), c.deleted.Load(), c.failed.Load(), float64(c.deleted.Load())/el)
			}
		}
	}()

	for i := *first; i < *last; i++ {
		idx <- i
	}

	close(idx)
	wg.Wait()
	close(done)

	log.Printf("done: nodes=%d deleted=%d failed=%d in %s",
		c.listed.Load(), c.deleted.Load(), c.failed.Load(), time.Since(start).Round(time.Second))
}
