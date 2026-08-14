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

// Package kwok implements a simulated CSP provider for KWOK test clusters.
//
// SendRebootSignal encodes the current Unix timestamp in the requestID and
// optionally sets a label to trigger KWOK lifecycle stages (see
// kwok-stages-reboot.yaml). IsNodeReady returns true once the configured
// reboot duration has elapsed AND the Kubernetes node reports Ready=True.
//
// The label (kwok.x-k8s.io/reboot) is removed by a background goroutine
// after rebootDuration+5s, preventing the KWOK stages from looping.
package kwok

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/nvidia/nvsentinel/janitor-provider/pkg/model"
)

const (
	// RebootLabel is set on the node to trigger KWOK reboot simulation stages.
	RebootLabel = "kwok.x-k8s.io/reboot"
	// TerminateLabel is set on the node to signal termination.
	TerminateLabel = "kwok.x-k8s.io/terminate"
	// DefaultRebootDuration is how long to simulate a node reboot.
	DefaultRebootDuration = 30 * time.Second
)

// Client implements model.CSPClient for KWOK simulated nodes.
type Client struct {
	k8s            kubernetes.Interface
	rebootDuration time.Duration
	// enableStages controls whether SendRebootSignal sets the KWOK stage label.
	enableStages bool
	// skipNodeCheck skips the kubernetesReady check in IsNodeReady, returning
	// true purely based on elapsed time. Set KWOK_SKIP_NODE_CHECK=true.
	// Use for benchmarking when KWOK node conditions are unreliable (e.g. in
	// large clusters where KWOK controller is unstable).
	skipNodeCheck bool
}

// NewClient creates a kwok CSP client using in-cluster credentials.
func NewClient(ctx context.Context) (*Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("building in-cluster config: %w", err)
	}
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building kubernetes client: %w", err)
	}
	enableStages  := os.Getenv("KWOK_ENABLE_STAGES") == "true"
	skipNodeCheck := os.Getenv("KWOK_SKIP_NODE_CHECK") == "true"
	return &Client{k8s: k8s, rebootDuration: DefaultRebootDuration,
		enableStages: enableStages, skipNodeCheck: skipNodeCheck}, nil
}

// SendRebootSignal triggers the reboot simulation.
//
// It optionally sets kwok.x-k8s.io/reboot=requested on the node to trigger
// the KWOK lifecycle stages (node goes NotReady for ~30s). A background
// goroutine removes the label after rebootDuration+5s to prevent stage loops.
//
// The requestID encodes "<nodeName>:<unix_start_timestamp>" so IsNodeReady
// can compute elapsed time without any shared state.
func (c *Client) SendRebootSignal(ctx context.Context, node corev1.Node) (model.ResetSignalRequestRef, error) {
	slog.InfoContext(ctx, "KWOK: triggering reboot simulation", "node", node.Name,
		"duration", c.rebootDuration)

	startTs := time.Now().Unix()
	requestID := fmt.Sprintf("%s:%d", node.Name, startTs)

	// Optionally label the node to trigger KWOK lifecycle stages.
	// Disabled by default — stages can cause a NotReady loop without careful
	// coordination. Enable with KWOK_ENABLE_STAGES=true for realistic simulation.
	if c.enableStages {
		if err := c.patchLabel(ctx, node.Name, RebootLabel, "requested"); err != nil {
			slog.WarnContext(ctx, "KWOK: failed to set reboot label (stages won't fire)", "node", node.Name, "error", err)
		} else {
			go func() {
				time.Sleep(c.rebootDuration + 5*time.Second)
				if err := c.removeLabel(context.Background(), node.Name, RebootLabel); err != nil {
					slog.Warn("KWOK: failed to remove reboot label", "node", node.Name, "error", err)
				}
			}()
		}
	}

	return model.ResetSignalRequestRef(requestID), nil
}

// IsNodeReady returns true once the simulated reboot duration has elapsed
// AND the Kubernetes node condition NodeReady is True.
//
// The elapsed time is derived from the requestID timestamp, making this
// call fully stateless.
func (c *Client) IsNodeReady(ctx context.Context, node corev1.Node, requestID string) (bool, error) {
	elapsed, err := elapsedFromRequestID(requestID)
	if err != nil {
		// Fallback: treat as immediately ready (shouldn't happen in practice).
		slog.WarnContext(ctx, "KWOK: could not parse requestID, assuming ready", "requestID", requestID)
		return isNodeKubernetesReady(node), nil
	}

	if elapsed < c.rebootDuration {
		slog.DebugContext(ctx, "KWOK: reboot still in progress",
			"node", node.Name, "elapsed", elapsed, "required", c.rebootDuration)
		return false, nil
	}

	// In skip-node-check mode (KWOK_SKIP_NODE_CHECK=true), return true purely
	// on elapsed time. Use this when KWOK node conditions are unreliable.
	if c.skipNodeCheck {
		slog.DebugContext(ctx, "KWOK: elapsed, skipping node check", "node", node.Name)
		return true, nil
	}

	ready := isNodeKubernetesReady(node)
	slog.DebugContext(ctx, "KWOK: reboot duration elapsed, checking node",
		"node", node.Name, "kubernetesReady", ready)
	return ready, nil
}

// SendTerminateSignal simulates node termination (no recovery).
func (c *Client) SendTerminateSignal(ctx context.Context, node corev1.Node) (model.TerminateNodeRequestRef, error) {
	slog.InfoContext(ctx, "KWOK: triggering termination simulation", "node", node.Name)
	if err := c.patchLabel(ctx, node.Name, TerminateLabel, "requested"); err != nil {
		slog.WarnContext(ctx, "KWOK: failed to set terminate label", "node", node.Name, "error", err)
	}
	return model.TerminateNodeRequestRef(node.Name), nil
}

// elapsedFromRequestID parses "<nodeName>:<unixTimestamp>" and returns elapsed time.
func elapsedFromRequestID(requestID string) (time.Duration, error) {
	parts := strings.SplitN(requestID, ":", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid requestID format: %q", requestID)
	}
	ts, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid timestamp in requestID %q: %w", requestID, err)
	}
	return time.Since(time.Unix(ts, 0)), nil
}

// isNodeKubernetesReady checks the NodeReady condition.
func isNodeKubernetesReady(node corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (c *Client) patchLabel(ctx context.Context, nodeName, key, value string) error {
	patch := map[string]interface{}{"metadata": map[string]interface{}{"labels": map[string]interface{}{key: value}}}
	data, _ := json.Marshal(patch)
	_, err := c.k8s.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

func (c *Client) removeLabel(ctx context.Context, nodeName, key string) error {
	patch := map[string]interface{}{"metadata": map[string]interface{}{"labels": map[string]interface{}{key: nil}}}
	data, _ := json.Marshal(patch)
	_, err := c.k8s.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}
