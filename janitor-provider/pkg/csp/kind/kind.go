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

package kind

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/nvidia/nvsentinel/janitor-provider/pkg/model"
)

var (
	_ model.CSPClient = (*Client)(nil)
)

// defaultRebootDuration is how long a simulated reboot takes when
// KIND_REBOOT_DURATION is unset. It is deliberately short: a long simulated
// wait becomes the dominant term in every measurement and hides the behaviour
// of the components themselves. Scale tests sweep this value upward from near
// zero to find where the external wait starts masking internal limits.
const defaultRebootDuration = 5 * time.Second

// Client is the Kind implementation of the CSP Client interface.
type Client struct {
	rebootDuration time.Duration
}

// NewClient creates a new Kind client. The simulated reboot duration is read
// from KIND_REBOOT_DURATION (any Go duration string, e.g. "5m", "1h").
func NewClient(ctx context.Context) (*Client, error) {
	// Kind client is a simulation client with no actual CSP connection
	d := defaultRebootDuration

	if v := os.Getenv("KIND_REBOOT_DURATION"); v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse KIND_REBOOT_DURATION %q: %w", v, err)
		}

		if parsed < 0 {
			return nil, fmt.Errorf("KIND_REBOOT_DURATION must not be negative, got %s", parsed)
		}

		d = parsed
	}

	slog.InfoContext(ctx, "Kind CSP client initialised", "simulatedRebootDuration", d)

	return &Client{rebootDuration: d}, nil
}

// SendRebootSignal simulates sending a reboot signal for a kind node. It
// returns immediately, encoding the start time in the request reference the
// way the azure and oci providers do; the wait is accounted for in
// IsNodeReady. Blocking here would instead cap how many reboots can be in
// flight at once, which is exactly the property scale tests need to measure.
func (c *Client) SendRebootSignal(ctx context.Context, node corev1.Node) (model.ResetSignalRequestRef, error) {
	return model.ResetSignalRequestRef(time.Now().UTC().Format(time.RFC3339)), nil
}

// IsNodeReady reports the node ready once the simulated reboot duration has
// elapsed since the signal was sent.
func (c *Client) IsNodeReady(ctx context.Context, node corev1.Node, requestID string) (bool, error) {
	if requestID == "" {
		// No start time recorded (e.g. a CR created before this field was
		// populated); fall back to reporting ready.
		return true, nil
	}

	startedAt, err := time.Parse(time.RFC3339, requestID)
	if err != nil {
		return false, fmt.Errorf("parse reboot start time %q: %w", requestID, err)
	}

	return time.Since(startedAt) >= c.rebootDuration, nil
}

// SendTerminateSignal simulates terminating a kind node by removing the docker container
func (c *Client) SendTerminateSignal(
	ctx context.Context,
	node corev1.Node,
) (model.TerminateNodeRequestRef, error) {
	// Check if provider ID has the correct prefix
	if !strings.HasPrefix(node.Spec.ProviderID, "kind://") {
		return "", fmt.Errorf("invalid provider ID format: %s", node.Spec.ProviderID)
	}

	// Extract container name from provider ID
	parts := strings.Split(node.Spec.ProviderID, "/")
	if len(parts) < 5 {
		return "", fmt.Errorf("invalid provider ID format: %s", node.Spec.ProviderID)
	}

	containerName := parts[len(parts)-1]
	clusterName := parts[3]

	slog.InfoContext(ctx, "Attempting to terminate node", "node", node.Name, "container", containerName)

	// Create a timeout context for docker operations
	dockerCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Check if container exists
	// nolint:gosec // G204: Command args are derived from kubernetes API, not user input
	cmd := exec.CommandContext(
		dockerCtx,
		"docker",
		"ps",
		"-a",
		"--filter",
		fmt.Sprintf("label=io.x-k8s.kind.cluster=%s", clusterName),
		"--format",
		"{{.Names}}",
	)

	output, err := cmd.Output()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "", fmt.Errorf("timeout while listing containers: %w", err)
		}

		return "", fmt.Errorf("failed to list containers: %w", err)
	}

	found := slices.Contains(strings.Split(strings.TrimSpace(string(output)), "\n"), containerName)

	if !found {
		slog.InfoContext(ctx, "Container not found, assuming already deleted", "container", containerName)

		return model.TerminateNodeRequestRef(""), nil
	}

	slog.InfoContext(ctx, "Found container, attempting deletion", "container", containerName)

	if err := c.deleteAndVerifyContainer(ctx, dockerCtx, containerName); err != nil {
		return "", err
	}

	slog.InfoContext(ctx, "Successfully deleted container", "container", containerName)

	return model.TerminateNodeRequestRef(""), nil
}

func (c *Client) deleteAndVerifyContainer(
	ctx, dockerCtx context.Context, containerName string,
) error {
	// nolint:gosec // G204: Command args are derived from kubernetes API, not user input
	cmd := exec.CommandContext(dockerCtx, "docker", "rm", "-f", containerName)

	if err := cmd.Run(); err != nil {
		if dockerCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("timeout while deleting container: %w", err)
		}

		return fmt.Errorf("failed to delete container: %w", err)
	}

	// Verify container is actually gone
	// nolint:gosec // G204: Command args are derived from kubernetes API, not user input
	cmd = exec.CommandContext(
		dockerCtx,
		"docker",
		"ps",
		"-a",
		"--filter",
		fmt.Sprintf("name=^%s$", containerName),
		"--format",
		"{{.Names}}",
	)

	output, err := cmd.Output()
	if err != nil {
		if dockerCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("timeout while verifying container deletion: %w", err)
		}

		return fmt.Errorf("failed to verify container deletion: %w", err)
	}

	if slices.Contains(strings.Split(strings.TrimSpace(string(output)), "\n"), containerName) {
		return fmt.Errorf("container %s still exists after deletion attempt", containerName)
	}

	return nil
}
