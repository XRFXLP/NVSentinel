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

package helpers

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/e2e-framework/klient"
)

const (
	platformConnectorName  = "platform-connectors"
	simpleHealthClientName = "simple-health-client"
)

type platformConnectorPipelineStage struct {
	Name       string `json:"name"`
	Enabled    bool   `json:"enabled"`
	ConfigPath string `json:"config,omitempty"`
}

// UpsertNodeCondition adds or replaces a node condition through the status subresource.
func UpsertNodeCondition(
	ctx context.Context,
	t *testing.T,
	client klient.Client,
	nodeName string,
	condition v1.NodeCondition,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			node, nodeErr := GetNodeByName(ctx, client, nodeName)
			if nodeErr != nil {
				return nodeErr
			}

			now := metav1.Now()
			if condition.LastHeartbeatTime.IsZero() {
				condition.LastHeartbeatTime = now
			}

			if condition.LastTransitionTime.IsZero() {
				condition.LastTransitionTime = now
			}

			replaced := false

			for i := range node.Status.Conditions {
				if node.Status.Conditions[i].Type == condition.Type {
					node.Status.Conditions[i] = condition
					replaced = true

					break
				}
			}

			if !replaced {
				node.Status.Conditions = append(node.Status.Conditions, condition)
			}

			return client.Resources().UpdateStatus(ctx, node)
		})
		if err != nil {
			t.Logf("Failed to upsert node condition: %v", err)

			return false
		}

		return true
	}, EventuallyWaitTimeout, WaitInterval, "failed to upsert node condition")
}

// EnablePlatformConnectorDedup enables the dedup filter for one E2E scenario and
// returns a ConfigMap backup that must be restored by the caller.
func EnablePlatformConnectorDedup(
	ctx context.Context,
	t *testing.T,
	client klient.Client,
	connectorNodeName string,
	burstWindow string,
	evictionInterval string,
	skipChecks []string,
) []byte {
	t.Helper()

	backupData, err := BackupConfigMap(ctx, client, platformConnectorName, NVSentinelNamespace)
	require.NoError(t, err, "failed to backup platform connector ConfigMap")

	err = setPlatformConnectorDedup(ctx, client, true, burstWindow, evictionInterval, skipChecks)
	require.NoError(t, err, "failed to enable platform connector dedup")

	restartPlatformConnectorOnNode(ctx, t, client, connectorNodeName)

	return backupData
}

func RestorePlatformConnectorConfig(
	ctx context.Context,
	t *testing.T,
	client klient.Client,
	configMapBackup []byte,
	connectorNodeName string,
) {
	t.Helper()

	if configMapBackup == nil {
		t.Log("No platform connector ConfigMap backup to restore")
		return
	}

	err := createConfigMapFromBytes(
		ctx, client, configMapBackup, platformConnectorName, NVSentinelNamespace,
	)
	require.NoError(t, err, "failed to restore platform connector ConfigMap")

	restartPlatformConnectorOnNode(ctx, t, client, connectorNodeName)
}

func PlatformConnectorSenderNode(ctx context.Context, t *testing.T, client klient.Client) string {
	t.Helper()

	pod, err := GetPodOnWorkerNode(ctx, t, client, NVSentinelNamespace, simpleHealthClientName)
	require.NoError(t, err, "failed to find %s pod", simpleHealthClientName)

	return pod.Spec.NodeName
}

func setPlatformConnectorDedup(
	ctx context.Context,
	client klient.Client,
	enabled bool,
	burstWindow string,
	evictionInterval string,
	skipChecks []string,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm := &v1.ConfigMap{}
		if err := client.Resources().Get(ctx, platformConnectorName, NVSentinelNamespace, cm); err != nil {
			return err
		}

		if cm.Data == nil {
			return fmt.Errorf("configmap %s/%s has no data", NVSentinelNamespace, platformConnectorName)
		}

		configJSON, ok := cm.Data["config.json"]
		if !ok {
			return fmt.Errorf("configmap %s/%s missing config.json", NVSentinelNamespace, platformConnectorName)
		}

		updatedConfig, err := setDedupPipelineStage(configJSON, enabled)
		if err != nil {
			return err
		}

		dedupTOML, err := buildDedupTOML(burstWindow, evictionInterval, skipChecks)
		if err != nil {
			return err
		}

		cm.Data["config.json"] = updatedConfig
		cm.Data["dedup.toml"] = dedupTOML

		return client.Resources().Update(ctx, cm)
	})
}

func setDedupPipelineStage(configJSON string, enabled bool) (string, error) {
	var config map[string]json.RawMessage
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return "", fmt.Errorf("failed to unmarshal platform connector config.json: %w", err)
	}

	var pipeline []platformConnectorPipelineStage
	if rawPipeline, ok := config["pipeline"]; ok {
		if err := json.Unmarshal(rawPipeline, &pipeline); err != nil {
			return "", fmt.Errorf("failed to unmarshal platform connector pipeline: %w", err)
		}
	}

	foundDedup := false

	for i := range pipeline {
		if pipeline[i].Name == "Deduplicator" {
			pipeline[i].Enabled = enabled
			pipeline[i].ConfigPath = "/etc/config/dedup.toml"
			foundDedup = true

			break
		}
	}

	if !foundDedup {
		pipeline = append(pipeline, platformConnectorPipelineStage{
			Name:       "Deduplicator",
			Enabled:    enabled,
			ConfigPath: "/etc/config/dedup.toml",
		})
	}

	pipelineJSON, err := json.Marshal(pipeline)
	if err != nil {
		return "", fmt.Errorf("failed to marshal platform connector pipeline: %w", err)
	}

	config["pipeline"] = pipelineJSON

	updated, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal platform connector config.json: %w", err)
	}

	return string(updated), nil
}

func buildDedupTOML(burstWindow string, evictionInterval string, skipChecks []string) (string, error) {
	skipChecksJSON, err := json.Marshal(skipChecks)
	if err != nil {
		return "", fmt.Errorf("failed to marshal dedup skip checks: %w", err)
	}

	return fmt.Sprintf("burstWindow = %q\nevictionInterval = %q\nskipChecks = %s\n",
		burstWindow, evictionInterval, string(skipChecksJSON)), nil
}

func restartPlatformConnectorOnNode(ctx context.Context, t *testing.T, client klient.Client, nodeName string) {
	t.Helper()

	require.NotEmpty(t, nodeName, "platform connector restart node must be set")

	pod, err := GetDaemonSetPodOnWorkerNode(ctx, t, client, platformConnectorName, platformConnectorName, nodeName)
	require.NoError(t, err, "failed to find platform connector pod on node %s", nodeName)

	RestartDaemonSetPodOnNode(
		ctx, t, client, NVSentinelNamespace, platformConnectorName, platformConnectorName, nodeName, pod.Name,
	)
}
