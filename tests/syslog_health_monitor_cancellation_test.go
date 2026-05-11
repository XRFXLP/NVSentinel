//go:build amd64_group
// +build amd64_group

// Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
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

package tests

import (
	"context"
	"strings"
	"testing"

	"tests/helpers"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

const (
	keyCancellationsCMBackup  contextKey = "cancellationsConfigMapBackup"
	keyCancellationsTestNode  contextKey = "cancellationsTestNode"
	keyCancellationsSyslogPod contextKey = "cancellationsSyslogPod"
	keyCancellationsStopChan  contextKey = "cancellationsStopChan"
	keyCancellationsOrigArgs  contextKey = "cancellationsOriginalArgs"
)

// TestSyslogHealthMonitorCancellationRules verifies the per-monitor
// cancellation pipeline end-to-end:
//
//  1. Configure a rule: observing XID 79 cancels XID 119.
//  2. Inject XID 119 on GPU-2 (PCI:0002:00:00) and XID 94 on GPU-2.
//     Both must appear in the SysLogsXIDError node condition.
//  3. Inject XID 79 on GPU-2. The handler emits the original XID 79 fault
//     plus a synthetic healthy XID 119 event in the same gRPC batch. The
//     downstream entity-and-error-code-aware clearer must remove the XID 119
//     entry while leaving the unrelated XID 94 entry on the same GPU intact.
//
// The third step exercises both the "rule fires" path and the "scoped clear
// preserves unrelated faults on the same entity" path.
func TestSyslogHealthMonitorCancellationRules(t *testing.T) {
	feature := features.New("Syslog Health Monitor - Cancellation Rules").
		WithLabel("suite", "syslog-health-monitor").
		WithLabel("component", "cancellation-rules")

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err, "failed to create kubernetes client")

		// Snapshot the unified syslog-health-monitor ConfigMap before
		// mutating its cancellations.toml key; teardown restores it so
		// subsequent tests run against a pristine config.
		t.Logf("Backing up syslog-health-monitor ConfigMap %s/%s",
			helpers.NVSentinelNamespace, helpers.SyslogConfigMapName)
		backup, err := helpers.BackupConfigMap(ctx, client,
			helpers.SyslogConfigMapName, helpers.NVSentinelNamespace)
		require.NoError(t, err, "failed to back up syslog-health-monitor ConfigMap")

		ctx = context.WithValue(ctx, keyCancellationsCMBackup, backup)

		t.Log("Patching cancellations ConfigMap with rule: SysLogsXIDError on XID 79 cancels XID 119")
		require.NoError(t, helpers.SetSyslogCancellationRules(ctx, client,
			[]helpers.SyslogCheckCancellations{
				{
					Name:    "SysLogsXIDError",
					Enabled: true,
					Rules: []helpers.SyslogCancellationRule{
						{OnErrorCode: "79", CancelErrorCodes: []string{"119"}},
					},
				},
			},
		), "failed to update cancellations ConfigMap")

		// SetUpSyslogHealthMonitor restarts the syslog-health-monitor pod
		// after metadata injection; the restarted pod loads the patched
		// cancellation rules from the mounted ConfigMap.
		testNodeName, syslogPod, stopChan, originalArgs := helpers.SetUpSyslogHealthMonitor(ctx, t, client, nil, false)

		ctx = context.WithValue(ctx, keyCancellationsTestNode, testNodeName)
		ctx = context.WithValue(ctx, keyCancellationsSyslogPod, syslogPod.Name)
		ctx = context.WithValue(ctx, keyCancellationsStopChan, stopChan)
		ctx = context.WithValue(ctx, keyCancellationsOrigArgs, originalArgs)

		return ctx
	})

	feature.Assess("Inject XID 119 and XID 94 — both appear in node condition", func(
		ctx context.Context, t *testing.T, c *envconf.Config,
	) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err, "failed to create kubernetes client")

		nodeName := ctx.Value(keyCancellationsTestNode).(string)

		// XID 119 → COMPONENT_RESET (fatal). XID 94 is overridden to fatal
		// CONTACT_SUPPORT in values-tilt.yaml. Both target GPU-2
		// (PCI:0002:00:00 → GPU-22222222-...).
		helpers.InjectSyslogMessages(t, helpers.StubJournalHTTPPort, []string{
			"kernel: [16450076.435595] NVRM: Xid (PCI:0002:00:00): 119, pid=1582259, name=nvc:[driver], Timeout after 6s of waiting for RPC response from GPU1 GSP! Expected function 76 (GSP_RM_CONTROL) (0x20802a02 0x8).",
			"kernel: [16450076.435595] NVRM: Xid (PCI:0002:00:00): 94, pid=789012, name=process, Contained ECC error.",
		})

		expectedSequencePatterns := []string{
			`ErrorCode:119 PCI:0002:00:00 GPU_UUID:GPU-22222222-2222-2222-2222-222222222222 kernel:.*?NVRM: Xid \(PCI:0002:00:00\): 119.*?Recommended Action=COMPONENT_RESET`,
			`ErrorCode:94 PCI:0002:00:00 GPU_UUID:GPU-22222222-2222-2222-2222-222222222222 kernel:.*?NVRM: Xid \(PCI:0002:00:00\): 94.*?Recommended Action=CONTACT_SUPPORT`,
		}

		t.Log("Verifying node condition contains both XID 119 and XID 94 on GPU-2")
		require.Eventually(t, func() bool {
			return helpers.VerifyNodeConditionMatchesSequence(t, ctx, client, nodeName,
				"SysLogsXIDError", "SysLogsXIDErrorIsNotHealthy", expectedSequencePatterns)
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval,
			"Node condition should contain both XID 119 and XID 94 entries on GPU-2")

		return ctx
	})

	feature.Assess("Inject XID 79 — synthetic cancellation clears XID 119 only", func(
		ctx context.Context, t *testing.T, c *envconf.Config,
	) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err, "failed to create kubernetes client")

		nodeName := ctx.Value(keyCancellationsTestNode).(string)

		// XID 79 itself is fatal (CONTACT_SUPPORT) so the source event will
		// also surface as a new entry on GPU-2; the synthetic healthy event
		// fans out alongside it in the same gRPC batch and clears XID 119.
		helpers.InjectSyslogMessages(t, helpers.StubJournalHTTPPort, []string{
			"kernel: [16450076.435595] NVRM: Xid (PCI:0002:00:00): 79, pid=123456, name=test-cancel, GPU has fallen off the bus.",
		})

		t.Log("Verifying that XID 119 is cleared but XID 94 (unrelated, same entity) remains")
		require.Eventually(t, func() bool {
			condition, err := helpers.CheckNodeConditionExists(ctx, client, nodeName,
				"SysLogsXIDError", "SysLogsXIDErrorIsNotHealthy")
			if err != nil || condition == nil {
				t.Logf("Condition not found yet: %v", err)
				return false
			}

			msg := condition.Message

			// New fault from XID 79 is expected (it triggered the
			// cancellation but is itself an unhealthy observation).
			if !strings.Contains(msg, "ErrorCode:79") {
				t.Logf("Waiting for XID 79 source event to appear: %s", msg)
				return false
			}

			// XID 94 must remain — the cancellation rule targeted XID 119
			// only; the entity-and-error-code-aware clearer must leave
			// unrelated codes on the same entity intact.
			if !strings.Contains(msg, "ErrorCode:94") {
				t.Logf("FAIL: XID 94 was incorrectly cleared by XID-119-scoped cancellation: %s", msg)
				return false
			}

			// XID 119 must be gone — the synthetic healthy XID 119 event
			// emitted alongside XID 79 should have removed it.
			if entryCount(msg, "ErrorCode:119") != 0 {
				t.Logf("Waiting for synthetic cancellation of XID 119 to take effect: %s", msg)
				return false
			}

			t.Logf("PASS: XID 119 cleared by cancellation, XID 94 preserved, XID 79 surfaced: %s", msg)

			return true
		}, helpers.EventuallyWaitTimeout, helpers.WaitInterval,
			"Synthetic cancellation should clear XID 119 while leaving XID 94 on the same GPU intact")

		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err, "failed to create kubernetes client")

		nodeName := ctx.Value(keyCancellationsTestNode).(string)
		syslogPod := ctx.Value(keyCancellationsSyslogPod).(string)
		stopChan := ctx.Value(keyCancellationsStopChan).(chan struct{})
		originalArgs, _ := ctx.Value(keyCancellationsOrigArgs).([]string)

		// Restore the original cancellations ConfigMap so subsequent tests
		// see no rules. The pod restart in TearDownSyslogHealthMonitor will
		// then reload the empty config.
		if backup, ok := ctx.Value(keyCancellationsCMBackup).([]byte); ok && backup != nil {
			t.Logf("Restoring original syslog-health-monitor ConfigMap")

			// Mark the test failed (but keep going so the rest of teardown
			// still runs) when we fail to restore the ConfigMap. A leaked
			// cancellation rule would silently affect every subsequent test
			// in the same run, so the failure must be visible.
			if restoreErr := helpers.ReplaceConfigMapFromBackup(
				ctx, client, backup,
				helpers.SyslogConfigMapName, helpers.NVSentinelNamespace,
			); restoreErr != nil {
				t.Errorf("failed to restore syslog-health-monitor ConfigMap: %v", restoreErr)
			}
		}

		helpers.TearDownSyslogHealthMonitor(ctx, t, client, nodeName, stopChan, originalArgs, syslogPod)

		return ctx
	})

	testEnv.Test(t, feature.Feature())
}

// entryCount returns how many ";"-separated condition message entries contain
// the given substring. Used to assert that a particular ErrorCode token has
// been removed entirely from the node condition.
func entryCount(message, substr string) int {
	count := 0

	for _, part := range strings.Split(message, ";") {
		if strings.Contains(part, substr) {
			count++
		}
	}

	return count
}
