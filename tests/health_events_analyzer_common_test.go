//go:build amd64_group
// +build amd64_group

// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"tests/helpers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

// Tests in this file exercise health-events-analyzer rules that are known to
// be backed by database-agnostic logic on the PostgreSQL path:
//
//   - TestRepeatedXIDOnSameGPU — uses the Go-based XidBurstDetector on
//     PostgreSQL (equivalent to the MongoDB $setWindowFields pipeline).
//     This is the primary regression gate for
//     https://github.com/NVIDIA/NVSentinel/issues/1191.
//   - TestSoloNoBurstRule — the XIDErrorSoloNoBurst rule uses only $match
//     expressions that the PostgreSQL aggregation translator supports.
//   - TestHealthEventsAnalyzerStoreOnlyStrategy — exercises the reconciler's
//     STORE_ONLY processing-strategy path, independent of the rule body.
//
// These tests intentionally omit the "mongodb" build tag and are expected to
// pass against both backends.
//
// The counterpart file health_events_analyzer_test.go holds tests whose rules
// rely on MongoDB-only pipeline features (complex $addFields with
// $arrayToObject/$map/$filter, bitmask expressions, self-referencing
// expressions against JSONB fields, etc.) and remain gated behind the
// "mongodb" build tag until those rules are ported to a database-agnostic
// evaluation framework (tracking issue:
// https://github.com/NVIDIA/NVSentinel/issues/606).

const (
	keyOriginalArgsContextKey contextKey = "originalArgs"
)

func TestRepeatedXIDOnSameGPU(t *testing.T) {
	// Works with both MongoDB ($setWindowFields pipeline) and PostgreSQL (XidBurstDetector).
	// This test is the primary regression gate for the per-GPU XID burst
	// detector behavior (see https://github.com/NVIDIA/NVSentinel/issues/1191).
	feature := features.New("TestRepeatedXIDOnSameGPU").
		WithLabel("suite", "health-event-analyzer")

	var testCtx *helpers.HealthEventsAnalyzerTestContext
	var testNodeName string
	var entitiesImpacted [][]helpers.EntityImpacted

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		testNodeName = helpers.AcquireNodeFromPool(ctx, t, client, helpers.DefaultExpiry)

		var newCtx context.Context
		newCtx, testCtx = helpers.SetupHealthEventsAnalyzerTest(ctx, t, c, "", "health-events-analyzer-test", testNodeName)

		return newCtx
	})

	feature.Assess("Inject multiple XID errors and check if node condition is added if required", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		assert.NoError(t, err, "failed to create client")

		entities := []helpers.EntityImpacted{
			{
				EntityType:  "PCI",
				EntityValue: "0001:00:00",
			},
			{
				EntityType:  "GPU_UUID",
				EntityValue: "GPU-11111111-1111-1111-1111-111111111111",
			},
		}

		entitiesImpacted = append(entitiesImpacted, entities)

		// Burst 1: 5 events within 10s gaps (same burst)
		// Burst 1 contents: XID 119 (x2), 120, 48, 31
		// Expectations: No trigger yet (need at least 2 bursts to trigger)
		errorCodes := []string{helpers.ERRORCODE_119, helpers.ERRORCODE_120, helpers.ERRORCODE_48, helpers.ERRORCODE_119, helpers.ERRORCODE_31}
		for _, errorCode := range errorCodes {
			helpers.SendHealthEvent(ctx, t, helpers.NewHealthEvent(testNodeName).
				WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
				WithCheckName("SysLogsXIDError").
				WithEntitiesImpacted(entities).
				WithFatal(true).
				WithErrorCode(errorCode).
				WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM)),
			)
		}

		helpers.EnsureNodeConditionNotPresent(ctx, t, client, testNodeName, "RepeatedXIDErrorOnSameGPU")

		t.Log("Waiting 22s to create burst gap (>20s required)")
		time.Sleep(22 * time.Second)

		// Burst 2: XID 120 (non-sticky) creates new burst after 22s gap
		// Burst 2 initial contents: XID 120, 79
		// Expectations: XID 120 triggers (appears in Burst 1 and Burst 2)
		errorCodes = []string{helpers.ERRORCODE_120, helpers.ERRORCODE_79}
		for _, errorCode := range errorCodes {
			helpers.SendHealthEvent(ctx, t, helpers.NewHealthEvent(testNodeName).
				WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
				WithCheckName("SysLogsXIDError").
				WithEntitiesImpacted(entities).
				WithFatal(true).
				WithErrorCode(errorCode).
				WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM)),
			)
		}

		message := fmt.Sprintf("ErrorCode:%s PCI:0001:00:00 GPU_UUID:GPU-11111111-1111-1111-1111-111111111111 Recommended Action=CONTACT_SUPPORT;", helpers.ERRORCODE_120)
		helpers.WaitForNodeConditionWithCheckName(ctx, t, client, testNodeName, "RepeatedXIDErrorOnSameGPU",
			message, "RepeatedXIDErrorOnSameGPUIsNotHealthy", v1.ConditionTrue)

		t.Log("Waiting 22s to create burst gap (>20s required)")
		time.Sleep(22 * time.Second)

		// Burst 2 (continued): XID 119 (sticky) arrives but merges into existing Burst 2
		// because XID 79 (sticky) occurred 22s ago (within 30s sticky window)
		// Burst 2 final contents: XID 120, 79, 119, 48
		// Expectations: 119 and 48 trigger (both appear in Burst 1 and Burst 2)
		errorCodes = []string{helpers.ERRORCODE_119, helpers.ERRORCODE_48}
		for _, errorCode := range errorCodes {
			helpers.SendHealthEvent(ctx, t, helpers.NewHealthEvent(testNodeName).
				WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
				WithCheckName("SysLogsXIDError").
				WithEntitiesImpacted(entities).
				WithFatal(true).
				WithErrorCode(errorCode).
				WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM)),
			)
		}

		t.Logf("Verifying RepeatedXIDErrorOnSameGPU condition exists after events merged into Burst 2")
		message += fmt.Sprintf("ErrorCode:%s PCI:0001:00:00 GPU_UUID:GPU-11111111-1111-1111-1111-111111111111 Recommended Action=CONTACT_SUPPORT;", helpers.ERRORCODE_119)
		message += fmt.Sprintf("ErrorCode:%s PCI:0001:00:00 GPU_UUID:GPU-11111111-1111-1111-1111-111111111111 Recommended Action=CONTACT_SUPPORT;", helpers.ERRORCODE_48)
		helpers.WaitForNodeConditionWithCheckName(ctx, t, client, testNodeName, "RepeatedXIDErrorOnSameGPU",
			message, "RepeatedXIDErrorOnSameGPUIsNotHealthy", v1.ConditionTrue)

		t.Log("Waiting 22s to create burst gap (>20s required)")
		time.Sleep(22 * time.Second)

		// Burst 3: XID 13 (non-sticky) creates new burst after 16s gap
		// Burst 3 contents: XID 13, 31
		// Expectations: XID 31 triggers (appears in Burst 1 and Burst 3)
		errorCodes = []string{helpers.ERRORCODE_13, helpers.ERRORCODE_31}
		for _, errorCode := range errorCodes {
			helpers.SendHealthEvent(ctx, t, helpers.NewHealthEvent(testNodeName).
				WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
				WithCheckName("SysLogsXIDError").
				WithEntitiesImpacted(entities).
				WithFatal(true).
				WithErrorCode(errorCode).
				WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM)),
			)
		}

		time.Sleep(5 * time.Second)

		// Burst 3 (continued): XID 13 arrives again after 5s gap (< 20s), stays in same burst
		// Burst 3 final contents: XID 13 (x2), 31 (x1)
		// Expectations: XID 13 will NOT trigger (only appears in Burst 3, and targetXidCount=2 in maxBurst),
		// 				 XID 31 will also not trigger as we are excluding XID 31 from RepeatedXIDErrorOnSameGPU rule
		helpers.SendHealthEvent(ctx, t, helpers.NewHealthEvent(testNodeName).
			WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
			WithCheckName("SysLogsXIDError").
			WithEntitiesImpacted(entities).
			WithFatal(true).
			WithErrorCode(helpers.ERRORCODE_13).
			WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM)))

		helpers.WaitForNodeConditionWithCheckName(ctx, t, client, testNodeName, "RepeatedXIDErrorOnSameGPU",
			message, "RepeatedXIDErrorOnSameGPUIsNotHealthy", v1.ConditionTrue)

		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		for _, entities := range entitiesImpacted {
			syslogHealthEvent := helpers.NewHealthEvent(testNodeName).
				WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
				WithEntitiesImpacted(entities).
				WithCheckName("SysLogsXIDError").
				WithFatal(false).
				WithHealthy(true).
				WithMessage("No health failures").
				WithComponentClass("GPU")

			helpers.SendHealthEvent(ctx, t, syslogHealthEvent)
		}

		return helpers.TeardownHealthEventsAnalyzer(ctx, t, c, testNodeName, testCtx.ConfigMapBackup)
	})

	testEnv.Test(t, feature.Feature())
}

func TestSoloNoBurstRule(t *testing.T) {
	feature := features.New("TestSoloNoBurstRule").
		WithLabel("suite", "health-event-analyzer")

	var testCtx *helpers.HealthEventsAnalyzerTestContext
	var testNodeName string
	var entitiesImpacted [][]helpers.EntityImpacted

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		testNodeName = helpers.AcquireNodeFromPool(ctx, t, client, helpers.DefaultExpiry)

		ctx, testCtx = helpers.SetupHealthEventsAnalyzerTest(ctx, t, c, "", "health-events-analyzer-test", testNodeName)
		t.Logf("Using node: %s", testNodeName)

		entities1 := []helpers.EntityImpacted{
			{
				EntityType:  "PCI",
				EntityValue: "0001:00:00",
			},
			{
				EntityType:  "GPU_UUID",
				EntityValue: "GPU-11111111-1111-1111-1111-111111111111",
			},
			{
				EntityType:  "GPC",
				EntityValue: "0",
			},
			{
				EntityType:  "TPC",
				EntityValue: "1",
			},
			{
				EntityType:  "SM",
				EntityValue: "2",
			},
		}

		entities2 := []helpers.EntityImpacted{
			{
				EntityType:  "PCI",
				EntityValue: "0002:00:00",
			},
			{
				EntityType:  "GPU_UUID",
				EntityValue: "GPU-22222222-2222-2222-2222-222222222222",
			},
		}

		entitiesImpacted = append(entitiesImpacted, entities1)
		entitiesImpacted = append(entitiesImpacted, entities2)

		errorCodes := []string{helpers.ERRORCODE_13, helpers.ERRORCODE_13}
		for _, errorCode := range errorCodes {
			helpers.SendHealthEvent(ctx, t, helpers.NewHealthEvent(testNodeName).
				WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
				WithCheckName("SysLogsXIDError").
				WithEntitiesImpacted(entities1).
				WithFatal(true).
				WithErrorCode(errorCode).
				WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM)),
			)
		}

		t.Log("Waiting 5s to create burst gap")
		time.Sleep(5 * time.Second)

		helpers.SendHealthEvent(ctx, t, helpers.NewHealthEvent(testNodeName).
			WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
			WithCheckName("SysLogsXIDError").
			WithEntitiesImpacted(entities2).
			WithFatal(true).
			WithErrorCode(helpers.ERRORCODE_13).
			WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM)))

		return ctx
	})

	feature.Assess("Check if XIDErrorSoloNoBurst node condition is added", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		expectedEvent := v1.Event{
			Type:    "XIDErrorSoloNoBurst",
			Reason:  "XIDErrorSoloNoBurstIsNotHealthy",
			Message: "ErrorCode:13 PCI:0002:00:00 GPU_UUID:GPU-22222222-2222-2222-2222-222222222222 App passing bad data or using incorrect GPU methods. check error PID to identify source of the problem, if application is known good and problem persists, then contact support Recommended Action=NONE;",
		}

		helpers.WaitForNodeEvent(ctx, t, client, testNodeName, expectedEvent)

		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		for _, entities := range entitiesImpacted {
			syslogHealthEvent := helpers.NewHealthEvent(testNodeName).
				WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
				WithEntitiesImpacted(entities).
				WithCheckName("SysLogsXIDError").
				WithFatal(false).
				WithHealthy(true).
				WithMessage("No health failures").
				WithComponentClass("GPU")
			helpers.SendHealthEvent(ctx, t, syslogHealthEvent)
		}

		return helpers.TeardownHealthEventsAnalyzer(ctx, t, c, testNodeName, testCtx.ConfigMapBackup)
	})

	testEnv.Test(t, feature.Feature())
}

func TestHealthEventsAnalyzerStoreOnlyStrategy(t *testing.T) {
	feature := features.New("TestHealthEventsAnalyzerStoreOnlyStrategy").
		WithLabel("suite", "health-event-analyzer")

	var testCtx *helpers.HealthEventsAnalyzerTestContext
	var testNodeName string
	var originalArgs []string

	var entitiesImpacted [][]helpers.EntityImpacted

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		originalArgs, err = helpers.SetDeploymentArgs(ctx, t, client, helpers.HEALTH_EVENTS_ANALYZER_DEPLOYMENT_NAME, helpers.NVSentinelNamespace, helpers.HEALTH_EVENTS_ANALYZER_CONTAINER_NAME, map[string]string{
			"--processing-strategy": "STORE_ONLY",
		})
		require.NoError(t, err)

		ctx = context.WithValue(ctx, keyOriginalArgsContextKey, originalArgs)

		helpers.WaitForDeploymentRollout(ctx, t, client, helpers.HEALTH_EVENTS_ANALYZER_DEPLOYMENT_NAME, helpers.NVSentinelNamespace)

		testNodeName = helpers.AcquireNodeFromPool(ctx, t, client, helpers.DefaultExpiry)

		ctx, testCtx = helpers.SetupHealthEventsAnalyzerTest(ctx, t, c, "", "health-events-analyzer-test", testNodeName)
		testNodeName = testCtx.NodeName
		t.Logf("Using node: %s", testNodeName)

		t.Log("Injecting XID error 120(x2) on same GPU. The node condition should not be added as the processing strategy is STORE_ONLY.")

		entities := []helpers.EntityImpacted{
			{
				EntityType:  "PCI",
				EntityValue: "0001:00:00",
			},
			{
				EntityType:  "GPU_UUID",
				EntityValue: "GPU-11111111-1111-1111-1111-111111111111",
			},
		}

		entitiesImpacted = append(entitiesImpacted, entities)

		errorCodes := []string{helpers.ERRORCODE_120}
		for _, errorCode := range errorCodes {
			helpers.SendHealthEvent(ctx, t, helpers.NewHealthEvent(testNodeName).
				WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
				WithCheckName("SysLogsXIDError").
				WithEntitiesImpacted(entities).
				WithFatal(true).
				WithErrorCode(errorCode).
				WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM)),
			)
		}

		t.Log("Waiting 17s to create burst gap (>15s required)")
		time.Sleep(17 * time.Second)

		errorCodes = []string{helpers.ERRORCODE_120}
		for _, errorCode := range errorCodes {
			helpers.SendHealthEvent(ctx, t, helpers.NewHealthEvent(testNodeName).
				WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
				WithCheckName("SysLogsXIDError").
				WithEntitiesImpacted(entities).
				WithFatal(true).
				WithErrorCode(errorCode).
				WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM)),
			)
		}

		return ctx
	})

	feature.Assess("Verify node condition is not added and node is not cordoned when processing STORE_ONLY events", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		t.Log("Verifying no node condition is created when processing STORE_ONLY strategy")
		helpers.EnsureNodeConditionNotPresent(ctx, t, client, testNodeName, "RepeatedXIDErrorOnSameGPU")

		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		require.NoError(t, err)

		originalArgs := ctx.Value(keyOriginalArgsContextKey).([]string)

		for _, entities := range entitiesImpacted {
			syslogHealthEvent := helpers.NewHealthEvent(testNodeName).
				WithAgent(helpers.SYSLOG_HEALTH_MONITOR_AGENT).
				WithEntitiesImpacted(entities).
				WithCheckName("SysLogsXIDError").
				WithFatal(false).
				WithHealthy(true).
				WithMessage("No health failures").
				WithComponentClass("GPU")
			helpers.SendHealthEvent(ctx, t, syslogHealthEvent)
		}

		err = helpers.RestoreDeploymentArgs(t, ctx, client, helpers.HEALTH_EVENTS_ANALYZER_DEPLOYMENT_NAME, helpers.NVSentinelNamespace, helpers.HEALTH_EVENTS_ANALYZER_CONTAINER_NAME, originalArgs)
		require.NoError(t, err)

		helpers.WaitForDeploymentRollout(ctx, t, client, helpers.HEALTH_EVENTS_ANALYZER_DEPLOYMENT_NAME, helpers.NVSentinelNamespace)

		return helpers.TeardownHealthEventsAnalyzer(ctx, t, c, testNodeName, testCtx.ConfigMapBackup)
	})

	testEnv.Test(t, feature.Feature())
}
