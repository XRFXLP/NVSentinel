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
	"testing"
	"tests/helpers"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestMultipleRemediationsCompleted(t *testing.T) {
	feature := features.New("TestMultipleRemediationsCompleted").
		WithLabel("suite", "health-event-analyzer")

	var testCtx *helpers.HealthEventsAnalyzerTestContext

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		var newCtx context.Context
		newCtx, testCtx = helpers.SetupHealthEventsAnalyzerTest(ctx, t, c, "data/health-events-analyzer-config.yaml", "health-events-analyzer-test")

		client, err := c.NewClient()
		require.NoError(t, err)
		helpers.TriggerMultipleRemediationsCycle(ctx, t, client, testCtx.NodeName)

		return newCtx
	})

	feature.Assess("Check if MultipleRemediations node condition is added", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		client, err := c.NewClient()
		assert.NoError(t, err, "failed to create client")
		gpuNodeName := testCtx.NodeName

		event := helpers.NewHealthEvent(gpuNodeName).
			WithErrorCode(helpers.ERRORCODE_31).
			WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM))
		helpers.SendHealthEvent(ctx, t, event)

		helpers.WaitForNodeConditionWithCheckName(ctx, t, client, gpuNodeName, "MultipleRemediations", "ErrorCode:31 GPU:0 Recommended Action=CONTACT_SUPPORT;")

		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		return helpers.TeardownHealthEventsAnalyzer(ctx, t, c, testCtx.NodeName, testCtx.ConfigMapBackup)
	})

	testEnv.Test(t, feature.Feature())
}

func TestMultipleRemediationsNotTriggered(t *testing.T) {
	feature := features.New("TestMultipleRemediationsNotTriggered").
		WithLabel("suite", "health-event-analyzer")

	var testCtx *helpers.HealthEventsAnalyzerTestContext

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		var newCtx context.Context
		newCtx, testCtx = helpers.SetupHealthEventsAnalyzerTest(ctx, t, c, "data/health-events-analyzer-config.yaml", "health-events-analyzer-test")

		gpuNodeName := testCtx.NodeName

		t.Logf("Injecting non-fatal events to node %s", gpuNodeName)
		for i := 0; i < 5; i++ {
			event := helpers.NewHealthEvent(gpuNodeName).
				WithFatal(false).
				WithErrorCode(helpers.ERRORCODE_13).
				WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM))

			helpers.SendHealthEvent(ctx, t, event)

			helpers.SendHealthyEvent(ctx, t, gpuNodeName)
		}

		return newCtx
	})

	feature.Assess("Check if MultipleRemediations node condition is NOT added for non-fatal events", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		gpuNodeName := testCtx.NodeName

		client, err := c.NewClient()
		assert.NoError(t, err, "failed to create client")

		event := helpers.NewHealthEvent(gpuNodeName).
			WithFatal(false).
			WithErrorCode(helpers.ERRORCODE_13).
			WithRecommendedAction(int(pb.RecommendedAction_RESTART_VM))
		helpers.SendHealthEvent(ctx, t, event)

		helpers.EnsureNodeConditionNotPresent(ctx, t, client, gpuNodeName, "MultipleRemediations")

		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		return helpers.TeardownHealthEventsAnalyzer(ctx, t, c, testCtx.NodeName, testCtx.ConfigMapBackup)
	})

	testEnv.Test(t, feature.Feature())
}
