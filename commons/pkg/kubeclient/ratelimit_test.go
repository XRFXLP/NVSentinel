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

package kubeclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func TestRateLimitConfigApply(t *testing.T) {
	config := &rest.Config{}

	err := (RateLimitConfig{QPS: 50, Burst: 100}).Apply(config)

	require.NoError(t, err)
	assert.Equal(t, float32(50), config.QPS)
	assert.Equal(t, 100, config.Burst)
}

func TestRateLimitConfigApplyUsesClientGoDefaultsForZeroValues(t *testing.T) {
	config := &rest.Config{}

	err := (RateLimitConfig{}).Apply(config)

	require.NoError(t, err)
	assert.Equal(t, rest.DefaultQPS, config.QPS)
	assert.Equal(t, rest.DefaultBurst, config.Burst)
}

func TestRateLimitConfigApplyRejectsNegativeValues(t *testing.T) {
	tests := []RateLimitConfig{
		{QPS: -1, Burst: 10},
		{QPS: 5, Burst: -1},
	}

	for _, config := range tests {
		assert.Error(t, config.Apply(&rest.Config{}))
	}
}
