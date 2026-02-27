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

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePositiveFloat(t *testing.T) {
	t.Run("default when unset", func(t *testing.T) {
		t.Setenv("TEST_FLOAT", "")
		v, err := parsePositiveFloat("TEST_FLOAT", 150.0)
		require.NoError(t, err)
		assert.Equal(t, 150.0, v)
	})

	t.Run("parses valid float", func(t *testing.T) {
		t.Setenv("TEST_FLOAT", "42.5")
		v, err := parsePositiveFloat("TEST_FLOAT", 0)
		require.NoError(t, err)
		assert.Equal(t, 42.5, v)
	})

	t.Run("rejects non-positive", func(t *testing.T) {
		t.Setenv("TEST_FLOAT", "-1.0")
		_, err := parsePositiveFloat("TEST_FLOAT", 0)
		require.Error(t, err)
	})

	t.Run("rejects invalid", func(t *testing.T) {
		t.Setenv("TEST_FLOAT", "abc")
		_, err := parsePositiveFloat("TEST_FLOAT", 0)
		require.Error(t, err)
	})
}

func TestParsePositiveInt(t *testing.T) {
	t.Run("default when unset", func(t *testing.T) {
		t.Setenv("TEST_INT", "")
		v, err := parsePositiveInt("TEST_INT", 256)
		require.NoError(t, err)
		assert.Equal(t, 256, v)
	})

	t.Run("parses valid int", func(t *testing.T) {
		t.Setenv("TEST_INT", "128")
		v, err := parsePositiveInt("TEST_INT", 0)
		require.NoError(t, err)
		assert.Equal(t, 128, v)
	})

	t.Run("rejects non-positive", func(t *testing.T) {
		t.Setenv("TEST_INT", "0")
		_, err := parsePositiveInt("TEST_INT", 0)
		require.Error(t, err)
	})
}

func TestParseBool(t *testing.T) {
	t.Run("default when unset", func(t *testing.T) {
		t.Setenv("TEST_BOOL", "")
		assert.False(t, parseBool("TEST_BOOL", false))
	})

	t.Run("parses true", func(t *testing.T) {
		t.Setenv("TEST_BOOL", "true")
		assert.True(t, parseBool("TEST_BOOL", false))
	})

	t.Run("invalid returns default", func(t *testing.T) {
		t.Setenv("TEST_BOOL", "not-a-bool")
		assert.True(t, parseBool("TEST_BOOL", true))
	})
}

func TestRequireEnv(t *testing.T) {
	t.Run("returns value when set", func(t *testing.T) {
		t.Setenv("TEST_REQ", "value")
		v, err := requireEnv("TEST_REQ")
		require.NoError(t, err)
		assert.Equal(t, "value", v)
	})

	t.Run("error when empty", func(t *testing.T) {
		t.Setenv("TEST_REQ", "")
		_, err := requireEnv("TEST_REQ")
		require.Error(t, err)
	})
}

func TestValidateExecutable(t *testing.T) {
	t.Run("valid executable", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "binary")
		require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755))

		assert.NoError(t, validateExecutable(path))
	})

	t.Run("missing file", func(t *testing.T) {
		assert.Error(t, validateExecutable("/nonexistent/binary"))
	})

	t.Run("directory not executable", func(t *testing.T) {
		dir := t.TempDir()
		assert.Error(t, validateExecutable(dir))
	})

	t.Run("non-executable file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "noexec")
		require.NoError(t, os.WriteFile(path, []byte("data"), 0o644))

		assert.Error(t, validateExecutable(path))
	})
}

func TestParseProcessingStrategy(t *testing.T) {
	t.Run("default strategy", func(t *testing.T) {
		t.Setenv("PROCESSING_STRATEGY", "")
		s, err := parseProcessingStrategy()
		require.NoError(t, err)
		assert.NotZero(t, s)
	})

	t.Run("explicit strategy", func(t *testing.T) {
		t.Setenv("PROCESSING_STRATEGY", "EXECUTE_REMEDIATION")
		s, err := parseProcessingStrategy()
		require.NoError(t, err)
		assert.NotZero(t, s)
	})

	t.Run("invalid strategy", func(t *testing.T) {
		t.Setenv("PROCESSING_STRATEGY", "INVALID_STRATEGY")
		_, err := parseProcessingStrategy()
		require.Error(t, err)
	})
}
