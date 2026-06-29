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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// YAML fixtures for TestLoad. Kept as named constants (rather than inlined in
// each subtest) so the test bodies read as assertions, not data. Raw string
// literals start at column 0 by necessity — gofmt leaves them untouched.
const (
	yamlMinimal = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
`

	yamlGangEnabled = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
gangCoordination:
  enabled: true
`

	yamlGangCustomValues = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
gangCoordination:
  enabled: true
  timeout: "5m30s"
  masterPort: 29501
  configMapMountPath: "/custom/path"
`

	yamlGangInvalidTimeout = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
gangCoordination:
  enabled: true
  timeout: "not-a-duration"
`

	yamlInvalidSyntax = `{invalid yaml: [`

	yamlExtraHostPathDefault = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
gangCoordination:
  enabled: true
  extraHostPathMounts:
    - name: host-libs
      hostPath: /opt/libs
      mountPath: /opt/libs
`

	yamlPlacementPrepend = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
initContainerPlacement: "prepend"
`

	yamlPlacementInvalid = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
initContainerPlacement: "middle"
`

	yamlEmptyInitContainerName = `
initContainers:
  - name: ""
    image: dcgm:latest
`

	yamlDuplicateInitContainerNames = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
  - name: preflight-dcgm-diag
    image: dcgm:v2
`

	yamlDefaultEnabled = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
  - name: preflight-nccl-allreduce
    image: nccl:latest
    defaultEnabled: false
`

	yamlInheritanceFlags = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
  - name: preflight-nccl-loopback
    image: nccl:latest
    inheritUserEnv: false
    inheritUserVolumeMounts: false
`

	yamlExtraHostPathExplicitFalse = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
gangCoordination:
  enabled: true
  extraHostPathMounts:
    - name: host-libs
      hostPath: /opt/libs
      mountPath: /opt/libs
      readOnly: false
`

	yamlGangDiscoveryOverrides = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
gangCoordination:
  enabled: true
gangDiscovery:
  name: volcano
  annotationKeys: ["scheduling.k8s.io/group-name"]
  podGroupGVR:
    group: scheduling.volcano.sh
    version: v1beta1
    resource: podgroups
  minCountExpr: podGroup.spec.minMember
gangDiscoveryOverrides:
  - namespaces: ["team-a", "team-a-staging"]
    gangDiscovery:
      name: kai
      annotationKeys: ["scheduling.run.ai/pod-group"]
      podGroupGVR:
        group: scheduling.run.ai
        version: v2alpha2
        resource: podgroups
      minCountExpr: podGroup.spec.minMember
  - namespaces: ["team-b"]
    gangDiscovery: {}
`

	yamlOverrideEmptyNamespaceList = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
gangCoordination:
  enabled: true
gangDiscoveryOverrides:
  - namespaces: []
    gangDiscovery: {}
`

	yamlOverrideEmptyNamespaceString = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
gangCoordination:
  enabled: true
gangDiscoveryOverrides:
  - namespaces: [""]
    gangDiscovery: {}
`

	yamlOverrideDuplicateNamespace = `
initContainers:
  - name: preflight-dcgm-diag
    image: dcgm:latest
gangCoordination:
  enabled: true
gangDiscoveryOverrides:
  - namespaces: ["team-a"]
    gangDiscovery: {}
  - namespaces: ["team-a"]
    gangDiscovery: {}
`
)

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

// TestLoad covers YAML parsing, default population (GPU resources,
// gang coordination), validation errors (bad timeout), file errors,
// and extraHostPathMounts readOnly defaulting.
func TestLoad(t *testing.T) {
	t.Run("minimal config with defaults", func(t *testing.T) {
		path := writeYAML(t, yamlMinimal)
		cfg, err := Load(path)
		require.NoError(t, err)

		assert.Equal(t, []string{"nvidia.com/gpu"}, cfg.GPUResourceNames)
		assert.Equal(t, "EXECUTE_REMEDIATION", cfg.ProcessingStrategy)
		assert.Len(t, cfg.InitContainers, 1)
		assert.Equal(t, "preflight-dcgm-diag", cfg.InitContainers[0].Name)
	})

	t.Run("gang enabled defaults", func(t *testing.T) {
		path := writeYAML(t, yamlGangEnabled)
		cfg, err := Load(path)
		require.NoError(t, err)

		assert.True(t, cfg.GangCoordination.Enabled)
		assert.Equal(t, 10*time.Minute, cfg.GangCoordination.TimeoutDuration)
		assert.Equal(t, 29500, cfg.GangCoordination.MasterPort)
		assert.Equal(t, "/etc/preflight", cfg.GangCoordination.ConfigMapMountPath)
		require.NotNil(t, cfg.GangCoordination.MirrorResourceClaims)
		assert.True(t, *cfg.GangCoordination.MirrorResourceClaims)
	})

	t.Run("gang custom values", func(t *testing.T) {
		path := writeYAML(t, yamlGangCustomValues)
		cfg, err := Load(path)
		require.NoError(t, err)

		assert.Equal(t, 5*time.Minute+30*time.Second, cfg.GangCoordination.TimeoutDuration)
		assert.Equal(t, 29501, cfg.GangCoordination.MasterPort)
		assert.Equal(t, "/custom/path", cfg.GangCoordination.ConfigMapMountPath)
	})

	t.Run("gang invalid timeout", func(t *testing.T) {
		path := writeYAML(t, yamlGangInvalidTimeout)
		_, err := Load(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
	})

	t.Run("invalid YAML", func(t *testing.T) {
		path := writeYAML(t, yamlInvalidSyntax)
		_, err := Load(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse")
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := Load("/nonexistent/path/config.yaml")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read")
	})

	t.Run("extra hostPath readOnly defaults to true", func(t *testing.T) {
		path := writeYAML(t, yamlExtraHostPathDefault)
		cfg, err := Load(path)
		require.NoError(t, err)

		require.Len(t, cfg.GangCoordination.ExtraHostPathMounts, 1)
		require.NotNil(t, cfg.GangCoordination.ExtraHostPathMounts[0].ReadOnly)
		assert.True(t, *cfg.GangCoordination.ExtraHostPathMounts[0].ReadOnly)
	})

	t.Run("initContainerPlacement defaults to append", func(t *testing.T) {
		path := writeYAML(t, yamlMinimal)
		cfg, err := Load(path)
		require.NoError(t, err)

		assert.Equal(t, PlacementAppend, cfg.InitContainerPlacement)
	})

	t.Run("initContainerPlacement prepend", func(t *testing.T) {
		path := writeYAML(t, yamlPlacementPrepend)
		cfg, err := Load(path)
		require.NoError(t, err)

		assert.Equal(t, PlacementPrepend, cfg.InitContainerPlacement)
	})

	t.Run("initContainerPlacement invalid value", func(t *testing.T) {
		path := writeYAML(t, yamlPlacementInvalid)
		_, err := Load(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "initContainerPlacement")
	})

	t.Run("empty init container name rejected", func(t *testing.T) {
		path := writeYAML(t, yamlEmptyInitContainerName)
		_, err := Load(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "name must be set")
	})

	t.Run("duplicate init container names rejected", func(t *testing.T) {
		path := writeYAML(t, yamlDuplicateInitContainerNames)
		_, err := Load(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
		assert.Contains(t, err.Error(), "preflight-dcgm-diag")
	})

	t.Run("defaultEnabled parsed from YAML", func(t *testing.T) {
		path := writeYAML(t, yamlDefaultEnabled)
		cfg, err := Load(path)
		require.NoError(t, err)
		require.Len(t, cfg.InitContainers, 2)

		assert.True(t, cfg.InitContainers[0].IsDefaultEnabled(), "nil DefaultEnabled should be true")
		assert.False(t, cfg.InitContainers[1].IsDefaultEnabled(), "explicit false should be false")
	})

	t.Run("inheritance flags parsed from YAML", func(t *testing.T) {
		path := writeYAML(t, yamlInheritanceFlags)
		cfg, err := Load(path)
		require.NoError(t, err)
		require.Len(t, cfg.InitContainers, 2)

		assert.True(t, cfg.InitContainers[0].InheritsUserEnv(), "nil InheritUserEnv should preserve inheritance")
		assert.True(t, cfg.InitContainers[0].InheritsUserVolumeMounts(),
			"nil InheritUserVolumeMounts should preserve inheritance")
		assert.False(t, cfg.InitContainers[1].InheritsUserEnv(), "explicit false should disable env inheritance")
		assert.False(t, cfg.InitContainers[1].InheritsUserVolumeMounts(),
			"explicit false should disable volume mount inheritance")
	})

	t.Run("extra hostPath readOnly explicit false", func(t *testing.T) {
		path := writeYAML(t, yamlExtraHostPathExplicitFalse)
		cfg, err := Load(path)
		require.NoError(t, err)

		require.Len(t, cfg.GangCoordination.ExtraHostPathMounts, 1)
		require.NotNil(t, cfg.GangCoordination.ExtraHostPathMounts[0].ReadOnly)
		assert.False(t, *cfg.GangCoordination.ExtraHostPathMounts[0].ReadOnly)
	})

	t.Run("gangDiscoveryOverrides parsed from YAML", func(t *testing.T) {
		path := writeYAML(t, yamlGangDiscoveryOverrides)
		cfg, err := Load(path)
		require.NoError(t, err)

		require.Len(t, cfg.GangDiscoveryOverrides, 2)
		assert.Equal(t, []string{"team-a", "team-a-staging"}, cfg.GangDiscoveryOverrides[0].Namespaces)
		assert.Equal(t, "kai", cfg.GangDiscoveryOverrides[0].GangDiscovery.Name)
		assert.Equal(t, []string{"team-b"}, cfg.GangDiscoveryOverrides[1].Namespaces)
		assert.Empty(t, cfg.GangDiscoveryOverrides[1].GangDiscovery.Name)
	})

	t.Run("gangDiscoveryOverrides with empty namespace list rejected", func(t *testing.T) {
		path := writeYAML(t, yamlOverrideEmptyNamespaceList)
		_, err := Load(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one namespace")
	})

	t.Run("gangDiscoveryOverrides with empty namespace string rejected", func(t *testing.T) {
		path := writeYAML(t, yamlOverrideEmptyNamespaceString)
		_, err := Load(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty namespace")
	})

	t.Run("gangDiscoveryOverrides duplicate namespace rejected", func(t *testing.T) {
		path := writeYAML(t, yamlOverrideDuplicateNamespace)
		_, err := Load(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "team-a")
		assert.Contains(t, err.Error(), "multiple")
	})
}
