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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func findVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for index := range volumes {
		if volumes[index].Name == name {
			return &volumes[index]
		}
	}

	return nil
}

func findVolumeMount(mounts []corev1.VolumeMount, name string) *corev1.VolumeMount {
	for index := range mounts {
		if mounts[index].Name == name {
			return &mounts[index]
		}
	}

	return nil
}

func findEnvVar(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for index := range env {
		if env[index].Name == name {
			return &env[index]
		}
	}

	return nil
}

func TestGetDefaultGPUResetJobTemplate_DriverRoot_SetsEnvAndMounts(t *testing.T) {
	tests := []struct {
		name                 string
		driverRoot           string
		expectedDriverRoot   string
		expectDriverRootVol  bool
		expectedSysMountPath string
	}{
		{
			name:                 "empty driverRoot falls back to the containerized driver default",
			driverRoot:           "",
			expectedDriverRoot:   DriverRootMountPath,
			expectDriverRootVol:  true,
			expectedSysMountPath: DriverRootMountPath + HostSysPath,
		},
		{
			name:                 "custom driverRoot moves the driver and sysfs mounts",
			driverRoot:           "/opt/nvidia/driver",
			expectedDriverRoot:   "/opt/nvidia/driver",
			expectDriverRootVol:  true,
			expectedSysMountPath: "/opt/nvidia/driver/sys",
		},
		{
			name:                 "root driverRoot drops the driver volume and mounts sysfs at /sys",
			driverRoot:           ContainerDriverRoot,
			expectedDriverRoot:   ContainerDriverRoot,
			expectDriverRootVol:  false,
			expectedSysMountPath: HostSysPath,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			template, err := getDefaultGPUResetJobTemplate(testNamespace, "alpine:latest", nil,
				ResourceRequirements{}, DefaultHostDriverRootPath, test.driverRoot, "", true, "")
			require.NoError(t, err)

			podSpec := template.Spec.Template.Spec
			container := podSpec.Containers[0]

			driverRootEnv := findEnvVar(container.Env, "DRIVER_ROOT")
			require.NotNil(t, driverRootEnv)
			assert.Equal(t, test.expectedDriverRoot, driverRootEnv.Value)

			driverRootVolume := findVolume(podSpec.Volumes, DriverRootVolumeName)
			driverRootMount := findVolumeMount(container.VolumeMounts, DriverRootVolumeName)

			if test.expectDriverRootVol {
				require.NotNil(t, driverRootVolume)
				require.NotNil(t, driverRootVolume.HostPath)
				assert.Equal(t, DefaultHostDriverRootPath, driverRootVolume.HostPath.Path)
				require.NotNil(t, driverRootMount)
				assert.Equal(t, test.expectedDriverRoot, driverRootMount.MountPath)
			} else {
				assert.Nil(t, driverRootVolume)
				assert.Nil(t, driverRootMount)
			}

			sysMount := findVolumeMount(container.VolumeMounts, HostSysVolumeName)
			require.NotNil(t, sysMount)
			assert.Equal(t, test.expectedSysMountPath, sysMount.MountPath)

			// /dev is mounted at /dev wherever the chroot points.
			devMount := findVolumeMount(container.VolumeMounts, HostDevVolumeName)
			require.NotNil(t, devMount)
			assert.Equal(t, HostDevPath, devMount.MountPath)
		})
	}
}

func TestGetDefaultGPUResetJobTemplate_InvalidDriverRoot_ReturnsError(t *testing.T) {
	tests := []struct {
		name       string
		driverRoot string
	}{
		{name: "relative path", driverRoot: "run/nvidia/driver"},
		{name: "double slash", driverRoot: "//"},
		{name: "trailing slash", driverRoot: "/run/nvidia/driver/"},
		{name: "parent traversal", driverRoot: "/run/nvidia/../nvidia/driver"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			template, err := getDefaultGPUResetJobTemplate(testNamespace, "alpine:latest", nil,
				ResourceRequirements{}, DefaultHostDriverRootPath, test.driverRoot, "", true, "")
			require.Error(t, err)
			assert.Nil(t, template)
			assert.Contains(t, err.Error(), "resetJob.driverRoot")
		})
	}
}

func TestApplyDriverSearchPaths_SetsOnlyConfiguredEnv(t *testing.T) {
	tests := []struct {
		name          string
		driverBinPath string
		driverLibPath string
	}{
		{name: "unset paths leave the Job unchanged"},
		{
			name:          "host-installed GKE driver sets both paths",
			driverBinPath: "/home/kubernetes/bin/nvidia/bin",
			driverLibPath: "/home/kubernetes/bin/nvidia/lib64",
		},
		{name: "bin path only", driverBinPath: "/opt/nvidia/bin"},
		{name: "lib path only", driverLibPath: "/opt/nvidia/lib64"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			template, err := getDefaultGPUResetJobTemplate(testNamespace, "alpine:latest", nil,
				ResourceRequirements{}, "/", DriverRootMountPath, "", true, "")
			require.NoError(t, err)

			envCountBefore := len(template.Spec.Template.Spec.Containers[0].Env)

			require.NoError(t, applyDriverSearchPaths(template, test.driverBinPath, test.driverLibPath))

			env := template.Spec.Template.Spec.Containers[0].Env
			for _, expected := range []struct{ name, value string }{
				{name: DriverBinPathEnvVar, value: test.driverBinPath},
				{name: DriverLibPathEnvVar, value: test.driverLibPath},
			} {
				envVar := findEnvVar(env, expected.name)
				if expected.value == "" {
					assert.Nil(t, envVar)

					continue
				}

				require.NotNil(t, envVar)
				assert.Equal(t, expected.value, envVar.Value)
			}

			expectedAdded := 0
			if test.driverBinPath != "" {
				expectedAdded++
			}

			if test.driverLibPath != "" {
				expectedAdded++
			}

			assert.Len(t, env, envCountBefore+expectedAdded)
		})
	}
}

func TestApplyDriverSearchPaths_InvalidPath_ReturnsError(t *testing.T) {
	tests := []struct {
		name          string
		driverBinPath string
		driverLibPath string
		expectedKey   string
	}{
		{name: "relative bin path", driverBinPath: "home/kubernetes/bin/nvidia/bin", expectedKey: "resetJob.driverBinPath"},
		{name: "trailing slash bin path", driverBinPath: "/opt/nvidia/bin/", expectedKey: "resetJob.driverBinPath"},
		{name: "colon in bin path", driverBinPath: "/opt/nvidia/bin:/usr/bin", expectedKey: "resetJob.driverBinPath"},
		{name: "parent traversal lib path", driverLibPath: "/opt/../nvidia/lib64", expectedKey: "resetJob.driverLibPath"},
		{name: "colon in lib path", driverLibPath: "/opt/nvidia/lib64:/lib", expectedKey: "resetJob.driverLibPath"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			template, err := getDefaultGPUResetJobTemplate(testNamespace, "alpine:latest", nil,
				ResourceRequirements{}, "/", DriverRootMountPath, "", true, "")
			require.NoError(t, err)

			err = applyDriverSearchPaths(template, test.driverBinPath, test.driverLibPath)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.expectedKey)
		})
	}
}
