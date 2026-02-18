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

package webhook

import (
	"fmt"
	"log/slog"
	"strconv"

	"github.com/nvidia/nvsentinel/preflight/pkg/config"
	"github.com/nvidia/nvsentinel/preflight/pkg/gang"
	"github.com/nvidia/nvsentinel/preflight/pkg/gang/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var supportedHostPathTypes = map[string]corev1.HostPathType{
	string(corev1.HostPathDirectory):         corev1.HostPathDirectory,
	string(corev1.HostPathDirectoryOrCreate): corev1.HostPathDirectoryOrCreate,
	string(corev1.HostPathFile):              corev1.HostPathFile,
	string(corev1.HostPathFileOrCreate):      corev1.HostPathFileOrCreate,
	string(corev1.HostPathSocket):            corev1.HostPathSocket,
	string(corev1.HostPathCharDev):           corev1.HostPathCharDev,
	string(corev1.HostPathBlockDev):          corev1.HostPathBlockDev,
}

const (
	nvsentinelSocketVolumeName = "nvsentinel-socket"
	// dshmVolumeName is the name for the shared memory volume needed by NCCL
	dshmVolumeName = "dshm"
	// ncclTopoVolumeName is the name for the NCCL topology ConfigMap volume
	ncclTopoVolumeName = "nccl-topo"
)

type PatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

type Injector struct {
	cfg        *config.Config
	discoverer gang.GangDiscoverer
}

func NewInjector(cfg *config.Config, discoverer gang.GangDiscoverer) *Injector {
	return &Injector{
		cfg:        cfg,
		discoverer: discoverer,
	}
}

// GangContext contains gang information extracted during injection.
// This is returned so the controller can register the peer.
type GangContext struct {
	GangID        string
	ConfigMapName string
}

func (i *Injector) InjectInitContainers(pod *corev1.Pod) ([]PatchOperation, *GangContext, error) {
	maxResources := i.findMaxResources(pod)
	if len(maxResources) == 0 {
		slog.Debug("Pod does not request GPU/network resources, skipping injection")
		return nil, nil, nil
	}

	// Check if pod is part of a gang
	var gangCtx *GangContext

	if i.cfg.GangCoordination.Enabled && i.discoverer != nil {
		if i.discoverer.CanHandle(pod) {
			gangID := i.discoverer.ExtractGangID(pod)
			if gangID != "" {
				gangCtx = &GangContext{
					GangID:        gangID,
					ConfigMapName: gang.ConfigMapName(gangID),
				}
				slog.Info("Pod is part of a gang",
					"pod", pod.Name,
					"namespace", pod.Namespace,
					"gangID", gangID,
					"configMap", gangCtx.ConfigMapName,
					"discoverer", i.discoverer.Name())
			}
		} else {
			slog.Debug("Pod not handled by gang discoverer",
				"pod", pod.Name,
				"namespace", pod.Namespace,
				"discoverer", i.discoverer.Name())
		}
	}

	initContainers := i.buildInitContainers(maxResources, gangCtx, pod.Spec.ResourceClaims)
	if len(initContainers) == 0 {
		// No init containers to inject, but still return gangCtx
		// so the controller can track gang membership
		return nil, gangCtx, nil
	}

	var patches []PatchOperation

	if len(pod.Spec.InitContainers) == 0 {
		patches = append(patches, PatchOperation{
			Op:    "add",
			Path:  "/spec/initContainers",
			Value: initContainers,
		})
	} else {
		// Append preflight init containers after existing init containers.
		// This preserves platform/user init ordering and ensures any
		// provider-injected setup init containers (e.g., GCP TCPXO daemon)
		// complete before running preflight checks.
		for _, c := range initContainers {
			patches = append(patches, PatchOperation{
				Op:    "add",
				Path:  "/spec/initContainers/-",
				Value: c,
			})
		}
	}

	volumePatches := i.injectVolumes(pod, gangCtx)
	patches = append(patches, volumePatches...)

	return patches, gangCtx, nil
}

// findMaxResources scans all containers and returns the maximum quantity
// for each GPU and network resource. Returns empty map if no GPU resources found.
func (i *Injector) findMaxResources(pod *corev1.Pod) corev1.ResourceList {
	maxResources := make(corev1.ResourceList)

	allResourceNames := append([]string{}, i.cfg.GPUResourceNames...)
	allResourceNames = append(allResourceNames, i.cfg.NetworkResourceNames...)

	for _, container := range pod.Spec.Containers {
		for _, name := range allResourceNames {
			resName := corev1.ResourceName(name)

			i.updateMax(maxResources, resName, container.Resources.Limits[resName])
			i.updateMax(maxResources, resName, container.Resources.Requests[resName])
		}
	}

	if !i.hasGPUResources(maxResources) {
		return nil
	}

	return maxResources
}

// hasGPUResources returns true if maxResources contains at least one
// non-zero GPU resource.
func (i *Injector) hasGPUResources(maxResources corev1.ResourceList) bool {
	for _, name := range i.cfg.GPUResourceNames {
		if qty, ok := maxResources[corev1.ResourceName(name)]; ok && !qty.IsZero() {
			return true
		}
	}

	return false
}

func (i *Injector) updateMax(resources corev1.ResourceList, name corev1.ResourceName, qty resource.Quantity) {
	if qty.IsZero() {
		return
	}

	if current, exists := resources[name]; !exists || qty.Cmp(current) > 0 {
		resources[name] = qty
	}
}

func (i *Injector) buildInitContainers(
	maxResources corev1.ResourceList,
	gangCtx *GangContext,
	podResourceClaims []corev1.PodResourceClaim,
) []corev1.Container {
	var initContainers []corev1.Container

	// Determine whether to mirror pod-level DRA claims to init containers.
	// Per ADR-026 §DRA Integration: "the webhook copies resource claim
	// references to the init container" so init containers get the same
	// device access (GPUs, RDMA, IMEX channels) as main containers.
	mirrorClaims := i.cfg.GangCoordination.MirrorResourceClaims != nil &&
		*i.cfg.GangCoordination.MirrorResourceClaims

	for _, tmpl := range i.cfg.InitContainers {
		container := tmpl.DeepCopy()

		if container.Resources.Requests == nil {
			container.Resources.Requests = make(corev1.ResourceList)
		}

		if container.Resources.Limits == nil {
			container.Resources.Limits = make(corev1.ResourceList)
		}

		for name, qty := range maxResources {
			container.Resources.Requests[name] = qty
			container.Resources.Limits[name] = qty
		}

		if _, ok := container.Resources.Requests[corev1.ResourceCPU]; !ok {
			container.Resources.Requests[corev1.ResourceCPU] = resource.MustParse("100m")
		}

		if _, ok := container.Resources.Requests[corev1.ResourceMemory]; !ok {
			container.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("500Mi")
		}

		i.injectCommonEnv(container)
		i.injectDCGMEnv(container)
		i.injectGangEnv(container, gangCtx)

		if gangCtx != nil {
			i.injectGangMounts(container, mirrorClaims, podResourceClaims)
		}

		initContainers = append(initContainers, *container)
	}

	return initContainers
}

// injectGangMounts adds gang-related volume mounts and DRA resource claims
// to an init container.
func (i *Injector) injectGangMounts(
	container *corev1.Container,
	mirrorClaims bool,
	podResourceClaims []corev1.PodResourceClaim,
) {
	// Gang ConfigMap and /dev/shm mounts are always needed for gang members.
	container.VolumeMounts = append(container.VolumeMounts,
		corev1.VolumeMount{
			Name:      types.GangConfigVolumeName,
			MountPath: i.cfg.GangCoordination.ConfigMapMountPath,
			ReadOnly:  true,
		},
		// NCCL requires a larger shared memory segment than the default 64MB.
		corev1.VolumeMount{
			Name:      dshmVolumeName,
			MountPath: "/dev/shm",
		},
	)

	// Add NCCL topology ConfigMap mount if configured.
	if i.cfg.GangCoordination.NCCLTopoConfigMap != "" {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      ncclTopoVolumeName,
			MountPath: "/etc/nccl",
			ReadOnly:  true,
		})
	}

	i.appendExtraHostPathMounts(container)
	i.appendExtraVolumeMounts(container)

	// Mirror all pod-level DRA resource claims to init containers.
	// This ensures init containers get the same device access as main
	// containers: GPUs, RDMA NICs, IMEX channels (GB200 MNNVL), etc.
	if mirrorClaims {
		for _, podClaim := range podResourceClaims {
			container.Resources.Claims = append(container.Resources.Claims, corev1.ResourceClaim{
				Name: podClaim.Name,
			})
		}
	}
}

// appendExtraHostPathMounts appends configured extra hostPath mounts to the container.
func (i *Injector) appendExtraHostPathMounts(container *corev1.Container) {
	for _, m := range i.cfg.GangCoordination.ExtraHostPathMounts {
		if m.Name == "" || m.MountPath == "" {
			continue
		}

		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      m.Name,
			MountPath: m.MountPath,
			ReadOnly:  boolDefault(m.ReadOnly, true),
		})
	}
}

// appendExtraVolumeMounts appends configured extra volume mounts (for pre-existing
// pod volumes such as GCP TCPXO plugin volumes) to the container.
func (i *Injector) appendExtraVolumeMounts(container *corev1.Container) {
	for _, m := range i.cfg.GangCoordination.ExtraVolumeMounts {
		if m.Name == "" || m.MountPath == "" {
			continue
		}

		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      m.Name,
			MountPath: m.MountPath,
			ReadOnly:  boolDefault(m.ReadOnly, true),
		})
	}
}

// injectCommonEnv injects environment variables common to all preflight init containers.
// These include NODE_NAME, PLATFORM_CONNECTOR_SOCKET, and PROCESSING_STRATEGY which are
// needed by any preflight check that publishes health events.
func (i *Injector) injectCommonEnv(container *corev1.Container) {
	envVars := []corev1.EnvVar{
		{
			Name: "NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
	}

	if i.cfg.DCGM.ConnectorSocket != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "PLATFORM_CONNECTOR_SOCKET",
			Value: i.cfg.DCGM.ConnectorSocket,
		})
	}

	if i.cfg.DCGM.ProcessingStrategy != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "PROCESSING_STRATEGY",
			Value: i.cfg.DCGM.ProcessingStrategy,
		})
	}

	i.mergeEnvVars(container, envVars)
}

func (i *Injector) injectVolumes(pod *corev1.Pod, gangCtx *GangContext) []PatchOperation {
	var patches []PatchOperation

	var volumesToAdd []corev1.Volume

	existingVolumes := make(map[string]bool)
	for _, vol := range pod.Spec.Volumes {
		existingVolumes[vol.Name] = true
	}

	if i.cfg.DCGM.ConnectorSocket != "" && !existingVolumes[nvsentinelSocketVolumeName] {
		// Platform-connector mounts /var/run/nvsentinel (host) -> /var/run (container)
		// and creates socket at /var/run/nvsentinel.sock inside its container.
		// This is the same hostPath used by gpu-health-monitor.
		hostPathType := corev1.HostPathDirectoryOrCreate

		volumesToAdd = append(volumesToAdd, corev1.Volume{
			Name: nvsentinelSocketVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/run/nvsentinel",
					Type: &hostPathType,
				},
			},
		})
	}

	if gangCtx != nil {
		volumesToAdd = append(volumesToAdd, i.collectGangVolumes(gangCtx, existingVolumes)...)
	}

	if len(volumesToAdd) == 0 {
		return patches
	}

	if len(pod.Spec.Volumes) == 0 {
		patches = append(patches, PatchOperation{
			Op:    "add",
			Path:  "/spec/volumes",
			Value: volumesToAdd,
		})
	} else {
		for _, vol := range volumesToAdd {
			patches = append(patches, PatchOperation{
				Op:    "add",
				Path:  "/spec/volumes/-",
				Value: vol,
			})
		}
	}

	return patches
}

// collectGangVolumes gathers all gang-related volumes (ConfigMap, shared memory,
// NCCL topology, extra hostPath) that are not already present in the pod.
func (i *Injector) collectGangVolumes(
	gangCtx *GangContext,
	existingVolumes map[string]bool,
) []corev1.Volume {
	var volumes []corev1.Volume

	// ConfigMap is optional because it may not exist yet when the pod is created.
	// The controller creates it when it discovers the gang.
	// Init containers poll the mounted path until peers are registered.
	if !existingVolumes[types.GangConfigVolumeName] {
		optional := true

		volumes = append(volumes, corev1.Volume{
			Name: types.GangConfigVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: gangCtx.ConfigMapName,
					},
					Optional: &optional,
				},
			},
		})
	}

	// Add shared memory volume for NCCL multi-GPU communication.
	// NCCL requires a larger /dev/shm than the default 64MB container limit.
	// Using emptyDir with Memory medium provides RAM-backed storage.
	// Cap at 64Gi to prevent unbounded RAM consumption on the node.
	if !existingVolumes[dshmVolumeName] {
		dshmSizeLimit := resource.MustParse("64Gi")
		volumes = append(volumes, corev1.Volume{
			Name: dshmVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium:    corev1.StorageMediumMemory,
					SizeLimit: &dshmSizeLimit,
				},
			},
		})
	}

	// Add NCCL topology ConfigMap volume if configured.
	// Required for Azure NDv4/v5 - NCCL needs this to map GPUs to IB NICs.
	if i.cfg.GangCoordination.NCCLTopoConfigMap != "" && !existingVolumes[ncclTopoVolumeName] {
		optional := true

		volumes = append(volumes, corev1.Volume{
			Name: ncclTopoVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: i.cfg.GangCoordination.NCCLTopoConfigMap,
					},
					Optional: &optional,
				},
			},
		})
	}

	volumes = append(volumes, i.collectExtraHostPathVolumes(existingVolumes)...)

	return volumes
}

// collectExtraHostPathVolumes builds volumes for configured extra hostPath mounts
// that are not already present in the pod.
func (i *Injector) collectExtraHostPathVolumes(existingVolumes map[string]bool) []corev1.Volume {
	var volumes []corev1.Volume

	for _, m := range i.cfg.GangCoordination.ExtraHostPathMounts {
		if m.Name == "" || m.HostPath == "" || existingVolumes[m.Name] {
			continue
		}

		hostPathType, ok := parseHostPathType(m.HostPathType)
		if !ok {
			slog.Warn("Ignoring unsupported hostPathType in extraHostPathMount",
				"name", m.Name,
				"hostPathType", m.HostPathType)

			continue
		}

		volumes = append(volumes, corev1.Volume{
			Name: m.Name,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: m.HostPath,
					Type: hostPathType,
				},
			},
		})
	}

	return volumes
}

func parseHostPathType(hostPathType string) (*corev1.HostPathType, bool) {
	if hostPathType == "" {
		return nil, true
	}

	t, ok := supportedHostPathTypes[hostPathType]
	if !ok {
		return nil, false
	}

	return &t, true
}

// injectDCGMEnv injects DCGM-specific environment variables for the dcgm-diag check.
func (i *Injector) injectDCGMEnv(container *corev1.Container) {
	if container.Name != "preflight-dcgm-diag" {
		return
	}

	envVars := []corev1.EnvVar{
		{
			Name:  "DCGM_DIAG_LEVEL",
			Value: fmt.Sprintf("%d", i.cfg.DCGM.DiagLevel),
		},
	}

	if i.cfg.DCGM.HostengineAddr != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "DCGM_HOSTENGINE_ADDR",
			Value: i.cfg.DCGM.HostengineAddr,
		})
	}

	i.mergeEnvVars(container, envVars)
}

// injectGangEnv injects gang-related environment variables for multi-node checks.
func (i *Injector) injectGangEnv(container *corev1.Container, gangCtx *GangContext) {
	if gangCtx == nil {
		return
	}

	slog.Info("Injecting gang environment variables", "gangID", gangCtx.GangID, "configMap", gangCtx.ConfigMapName)

	envVars := []corev1.EnvVar{
		{
			Name:  "GANG_ID",
			Value: gangCtx.GangID,
		},
		{
			Name:  "GANG_CONFIG_DIR",
			Value: i.cfg.GangCoordination.ConfigMapMountPath,
		},
		{
			Name:  "GANG_TIMEOUT_SECONDS",
			Value: i.cfg.GangCoordination.Timeout,
		},
		{
			Name:  "MASTER_PORT",
			Value: strconv.Itoa(i.cfg.GangCoordination.MasterPort),
		},
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: "POD_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
	}

	i.mergeEnvVars(container, envVars)
}

// mergeEnvVars merges the provided env vars into the container.
// User-defined env vars (already present in container) take precedence.
func (i *Injector) mergeEnvVars(container *corev1.Container, envVars []corev1.EnvVar) {
	existingEnvNames := make(map[string]bool)
	for _, env := range container.Env {
		existingEnvNames[env.Name] = true
	}

	for _, env := range envVars {
		if !existingEnvNames[env.Name] {
			container.Env = append(container.Env, env)
		}
	}
}

// boolDefault returns *ptr if non-nil, or def otherwise.
func boolDefault(ptr *bool, def bool) bool {
	if ptr != nil {
		return *ptr
	}

	return def
}
