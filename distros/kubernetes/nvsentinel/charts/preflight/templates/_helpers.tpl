{{/*
Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/*
Expand the name of the chart.
*/}}
{{- define "preflight.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "preflight.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "preflight.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "preflight.labels" -}}
helm.sh/chart: {{ include "preflight.chart" . }}
{{ include "preflight.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "preflight.selectorLabels" -}}
app.kubernetes.io/name: {{ include "preflight.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "preflight.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "preflight.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Webhook name for MutatingWebhookConfiguration
*/}}
{{- define "preflight.webhookName" -}}
{{ include "preflight.name" . }}.nvsentinel.nvidia.com
{{- end }}

{{/*
Certificate secret name
*/}}
{{- define "preflight.certSecretName" -}}
{{ include "preflight.fullname" . }}-webhook-tls
{{- end }}

{{/*
Certificate DNS names
*/}}
{{- define "preflight.certDnsNames" -}}
- {{ include "preflight.fullname" . }}
- {{ include "preflight.fullname" . }}.{{ .Release.Namespace }}
- {{ include "preflight.fullname" . }}.{{ .Release.Namespace }}.svc
- {{ include "preflight.fullname" . }}.{{ .Release.Namespace }}.svc.cluster.local
{{- end }}

{{/*
DCGM service endpoint - uses global.dcgm.service.endpoint with fallback to local
*/}}
{{- define "preflight.dcgmEndpoint" -}}
{{- if and .Values.global .Values.global.dcgm .Values.global.dcgm.service }}
{{- .Values.global.dcgm.service.endpoint | default .Values.dcgm.service.endpoint }}
{{- else }}
{{- .Values.dcgm.service.endpoint }}
{{- end }}
{{- end }}

{{/*
DCGM service port - uses global.dcgm.service.port with fallback to local
*/}}
{{- define "preflight.dcgmPort" -}}
{{- if and .Values.global .Values.global.dcgm .Values.global.dcgm.service }}
{{- .Values.global.dcgm.service.port | default .Values.dcgm.service.port }}
{{- else }}
{{- .Values.dcgm.service.port }}
{{- end }}
{{- end }}

{{/*
DCGM hostengine address - combines endpoint and port
*/}}
{{- define "preflight.dcgmHostengineAddr" -}}
{{- printf "%s:%v" (include "preflight.dcgmEndpoint" .) (include "preflight.dcgmPort" .) }}
{{- end }}

{{/*
DCGM diagnostic level
*/}}
{{- define "preflight.dcgmDiagLevel" -}}
{{- .Values.dcgm.diagLevel | default 1 }}
{{- end }}

{{/*
Event processing strategy
*/}}
{{- define "preflight.processingStrategy" -}}
{{- .Values.dcgm.processingStrategy | default "EXECUTE_REMEDIATION" }}
{{- end }}

{{/*
Platform connector socket path for health event reporting
Uses global.socketPath with unix:// prefix
*/}}
{{- define "preflight.connectorSocket" -}}
{{- if and .Values.global .Values.global.socketPath }}
{{- printf "unix://%s" .Values.global.socketPath }}
{{- else }}
{{- "unix:///var/run/nvsentinel.sock" }}
{{- end }}
{{- end }}

{{/*
=============================================================================
Network fabric profiles for NCCL all-reduce.
Selected by .Values.networkFabric (ib | efa | mnnvl-efa | tcpxo).
=============================================================================
*/}}

{{/*
Fabric env vars — returns a YAML list of env var objects.
*/}}
{{- define "preflight.fabric.env" -}}
{{- if eq .Values.networkFabric "ib" }}
- name: NCCL_TOPO_FILE
  value: "/etc/nccl/topo.xml"
- name: NCCL_IB_PCI_RELAXED_ORDERING
  value: "1"
- name: NCCL_SOCKET_IFNAME
  value: "eth0"
{{- else if eq .Values.networkFabric "efa" }}
- name: FI_EFA_USE_DEVICE_RDMA
  value: "1"
- name: FI_PROVIDER
  value: "efa"
- name: NCCL_SOCKET_IFNAME
  value: "eth0"
- name: LD_LIBRARY_PATH
  value: "/opt/amazon/ofi-nccl/lib:/opt/amazon/efa/lib:/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/cuda/lib64"
{{- else if eq .Values.networkFabric "mnnvl-efa" }}
- name: FI_EFA_USE_DEVICE_RDMA
  value: "1"
- name: NCCL_MNNVL_ENABLE
  value: "1"
- name: NCCL_NVLS_ENABLE
  value: "0"
- name: NCCL_NET_GDR_LEVEL
  value: "PHB"
- name: NCCL_P2P_NET_CHUNKSIZE
  value: "2097152"
- name: LD_LIBRARY_PATH
  value: "/opt/amazon-efa-ofi/ofi/lib:/opt/amazon-efa-ofi/efa/lib:/usr/local/nvidia/lib64:/usr/lib:/usr/lib64:/usr/local/cuda/lib64"
{{- else if eq .Values.networkFabric "tcpxo" }}
- name: NCCL_SOCKET_IFNAME
  value: "eth1,eth2,eth3,eth4,eth5,eth6,eth7,eth8"
- name: NCCL_CROSS_NIC
  value: "0"
- name: NCCL_NVLS_ENABLE
  value: "0"
- name: NCCL_FASTRAK_CTRL_DEV
  value: "eth0"
- name: NCCL_FASTRAK_IFNAME
  value: "eth1,eth2,eth3,eth4,eth5,eth6,eth7,eth8"
- name: NCCL_BUFFSIZE
  value: "8388608"
- name: NCCL_FASTRAK_NUM_FLOWS
  value: "2"
- name: NCCL_FASTRAK_USE_SNAP
  value: "1"
- name: NCCL_FASTRAK_USE_LLCM
  value: "1"
- name: NCCL_FASTRAK_LLCM_DEVICE_DIRECTORY
  value: "/dev/aperture_devices"
- name: NCCL_FASTRAK_ENABLE_CONTROL_CHANNEL
  value: "0"
- name: NCCL_FASTRAK_ENABLE_HOTPATH_LOGGING
  value: "0"
- name: NCCL_FASTRAK_PLUGIN_ACCEPT_TIMEOUT_MS
  value: "600000"
- name: NCCL_NET_GDR_LEVEL
  value: "PIX"
- name: NCCL_SHIMNET_GUEST_CONFIG_CHECKER_CONFIG_FILE
  value: "/usr/local/nvidia/lib64/a3plus_guest_config.textproto"
- name: NCCL_TUNER_PLUGIN
  value: "libnccl-tuner.so"
- name: NCCL_TUNER_CONFIG_PATH
  value: "/usr/local/nvidia/lib64/a3plus_tuner_config.textproto"
- name: LD_LIBRARY_PATH
  value: "/usr/local/nvidia/lib64:/usr/lib/x86_64-linux-gnu"
{{- end }}
{{- end }}

{{/*
Fabric extraHostPathMounts — returns a YAML list of hostPath mount objects.
*/}}
{{- define "preflight.fabric.extraHostPathMounts" -}}
{{- if eq .Values.networkFabric "efa" }}
- name: host-opt-amazon
  hostPath: /opt/amazon
  mountPath: /opt/amazon
  readOnly: true
  hostPathType: Directory
{{- else if eq .Values.networkFabric "mnnvl-efa" }}
- name: amazon-efa
  hostPath: /opt/amazon-efa-ofi
  mountPath: /opt/amazon-efa-ofi
  readOnly: true
  hostPathType: Directory
{{- end }}
{{- end }}

{{/*
Fabric extraVolumeMounts — returns a YAML list of volume mount objects.
*/}}
{{- define "preflight.fabric.extraVolumeMounts" -}}
{{- if eq .Values.networkFabric "tcpxo" }}
- name: nvtcpxo-libraries
  mountPath: /usr/local/nvidia
  readOnly: true
- name: nvtcpxo-aperture-devices
  mountPath: /dev/aperture_devices
{{- end }}
{{- end }}

{{/*
Fabric ncclTopoConfigMap — returns the ConfigMap name (empty if not needed).
*/}}
{{- define "preflight.fabric.ncclTopoConfigMap" -}}
{{- if eq .Values.networkFabric "ib" }}
{{- .Values.gangCoordination.ncclTopoConfigMap | default "" }}
{{- end }}
{{- end }}
