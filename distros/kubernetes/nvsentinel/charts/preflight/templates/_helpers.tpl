{{/*
Expand the name of the chart.
*/}}
{{- define "preflight.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
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
Create the webhook name
*/}}
{{- define "preflight.webhookName" -}}
{{- printf "%s.%s.nvidia.com" (include "preflight.fullname" .) .Release.Namespace }}
{{- end }}

{{/*
Create the certificate secret name
*/}}
{{- define "preflight.certSecretName" -}}
{{- printf "%s-webhook-cert" (include "preflight.fullname" .) }}
{{- end }}

{{/*
Generate the list of DNS names for the webhook certificate
*/}}
{{- define "preflight.certDnsNames" -}}
- {{ include "preflight.fullname" . }}
- {{ include "preflight.fullname" . }}.{{ .Release.Namespace }}
- {{ include "preflight.fullname" . }}.{{ .Release.Namespace }}.svc
- {{ include "preflight.fullname" . }}.{{ .Release.Namespace }}.svc.cluster.local
{{- end }}
