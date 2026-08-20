{{/*
Expand the name of the chart.
*/}}
{{- define "nvsentinel.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "nvsentinel.fullname" -}}
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
{{- define "nvsentinel.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "nvsentinel.labels" -}}
helm.sh/chart: {{ include "nvsentinel.chart" . }}
{{ include "nvsentinel.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "nvsentinel.selectorLabels" -}}
app.kubernetes.io/name: {{ include "nvsentinel.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "nvsentinel.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "nvsentinel.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
MongoDB client certificate helpers are local to this subchart so it can be
rendered and tested independently of the parent NVSentinel chart.
*/}}
{{- define "mongodb-store.certificates.secretName" -}}
{{- $global := .Values.global | default dict -}}
{{- $datastore := get $global "datastore" | default dict -}}
{{- $auth := get $datastore "auth" | default dict -}}
{{- $certificates := get $datastore "certificates" | default dict -}}
{{- if get $auth "clientCertSecretName" -}}
{{ get $auth "clientCertSecretName" }}
{{- else if get $certificates "secretName" -}}
{{ get $certificates "secretName" }}
{{- else -}}
mongo-app-client-cert-secret
{{- end -}}
{{- end -}}

{{- define "mongodb-store.certificates.volumeItems" -}}
{{- $global := .Values.global | default dict -}}
{{- $datastore := get $global "datastore" | default dict -}}
{{- $certificates := get $datastore "certificates" | default dict -}}
items:
  - key: {{ get $certificates "certKey" | default "tls.crt" }}
    path: tls.crt
  - key: {{ get $certificates "keyKey" | default "tls.key" }}
    path: tls.key
  - key: {{ get $certificates "caKey" | default "ca.crt" }}
    path: ca.crt
{{- end -}}
