{{/*
Expand the name of the chart.
*/}}
{{- define "gfs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "gfs.fullname" -}}
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
{{- define "gfs.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "gfs.labels" -}}
helm.sh/chart: {{ include "gfs.chart" . }}
{{ include "gfs.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "gfs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gfs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Master selector labels
*/}}
{{- define "gfs.masterSelectorLabels" -}}
app: {{ include "gfs.fullname" . }}-master
{{ include "gfs.selectorLabels" . }}
{{- end }}

{{/*
Chunkserver selector labels
*/}}
{{- define "gfs.chunkserverSelectorLabels" -}}
app: {{ include "gfs.fullname" . }}-chunkserver
{{ include "gfs.selectorLabels" . }}
{{- end }}

{{/*
FUSE selector labels
*/}}
{{- define "gfs.fuseSelectorLabels" -}}
app: {{ include "gfs.fullname" . }}-fuse
{{ include "gfs.selectorLabels" . }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "gfs.serviceAccountName" -}}
{{- if .Values.rbac.create }}
{{- default (printf "%s-sa" (include "gfs.fullname" .)) .Values.rbac.serviceAccountName }}
{{- else }}
{{- default "default" .Values.rbac.serviceAccountName }}
{{- end }}
{{- end }}
