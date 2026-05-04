{{/*  kave/templates/_helpers.tpl  */}}

{{- define "kave.name" -}}
{{ .Chart.Name }}
{{- end }}

{{- define "kave.image" -}}
{{- if not .Values.image.tag -}}
{{- fail "image.tag must be set via --set image.tag=<git-sha>" -}}
{{- end -}}
{{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end }}

{{- define "kave.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/name: {{ include "kave.name" . }}
{{- end }}

{{- define "kave.validateSecrets" -}}
{{- if eq .Values.config.adminAuthToken "-" -}}
{{- fail "config.adminAuthToken must be set via --set (default '-' is unsafe)" -}}
{{- end -}}
{{- end }}

{{- define "kave.validateReplicas" -}}
{{- if eq (mod .Values.voters.replicas 2) 0 -}}
{{- fail "voters.replicas must be odd for Raft quorum" -}}
{{- end -}}
{{- end }}
