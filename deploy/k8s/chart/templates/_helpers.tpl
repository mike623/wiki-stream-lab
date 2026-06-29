{{- define "wsl.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end -}}

{{- define "wsl.storageClass" -}}
{{- if .Values.storage.className }}
storageClassName: {{ .Values.storage.className }}
{{- end }}
{{- end -}}
