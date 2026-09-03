{{- define "marketplace.name" -}}
marketplace-lakehouse
{{- end }}

{{- define "marketplace.image" -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- end }}

