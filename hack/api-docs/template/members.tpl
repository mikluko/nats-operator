{{- /* members renders one table row per field, inlined fields in place. */ -}}
{{- define "members" }}
{{- range .Members }}
{{- if not (hiddenMember .) }}
{{- if fieldEmbedded . }}
{{- template "members" .Type }}
{{- else }}
| `{{ fieldName . | safe }}` | {{ template "typelink" .Type }} | {{ if isOptionalMember . }}No{{ else }}Yes{{ end }} | {{ template "doc" .CommentLines }}{{ template "default" .CommentLines }} |
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{- /* default renders a +kubebuilder:default marker as a sentence. */ -}}
{{- define "default" -}}
{{- range . -}}
{{- if and (gt (len .) 21) (eq (slice . 0 21) "+kubebuilder:default=") }} Default: `{{ safe (slice . 21) }}`.{{ end -}}
{{- end -}}
{{- end -}}
