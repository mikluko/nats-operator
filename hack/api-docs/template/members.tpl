{{- /* members renders one table row per field, inlined fields in place. */ -}}
{{- define "members" }}
{{- range .Fields }}
| `{{ .Name }}` | {{ template "typelink" .Type }} | {{ if index .Markers "optional" }}No{{ else }}Yes{{ end }} | {{ template "doc" .Doc }}{{ template "default" . }} |
{{- end }}
{{- end }}

{{- /* default renders a +kubebuilder:default marker as a sentence: a string
       as written, anything else as JSON. */ -}}
{{- define "default" -}}
{{- with index .Markers "kubebuilder:default" -}}
{{- $v := (last .).Value }} Default: `{{ if kindIs "string" $v }}{{ $v }}{{ else }}{{ toJson $v }}{{ end }}`.
{{- end -}}
{{- end -}}
