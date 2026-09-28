{{- define "type" }}
### {{ .Name }} {#{{ .Name }}}
{{ template "doc" .Doc }}
{{- if eq .Kind 0 }}\
Type: {{ template "typebadge" .UnderlyingType }}
{{- end }}
{{- with .SortedReferences }}\
Appears on:
{{- range $i, $t := . }}{{ if $i }},{{ end }} [{{ .Name }}](#{{ .Name }}){{ end }}.
{{- end }}
{{- with .EnumValues }}
| Value | Description |
| :---- | :---------- |
{{- range . }}
| `{{ .Name }}` | {{ template "doc" .Doc }} |
{{- end }}
{{- end }}
{{- if .Fields }}
| Field | Type | Required | Description |
| :---- | :--- | :------: | :---------- |
{{- with .GVK }}
| `apiVersion` | {{ template "badge" "string" }} | Yes | `{{ .Group }}/{{ .Version }}` |
| `kind` | {{ template "badge" "string" }} | Yes | `{{ .Kind }}` |
{{- end }}
{{- template "members" . }}
{{- end }}
{{ end }}
