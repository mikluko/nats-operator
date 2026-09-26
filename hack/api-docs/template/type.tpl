{{- define "type" }}
{{- $kind := false }}
{{- range .SecondClosestCommentLines }}{{ if eq . "+kubebuilder:object:root=true" }}{{ $kind = true }}{{ end }}{{ end }}
{{- range .CommentLines }}{{ if eq . "+kubebuilder:object:root=true" }}{{ $kind = true }}{{ end }}{{ end }}
### {{ .Name.Name | safe }} {#{{ .Name.Name | safe }}}
{{ template "doc" .CommentLines }}
{{- if eq .Kind "Alias" }}\
Type: {{ template "badge" (print .Underlying) }}
{{- end }}
{{- with (typeReferences .) }}\
Appears on:
{{- range $i, $t := . }}{{ if $i }},{{ end }} [{{ .Name.Name | safe }}](#{{ .Name.Name | safe }}){{ end }}.
{{- end }}
{{- with (constantsOfType .) }}
| Value | Description |
| :---- | :---------- |
{{- range . }}
| `{{ .ConstValue | safe }}` | {{ template "doc" .CommentLines }} |
{{- end }}
{{- end }}
{{- if .Members }}
| Field | Type | Required | Description |
| :---- | :--- | :------: | :---------- |
{{- if $kind }}
| `apiVersion` | {{ template "badge" "string" }} | Yes | `{{ apiGroup . | safe }}` |
| `kind` | {{ template "badge" "string" }} | Yes | `{{ .Name.Name | safe }}` |
{{- end }}
{{- template "members" . }}
{{- end }}
{{ end }}
