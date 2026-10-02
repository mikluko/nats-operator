{{- /* Hudocs' type shortcode as Markdown: the type's label in a code span. */ -}}
{{- $raw := .Get 0 -}}
{{- if .IsNamedParams -}}
{{- $raw = .Get "name" | default (.Get "type") -}}
{{- end -}}
{{- $label := printf "%v" $raw | strings.TrimSuffix "?" | strings.TrimPrefix "{" | strings.TrimSuffix "}" | strings.TrimSpace -}}
`{{ $label }}`
{{- "" -}}
