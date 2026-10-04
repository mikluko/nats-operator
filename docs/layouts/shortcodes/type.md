{{- /* A type's name as Markdown, in a code span. */ -}}
{{- $raw := .Get 0 -}}
{{- if .IsNamedParams -}}
{{- $raw = .Get "name" | default (.Get "type") -}}
{{- end -}}
{{- $label := printf "%v" $raw | strings.TrimSuffix "?" | strings.TrimPrefix "{" | strings.TrimSuffix "}" | strings.TrimSpace -}}
`{{ $label }}`
{{- "" -}}
