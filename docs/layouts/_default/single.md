{{- /* A page as Markdown: its title unless its source opens with one, its description, and its source with the shortcodes rendered. */ -}}
{{- if not (hasPrefix (strings.TrimSpace .RawContent) "# ") -}}
# {{ partial "title.html" . }}
{{ end -}}
{{- with .Description }}
> {{ . }}
{{ end }}
{{ .RenderShortcodes }}
