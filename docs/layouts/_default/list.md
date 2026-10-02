{{- /* A section as Markdown: its title, description and source, then a link to the Markdown of each page under it. */ -}}
# {{ partial "title.html" . }}
{{ with .Description }}
> {{ . }}
{{ end }}
{{ .RenderShortcodes }}
{{ range .Pages.ByWeight }}
- [{{ partial "title.html" . }}]({{ (.OutputFormats.Get "markdown").Permalink }}){{ with .Description }}: {{ . }}{{ end }}
{{- end }}
