{{- /* The front page as Markdown, from the same params its HTML layout reads. */ -}}
# {{ site.Title }}

> {{ site.Params.description }}

{{ .Params.lead }}

## {{ .Params.controllersHeading }}
{{ range .Params.controllers }}
{{- $story := site.GetPage .story }}
### {{ .name }}

{{ .text }}

Kinds: {{ range $i, $kind := .kinds }}{{ if $i }}, {{ end }}`{{ $kind }}`{{ end }}.

Story: [{{ $story.Title }}]({{ ($story.OutputFormats.Get "markdown").Permalink }})
{{ end }}
{{ .RenderShortcodes }}

## {{ .Params.areasHeading }}
{{ range .Params.areas }}
{{- $page := site.GetPage .page }}
- [{{ partial "title.html" $page }}]({{ ($page.OutputFormats.Get "markdown").Permalink }}): {{ .text }}
{{- end }}
{{ with site.GetPage "/docs/stories" }}
## {{ $.Params.storiesHeading }}
{{ range .Pages.ByWeight }}
1. [{{ .Title }}]({{ (.OutputFormats.Get "markdown").Permalink }})
{{- end }}
{{- end }}
