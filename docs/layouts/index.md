{{- /* The front page as Markdown, from the same params its HTML layout reads. */ -}}
{{- $hero := .Params.hero -}}
# {{ site.Title }}

> {{ site.Params.description }}

{{ $hero.lead }}

{{ .RenderShortcodes }}

## {{ .Params.controllersHeading }}
{{ range .Params.controllers }}
{{- $story := site.GetPage .story }}
### {{ .name }}

{{ .text }}

Kinds: {{ range $i, $kind := .kinds }}{{ if $i }}, {{ end }}`{{ $kind }}`{{ end }}.

Story: [{{ $story.Title }}]({{ ($story.OutputFormats.Get "markdown").Permalink }})
{{ end }}
## {{ .Params.featuresHeading }}
{{ range .Params.features }}
- **{{ .name }}.** {{ .text }}
{{- end }}
{{ with site.GetPage "/docs/stories" }}
## {{ $.Params.storiesHeading }}

{{ $.Params.storiesLead }}
{{ range .Pages.ByWeight }}
1. [{{ .Title }}]({{ (.OutputFormats.Get "markdown").Permalink }})
{{- end }}
{{- end }}
