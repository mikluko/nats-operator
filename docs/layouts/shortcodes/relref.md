{{- /* relref in a Markdown rendition: the address of the named page's own Markdown, with the fragment kept. */ -}}
{{- $parts := split (.Get 0) "#" -}}
{{- $target := .Page.GetPage (index $parts 0) -}}
{{- if not $target -}}
{{- errorf "%s: relref %q names no page" .Position (.Get 0) -}}
{{- end -}}
{{- ($target.OutputFormats.Get "markdown").Permalink -}}
{{- if gt (len $parts) 1 -}}#{{ index $parts 1 }}{{- end -}}
