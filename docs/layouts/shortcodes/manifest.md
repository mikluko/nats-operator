{{- /* A story file as Markdown: its name, its YAML, and the kubectl commands that apply or delete it. */ -}}
{{- $name := .Get 0 -}}
{{- $manifest := partial "manifest.html" (dict "page" .Page "name" $name) -}}
`{{ $name }}`

```yaml
{{ strings.TrimSpace $manifest.yaml }}
```
{{ with $manifest.commands }}
```sh
{{ delimit . "\n" }}
```
{{ end -}}
