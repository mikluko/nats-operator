package e2e

import (
	"fmt"

	"go.yaml.in/yaml/v3"
)

// PlaceholderTag marks a value in a status or live file as an example: the
// page shows it, and the harness accepts whatever the live object holds
// there, absence included.
const PlaceholderTag = "!any"

// placeholder stands in the expected tree for a value tagged PlaceholderTag.
type placeholder struct{}

// StripPlaceholders returns raw with every PlaceholderTag removed, so each
// example value decodes as the type it is written as.
func StripPlaceholders(raw []byte) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	untag(&doc)
	return yaml.Marshal(&doc)
}

func untag(n *yaml.Node) {
	if n.Tag == PlaceholderTag {
		n.Tag = ""
		n.Tag = n.ShortTag()
	}
	for _, c := range n.Content {
		untag(c)
	}
}

// decodeExpected decodes one YAML document into maps, lists and scalars,
// with placeholder{} wherever PlaceholderTag stands.
func decodeExpected(raw []byte) (map[string]any, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	if doc.Kind == 0 {
		return nil, nil
	}
	v, err := decodeNode(&doc)
	if err != nil {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("line %d: not a mapping", doc.Line)
	}
	return m, nil
}

func decodeNode(n *yaml.Node) (any, error) {
	if n.Tag == PlaceholderTag {
		return placeholder{}, nil
	}
	switch n.Kind {
	case yaml.DocumentNode:
		return decodeNode(n.Content[0])
	case yaml.AliasNode:
		return decodeNode(n.Alias)
	case yaml.MappingNode:
		m := make(map[string]any, len(n.Content)/2)
		for i := 0; i+1 < len(n.Content); i += 2 {
			v, err := decodeNode(n.Content[i+1])
			if err != nil {
				return nil, err
			}
			m[n.Content[i].Value] = v
		}
		return m, nil
	case yaml.SequenceNode:
		l := make([]any, 0, len(n.Content))
		for _, c := range n.Content {
			v, err := decodeNode(c)
			if err != nil {
				return nil, err
			}
			l = append(l, v)
		}
		return l, nil
	default:
		var v any
		if err := n.Decode(&v); err != nil {
			return nil, fmt.Errorf("line %d: %w", n.Line, err)
		}
		return v, nil
	}
}
