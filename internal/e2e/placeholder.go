package e2e

import (
	"fmt"

	"go.yaml.in/yaml/v3"

	"github.com/mikluko/nats-operator/internal/e2e/placeholders"
)

// placeholder stands in the expected tree for a value tagged placeholders.Tag.
type placeholder struct{}

// decodeExpected decodes one YAML document into maps, lists and scalars,
// with placeholder{} wherever placeholders.Tag stands.
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
	if n.Tag == placeholders.Tag {
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
