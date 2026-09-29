// Package placeholders defines the tag that marks a value in a story's
// status or live file as an example, and strips it.
package placeholders

import "go.yaml.in/yaml/v3"

// Tag marks a value in a status or live file as an example: the page shows
// it, and the harness accepts whatever the live object holds there, absence
// included.
const Tag = "!any"

// Strip returns raw with every Tag removed, so each example value decodes as
// the type it is written as.
func Strip(raw []byte) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	untag(&doc)
	return yaml.Marshal(&doc)
}

func untag(n *yaml.Node) {
	if n.Tag == Tag {
		n.Tag = ""
		n.Tag = n.ShortTag()
	}
	for _, c := range n.Content {
		untag(c)
	}
}
