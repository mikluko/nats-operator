package e2e

import (
	"bytes"
	"fmt"
	"os"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"sigs.k8s.io/yaml"
)

// ChartValuesHeading is the heading of the section of a story's page that
// shows the chart values a story scraping metrics installs.
const ChartValuesHeading = "The chart"

// readChartValues returns the one yaml code block of the section of the page
// at path headed ChartValuesHeading, decoded.
func readChartValues(path string) (map[string]any, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	doc := goldmark.New().Parser().Parse(text.NewReader(src))
	var blocks [][]byte
	in := false
	for n := doc.FirstChild(); n != nil; n = n.NextSibling() {
		switch n := n.(type) {
		case *ast.Heading:
			in = headingText(n, src) == ChartValuesHeading
		case *ast.FencedCodeBlock:
			if in && string(n.Language(src)) == "yaml" {
				var b bytes.Buffer
				for i := range n.Lines().Len() {
					seg := n.Lines().At(i)
					b.Write(seg.Value(src))
				}
				blocks = append(blocks, b.Bytes())
			}
		}
	}
	if len(blocks) != 1 {
		return nil, fmt.Errorf("%s: want one yaml code block under %q, got %d", path, ChartValuesHeading, len(blocks))
	}
	var vals map[string]any
	if err := yaml.UnmarshalStrict(blocks[0], &vals); err != nil {
		return nil, fmt.Errorf("%s: %s: %w", path, ChartValuesHeading, err)
	}
	return vals, nil
}

func headingText(h *ast.Heading, src []byte) string {
	var b bytes.Buffer
	_ = ast.Walk(h, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if t, ok := n.(*ast.Text); ok && entering {
			b.Write(t.Segment.Value(src))
		}
		return ast.WalkContinue, nil
	})
	return b.String()
}
