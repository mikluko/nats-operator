package chart_test

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	extast "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/text"
	"sigs.k8s.io/yaml"
)

const installPage = "../../docs/content/docs/install.md"

// pageTable is one table of the install page: the text of the heading above
// it, and the code spans of each body cell.
type pageTable struct {
	heading string
	header  []string
	rows    [][][]string
}

// TestInstallPage_Values pins that the install page's values table names
// only keys values.yaml has, covers every leaf of values.yaml, and states each
// default as values.yaml sets it.
func TestInstallPage_Values(t *testing.T) {
	tables := installTables(t)
	values := chartValues(t)

	var listed []string
	for _, tb := range tables {
		if tb.header[0] != "Value" {
			continue
		}
		require.Equal(t, []string{"Value", "Default"}, tb.header[:2])
		for _, row := range tb.rows {
			require.Len(t, row[0], 1, "row %v", row)
			require.Len(t, row[1], 1, "row %v", row)
			key := row[0][0]
			require.NotContains(t, listed, key)
			listed = append(listed, key)

			got, ok := lookup(values, key)
			require.True(t, ok, "values.yaml has no %s", key)
			var want any
			require.NoError(t, yaml.Unmarshal([]byte(row[1][0]), &want), key)
			require.Equal(t, got, want, "default of %s", key)
		}
	}
	require.NotEmpty(t, listed, "the install page has no values table")

	for _, leaf := range leaves(values, "") {
		covered := slices.ContainsFunc(listed, func(k string) bool {
			return leaf == k || strings.HasPrefix(leaf, k+".")
		})
		require.True(t, covered, "the install page omits %s", leaf)
	}
}

// TestInstallPage_RBAC pins that the install page's RBAC tables are each
// controller's generated ClusterRole, exactly.
func TestInstallPage_RBAC(t *testing.T) {
	headings := map[string]string{
		"Cluster controller":   "cluster",
		"Auth controller":      "auth",
		"JetStream controller": "jetstream",
	}
	page := map[string]grants{}
	var common grants
	for _, tb := range installTables(t) {
		if tb.header[0] != "API group" {
			continue
		}
		g := tableGrants(t, tb)
		if tb.heading == "Every controller" {
			common = g
			continue
		}
		c, ok := headings[tb.heading]
		require.True(t, ok, "RBAC table under %q", tb.heading)
		page[c] = g
	}
	require.NotNil(t, common)

	for _, c := range controllers {
		require.Equal(t, generatedRole(t, c), merge(page[c], common), c)
	}
}

// tableGrants reads an RBAC table, whose cells are code spans of API groups,
// resources and verbs, with `""` the core group.
func tableGrants(t *testing.T, tb pageTable) grants {
	t.Helper()
	g := grants{}
	for _, row := range tb.rows {
		require.Len(t, row[0], 1)
		group := row[0][0]
		if group == `""` {
			group = ""
		}
		verbs := slices.Sorted(slices.Values(row[2]))
		for _, res := range row[1] {
			key := group + "/" + res
			require.NotContains(t, g, key)
			g[key] = verbs
		}
	}
	return g
}

// installTables parses the install page and returns its tables in order.
func installTables(t *testing.T) []pageTable {
	t.Helper()
	src, err := os.ReadFile(installPage)
	require.NoError(t, err)
	md := goldmark.New(goldmark.WithExtensions(extension.Table))
	doc := md.Parser().Parse(text.NewReader(src))

	var tables []pageTable
	var heading string
	for n := doc.FirstChild(); n != nil; n = n.NextSibling() {
		switch n := n.(type) {
		case *ast.Heading:
			heading = plain(n, src)
		case *extast.Table:
			tb := pageTable{heading: heading}
			for row := n.FirstChild(); row != nil; row = row.NextSibling() {
				var cells [][]string
				for cell := row.FirstChild(); cell != nil; cell = cell.NextSibling() {
					if _, ok := row.(*extast.TableHeader); ok {
						tb.header = append(tb.header, plain(cell, src))
						continue
					}
					cells = append(cells, codeSpans(cell, src))
				}
				if cells != nil {
					tb.rows = append(tb.rows, cells)
				}
			}
			tables = append(tables, tb)
		}
	}
	return tables
}

// codeSpans returns the text of every code span directly in n.
func codeSpans(n ast.Node, src []byte) []string {
	var out []string
	for c := n.FirstChild(); c != nil; c = c.NextSibling() {
		if _, ok := c.(*ast.CodeSpan); ok {
			out = append(out, plain(c, src))
		}
	}
	return out
}

// plain concatenates the text segments under n.
func plain(n ast.Node, src []byte) string {
	var b strings.Builder
	_ = ast.Walk(n, func(c ast.Node, entering bool) (ast.WalkStatus, error) {
		if tx, ok := c.(*ast.Text); ok && entering {
			b.Write(tx.Segment.Value(src))
		}
		return ast.WalkContinue, nil
	})
	return b.String()
}

func chartValues(t *testing.T) map[string]any {
	t.Helper()
	b, err := os.ReadFile(chartDir + "/values.yaml")
	require.NoError(t, err)
	var v map[string]any
	require.NoError(t, yaml.Unmarshal(b, &v))
	return v
}

// lookup returns the value at the dotted key in v.
func lookup(v map[string]any, key string) (any, bool) {
	var cur any = v
	for part := range strings.SplitSeq(key, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[part]; !ok {
			return nil, false
		}
	}
	return cur, true
}

// leaves returns the dotted key of every non-map value under v.
func leaves(v map[string]any, prefix string) []string {
	var out []string
	for k, x := range v {
		if m, ok := x.(map[string]any); ok && len(m) > 0 {
			out = append(out, leaves(m, prefix+k+".")...)
			continue
		}
		out = append(out, prefix+k)
	}
	return out
}
