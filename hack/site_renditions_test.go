package hack_test

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
)

// renditionProblems returns "<file>: <problem>" for every page under root
// with no Markdown rendition beside it, every rendition that holds an
// unrendered shortcode, and every page under docs/ with no page below it that
// llms.txt does not link at base.
func renditionProblems(t *testing.T, root, base string) []string {
	t.Helper()
	var problems, leaves []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "index.html" {
			return err
		}
		rel, err := filepath.Rel(root, filepath.Dir(p))
		if err != nil {
			return err
		}
		dir := filepath.ToSlash(rel)
		md, err := os.ReadFile(filepath.Join(filepath.Dir(p), "index.md"))
		if errors.Is(err, fs.ErrNotExist) {
			problems = append(problems, path.Join(dir, "index.html")+": no index.md beside it")
			return nil
		}
		if err != nil {
			return err
		}
		if bytes.Contains(md, []byte("{{<")) || bytes.Contains(md, []byte("{{%")) {
			problems = append(problems, path.Join(dir, "index.md")+": an unrendered shortcode")
		}
		if strings.HasPrefix(dir, "docs/") && !hasChildPage(t, filepath.Dir(p)) {
			leaves = append(leaves, dir)
		}
		return nil
	})
	require.NoError(t, err)

	linked := llmsLinks(t, root)
	for _, dir := range leaves {
		if !linked[base+dir+"/index.md"] {
			problems = append(problems, "llms.txt: no link to "+dir+"/index.md")
		}
	}
	sort.Strings(problems)
	return problems
}

// hasChildPage reports whether a directory of dir holds a page.
func hasChildPage(t *testing.T, dir string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, e.Name(), "index.html")); err == nil {
			return true
		}
	}
	return false
}

// llmsLinks returns the destination of every link in root's llms.txt.
func llmsLinks(t *testing.T, root string) map[string]bool {
	t.Helper()
	src, err := os.ReadFile(filepath.Join(root, "llms.txt"))
	require.NoError(t, err)
	links := map[string]bool{}
	doc := goldmark.DefaultParser().Parse(text.NewReader(src))
	require.NoError(t, ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if link, ok := n.(*ast.Link); ok && entering {
			links[string(link.Destination)] = true
		}
		return ast.WalkContinue, nil
	}))
	return links
}

func TestRenditionProblems(t *testing.T) {
	files := map[string]string{
		"index.html":        ``,
		"index.md":          `# Home`,
		"llms.txt":          "# Site\n\n- [A](https://site.test/base/docs/a/index.md)\n",
		"docs/index.html":   ``,
		"docs/index.md":     `# Docs`,
		"docs/a/index.html": ``,
		"docs/a/index.md":   `# A`,
		"docs/b/index.html": ``,
		"docs/c/index.html": ``,
		"docs/c/index.md":   `{{< manifest "c.yaml" >}}`,
		"docs/d/index.html": ``,
		"docs/d/index.md":   `# D`,
	}
	root := t.TempDir()
	for name, body := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
	}
	require.Equal(t, []string{
		"docs/b/index.html: no index.md beside it",
		"docs/c/index.md: an unrendered shortcode",
		"llms.txt: no link to docs/c/index.md",
		"llms.txt: no link to docs/d/index.md",
	}, renditionProblems(t, root, siteBase))
}
