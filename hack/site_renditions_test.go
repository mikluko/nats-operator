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
	"golang.org/x/net/html"
)

// renditionProblems returns "<file>: <problem>" for every page under root
// with no Markdown rendition beside it or, unless the page is a redirect, no
// alternate link to it at base, every rendition that holds an unrendered
// shortcode, a home page with no alternate link to llms.txt, and every page
// under docs/ with no page below it that llms.txt does not link.
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
		links, redirect := alternates(t, p)
		if !redirect && links["text/markdown"] != base+path.Join(dir, "index.md") {
			problems = append(problems, path.Join(dir, "index.html")+": no alternate link to its index.md")
		}
		if dir == "." && links["text/plain"] != base+"llms.txt" {
			problems = append(problems, "index.html: no alternate link to llms.txt")
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

// alternates returns the href of every rel="alternate" link of the HTML file
// at name, by its type, and whether the file is a meta refresh to another
// page.
func alternates(t *testing.T, name string) (links map[string]string, redirect bool) {
	t.Helper()
	b, err := os.ReadFile(name)
	require.NoError(t, err)
	doc, err := html.Parse(bytes.NewReader(b))
	require.NoError(t, err)
	links = map[string]string{}
	for n := range doc.Descendants() {
		if n.Type != html.ElementNode || n.Data != "link" {
			continue
		}
		attrs := map[string]string{}
		for _, a := range n.Attr {
			attrs[a.Key] = a.Val
		}
		if attrs["rel"] == "alternate" && attrs["type"] != "" {
			links[attrs["type"]] = attrs["href"]
		}
	}
	return links, len(scan(doc).refresh) > 0
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
	alternate := func(dir string) string {
		return `<link rel="alternate" type="text/markdown" href="` + siteBase + dir + `index.md">`
	}
	files := map[string]string{
		"index.html":        alternate(""),
		"index.md":          `# Home`,
		"llms.txt":          "# Site\n\n- [A](https://site.test/base/docs/a/index.md)\n",
		"docs/index.html":   `<meta http-equiv="refresh" content="0; url=a/">`,
		"docs/index.md":     `# Docs`,
		"docs/a/index.html": alternate("docs/a/"),
		"docs/a/index.md":   `# A`,
		"docs/b/index.html": ``,
		"docs/c/index.html": alternate("docs/c/"),
		"docs/c/index.md":   `{{< manifest "c.yaml" >}}`,
		"docs/d/index.html": alternate("docs/a/"),
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
		"docs/d/index.html: no alternate link to its index.md",
		"index.html: no alternate link to llms.txt",
		"llms.txt: no link to docs/c/index.md",
		"llms.txt: no link to docs/d/index.md",
	}, renditionProblems(t, root, siteBase))
}
