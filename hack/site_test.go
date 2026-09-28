package hack_test

import (
	"bytes"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
)

// siteBase is the base URL the site is built under in tests. Its path is not
// the root, so a link that drops the base path is caught as broken.
const siteBase = "https://site.test/base/"

// buildSite renders docs/ under siteBase as .github/workflows/docs.yml does,
// and returns the output directory; it needs hugo extended on PATH.
func buildSite(t *testing.T) string {
	t.Helper()
	hugo, err := exec.LookPath("hugo")
	require.NoError(t, err, "the site test needs hugo extended on PATH")
	out := t.TempDir()
	cmd := exec.Command(hugo, "--gc", "--minify", "--baseURL", siteBase, "--destination", out, "--panicOnWarning")
	cmd.Dir = "../docs"
	combined, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", combined)
	return out
}

// page is one rendered HTML file.
type page struct {
	ids   map[string]bool
	links []string
	h1    int
}

// parsePages reads every .html file under root, keyed by its slash path
// relative to root.
func parsePages(t *testing.T, root string) map[string]*page {
	t.Helper()
	pages := map[string]*page{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) != ".html" {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		doc, err := html.Parse(bytes.NewReader(b))
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		pages[filepath.ToSlash(rel)] = scan(doc)
		return nil
	})
	require.NoError(t, err)
	return pages
}

func scan(doc *html.Node) *page {
	pg := &page{ids: map[string]bool{}}
	for n := range doc.Descendants() {
		if n.Type != html.ElementNode {
			continue
		}
		if n.Data == "h1" {
			pg.h1++
		}
		attrs := map[string]string{}
		for _, a := range n.Attr {
			attrs[a.Key] = a.Val
		}
		if id, ok := attrs["id"]; ok {
			pg.ids[id] = true
		}
		switch n.Data {
		case "a", "link":
			if v, ok := attrs["href"]; ok {
				pg.links = append(pg.links, v)
			}
		case "img", "script":
			if v, ok := attrs["src"]; ok {
				pg.links = append(pg.links, v)
			}
		case "meta":
			if strings.EqualFold(attrs["http-equiv"], "refresh") {
				if _, target, ok := strings.Cut(attrs["content"], "url="); ok {
					pg.links = append(pg.links, target)
				}
			}
		}
	}
	return pg
}

// brokenLinks returns "<page> -> <link>" for every link from a page under
// root that stays on the site at base but names no file under root, or names
// a fragment its target page does not define.
func brokenLinks(t *testing.T, root, base string) []string {
	t.Helper()
	baseURL, err := url.Parse(base)
	require.NoError(t, err)
	pages := parsePages(t, root)
	var broken []string
	for name, pg := range pages {
		from := baseURL.JoinPath(path.Dir(name) + "/")
		for _, link := range pg.links {
			if !linkResolves(root, pages, baseURL, from, name, link) {
				broken = append(broken, name+" -> "+link)
			}
		}
	}
	sort.Strings(broken)
	return broken
}

func linkResolves(root string, pages map[string]*page, base, from *url.URL, name, link string) bool {
	ref, err := url.Parse(link)
	if err != nil {
		return false
	}
	if ref.Scheme != "" && ref.Scheme != "http" && ref.Scheme != "https" {
		return true
	}
	target := from.ResolveReference(ref)
	if target.Host != base.Host {
		return true
	}
	if !strings.HasPrefix(target.Path, base.Path) {
		return false
	}
	rel := strings.TrimPrefix(target.Path, base.Path)
	file := rel
	if rel == "" || strings.HasSuffix(rel, "/") {
		file = rel + "index.html"
	}
	if ref.Path == "" && ref.Host == "" {
		file = name
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(file))); err != nil {
		return false
	}
	if target.Fragment == "" {
		return true
	}
	pg, ok := pages[file]
	return ok && pg.ids[target.Fragment]
}

func duplicateH1(t *testing.T, root string) []string {
	t.Helper()
	var dup []string
	for name, pg := range parsePages(t, root) {
		if pg.h1 > 1 {
			dup = append(dup, name)
		}
	}
	sort.Strings(dup)
	return dup
}

func TestSite(t *testing.T) {
	root := buildSite(t)
	require.Empty(t, brokenLinks(t, root, siteBase), "broken internal links")
	require.Empty(t, duplicateH1(t, root), "pages with more than one h1")
}

func TestSiteChecks(t *testing.T) {
	files := map[string]string{
		"index.html":        `<h1>Home</h1><a href="docs/">docs</a><a href="https://elsewhere.test/x">out</a><a href="mailto:a@b">mail</a>`,
		"docs/index.html":   `<meta http-equiv="refresh" content="0; url=https://site.test/base/docs/a/">`,
		"docs/a/index.html": `<h1>A</h1><h2 id="sec">S</h2><a href="#sec">self</a><a href="../b/#top">b</a><script src="/base/app.js"></script>`,
		"docs/b/index.html": `<h1 id="top">B</h1><h1>B again</h1><a href="/docs/a/">rootless</a><a href="../a/#missing">frag</a><a href="../c/">gone</a>`,
		"app.js":            ``,
	}
	root := t.TempDir()
	for name, body := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
	}
	require.Equal(t, []string{
		"docs/b/index.html -> ../a/#missing",
		"docs/b/index.html -> ../c/",
		"docs/b/index.html -> /docs/a/",
	}, brokenLinks(t, root, siteBase))
	require.Equal(t, []string{"docs/b/index.html"}, duplicateH1(t, root))
}
