package hack_test

import (
	"bytes"
	"encoding/json"
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

// siteBase is the base URL the site is built under in tests.
const siteBase = "https://site.test/base/"

// buildSite renders docs/ under siteBase and returns the output directory.
// It skips the test when hugo is not on PATH.
func buildSite(t *testing.T) string {
	t.Helper()
	hugo, err := exec.LookPath("hugo")
	if err != nil {
		t.Skip("hugo is not on PATH")
	}
	out := t.TempDir()
	cmd := exec.Command(hugo, "--gc", "--minify", "--baseURL", siteBase, "--destination", out, "--panicOnWarning")
	cmd.Dir = "../docs"
	combined, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", combined)
	return out
}

type page struct {
	h1      int
	refresh []string
}

// parsePages reads every .html file under root, keyed by its slash path
// relative to root.
func parsePages(t *testing.T, root string) map[string]page {
	t.Helper()
	pages := map[string]page{}
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

func scan(doc *html.Node) page {
	var pg page
	for n := range doc.Descendants() {
		if n.Type != html.ElementNode {
			continue
		}
		attrs := map[string]string{}
		for _, a := range n.Attr {
			attrs[a.Key] = a.Val
		}
		switch {
		case n.Data == "h1":
			pg.h1++
		case n.Data == "meta" && strings.EqualFold(attrs["http-equiv"], "refresh"):
			if _, target, ok := strings.Cut(attrs["content"], "url="); ok {
				pg.refresh = append(pg.refresh, target)
			}
		}
	}
	return pg
}

// siteProblems returns "<page>: <problem>" for every page under root, built
// under base, with more than one h1 or a meta refresh to a page not under
// root. Links and fragments are lychee's, in `just site-check`, which does
// not read meta refresh.
func siteProblems(t *testing.T, root, base string) []string {
	t.Helper()
	baseURL, err := url.Parse(base)
	require.NoError(t, err)
	var problems []string
	for name, pg := range parsePages(t, root) {
		if pg.h1 > 1 {
			problems = append(problems, name+": more than one h1")
		}
		from := baseURL.JoinPath(path.Dir(name) + "/")
		for _, link := range pg.refresh {
			if !resolves(root, baseURL, from, link) {
				problems = append(problems, name+": refresh to "+link)
			}
		}
	}
	sort.Strings(problems)
	return problems
}

// resolves reports whether link, on a page at from, names a file under root,
// the directory the site at base is rendered into.
func resolves(root string, base, from *url.URL, link string) bool {
	ref, err := url.Parse(link)
	if err != nil {
		return false
	}
	target := from.ResolveReference(ref)
	rel, ok := strings.CutPrefix(target.Path, base.Path)
	if !ok || target.Host != base.Host {
		return false
	}
	if rel == "" || strings.HasSuffix(rel, "/") {
		rel += "index.html"
	}
	_, err = os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
	return err == nil
}

func TestSite(t *testing.T) {
	out := buildSite(t)
	require.Empty(t, siteProblems(t, out, siteBase))
	require.Empty(t, renditionProblems(t, out, siteBase))
	require.Empty(t, searchIndexProblems(t, out))

	var fixtures []string
	require.NoError(t, filepath.WalkDir(out, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() && d.Name() == "e2e" {
			fixtures = append(fixtures, p)
		}
		return err
	}))
	require.Empty(t, fixtures, "the site serves a story's e2e/ directory")
}

// searchIndexProblems returns a problem for each way the search index the
// front page's dialog names fails the pages under root: it is missing, or an
// entry has no title, or addresses a page root lacks or a fragment that page
// lacks.
func searchIndexProblems(t *testing.T, root string) []string {
	t.Helper()
	base, err := url.Parse(siteBase)
	require.NoError(t, err)
	index, err := os.ReadFile(filepath.Join(root, "index.html"))
	require.NoError(t, err)
	doc, err := html.Parse(bytes.NewReader(index))
	require.NoError(t, err)
	var address string
	for n := range doc.Descendants() {
		if n.Type == html.ElementNode && n.Data == "dialog" {
			for _, a := range n.Attr {
				if a.Key == "data-index" {
					address = a.Val
				}
			}
		}
	}
	rel, ok := strings.CutPrefix(address, base.Path)
	if !ok {
		return []string{"no search dialog names an index under " + base.Path}
	}
	b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	require.NoError(t, err)
	var entries []struct {
		URL   string `json:"u"`
		Title string `json:"t"`
	}
	require.NoError(t, json.Unmarshal(b, &entries))
	if len(entries) == 0 {
		return []string{"the search index is empty"}
	}
	ids := map[string]map[string]bool{}
	var problems []string
	for _, e := range entries {
		if e.Title == "" {
			problems = append(problems, e.URL+": no title")
		}
		u, err := url.Parse(e.URL)
		require.NoError(t, err)
		pagePath, ok := strings.CutPrefix(u.Path, base.Path)
		if !ok {
			problems = append(problems, e.URL+": not under "+base.Path)
			continue
		}
		file := filepath.Join(root, filepath.FromSlash(pagePath), "index.html")
		if _, seen := ids[file]; !seen {
			ids[file] = pageIDs(file)
		}
		switch {
		case ids[file] == nil:
			problems = append(problems, e.URL+": no such page")
		case u.Fragment != "" && !ids[file][u.Fragment]:
			problems = append(problems, e.URL+": no such fragment")
		}
	}
	sort.Strings(problems)
	return problems
}

// pageIDs returns the id of every element of the HTML file, or nil if it
// cannot be read.
func pageIDs(file string) map[string]bool {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil
	}
	doc, err := html.Parse(bytes.NewReader(b))
	if err != nil {
		return nil
	}
	ids := map[string]bool{}
	for n := range doc.Descendants() {
		for _, a := range n.Attr {
			if a.Key == "id" {
				ids[a.Val] = true
			}
		}
	}
	return ids
}

func TestSiteProblems(t *testing.T) {
	files := map[string]string{
		"index.html":        `<h1>Home</h1>`,
		"docs/index.html":   `<meta http-equiv="refresh" content="0; url=https://site.test/base/docs/a/">`,
		"docs/a/index.html": `<h1>A</h1><h2>S</h2>`,
		"docs/b/index.html": `<h1>B</h1><h1>B again</h1>`,
		"docs/c/index.html": `<meta http-equiv="refresh" content="0; url=../gone/">`,
		"docs/d/index.html": `<meta http-equiv="refresh" content="0; url=/docs/a/">`,
		"docs/e/index.html": `<meta http-equiv="refresh" content="0; url=https://elsewhere.test/base/docs/a/">`,
	}
	root := t.TempDir()
	for name, body := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
	}
	require.Equal(t, []string{
		"docs/b/index.html: more than one h1",
		"docs/c/index.html: refresh to ../gone/",
		"docs/d/index.html: refresh to /docs/a/",
		"docs/e/index.html: refresh to https://elsewhere.test/base/docs/a/",
	}, siteProblems(t, root, siteBase))
}
