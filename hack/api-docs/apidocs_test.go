package apidocs_test

import (
	"bytes"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuin/goldmark"
	gast "github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	east "github.com/yuin/goldmark/extension/ast"
	gparser "github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

const (
	page    = "../../docs/content/docs/reference/api.md"
	apiDir  = "../../api"
	apiPath = "github.com/mikluko/nats-operator/api/"
)

// section is one type's part of the page: the first-column code spans of
// the table under its heading.
type section struct {
	rows []string
}

type apiPage struct {
	front    map[string]any
	sections map[string]*section
	// anchors are the in-page link targets, without the leading #.
	anchors []string
}

// readPage parses the page: its front matter, a section per type heading,
// and every in-page link. Tables under a group heading list its kinds.
func readPage(t *testing.T) apiPage {
	t.Helper()
	src, err := os.ReadFile(page)
	require.NoError(t, err)

	delim := []byte("---\n")
	require.True(t, bytes.HasPrefix(src, delim), "the page opens with YAML front matter")
	fm, body, ok := bytes.Cut(src[len(delim):], delim)
	require.True(t, ok, "the front matter is closed")
	p := apiPage{sections: map[string]*section{}}
	require.NoError(t, yaml.Unmarshal(fm, &p.front))

	md := goldmark.New(
		goldmark.WithExtensions(extension.Table),
		goldmark.WithParserOptions(gparser.WithHeadingAttribute()),
	)
	doc := md.Parser().Parse(text.NewReader(body))

	var cur *section
	for n := doc.FirstChild(); n != nil; n = n.NextSibling() {
		switch n := n.(type) {
		case *gast.Heading:
			cur = nil
			if n.Level != 3 {
				continue
			}
			id, ok := n.AttributeString("id")
			require.True(t, ok, "every type heading carries an ID")
			key := string(id.([]byte))
			require.NotContains(t, p.sections, key, "heading IDs are unique")
			cur = &section{}
			p.sections[key] = cur
		case *east.Table:
			if cur == nil {
				continue
			}
			for row := n.FirstChild(); row != nil; row = row.NextSibling() {
				if _, ok := row.(*east.TableRow); !ok {
					continue
				}
				code, ok := row.FirstChild().FirstChild().(*gast.CodeSpan)
				require.True(t, ok, "a row's first cell is a code span")
				cur.rows = append(cur.rows, codeText(code, body))
			}
		}
	}
	require.NoError(t, gast.Walk(doc, func(n gast.Node, entering bool) (gast.WalkStatus, error) {
		if l, ok := n.(*gast.Link); ok && entering {
			if a, ok := bytes.CutPrefix(l.Destination, []byte("#")); ok {
				p.anchors = append(p.anchors, string(a))
			}
		}
		return gast.WalkContinue, nil
	}))
	return p
}

func codeText(code *gast.CodeSpan, src []byte) string {
	var b strings.Builder
	for c := code.FirstChild(); c != nil; c = c.NextSibling() {
		if t, ok := c.(*gast.Text); ok {
			b.Write(t.Segment.Value(src))
		}
	}
	return b.String()
}

// jsonFields returns the JSON names of a struct's fields in declaration
// order, with inline fields flattened in place.
func jsonFields(typ reflect.Type) []string {
	var out []string
	for f := range typ.Fields() {
		name, opts, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if f.Anonymous && strings.Contains(opts, "inline") {
			if f.Type.Name() == "TypeMeta" {
				out = append(out, "apiVersion", "kind")
				continue
			}
			out = append(out, jsonFields(f.Type)...)
			continue
		}
		out = append(out, name)
	}
	return out
}

// apiTypes returns every named type in api/ reachable from a registered
// kind, lists aside, keyed by name.
func apiTypes(t *testing.T) (kinds, types map[string]reflect.Type) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		natsv1beta1.AddToScheme, clusterv1beta1.AddToScheme,
		authv1beta1.AddToScheme, jetstreamv1beta1.AddToScheme,
	} {
		require.NoError(t, add(scheme))
	}

	kinds, types = map[string]reflect.Type{}, map[string]reflect.Type{}
	var walk func(reflect.Type)
	walk = func(typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if !strings.HasPrefix(typ.PkgPath(), apiPath) || types[typ.Name()] != nil {
			return
		}
		types[typ.Name()] = typ
		if typ.Kind() == reflect.Struct {
			for f := range typ.Fields() {
				walk(f.Type)
			}
		}
	}
	for gvk, typ := range scheme.AllKnownTypes() {
		if !strings.HasPrefix(typ.PkgPath(), apiPath) || strings.HasSuffix(gvk.Kind, "List") {
			continue
		}
		kinds[gvk.Kind] = typ
		walk(typ)
	}
	require.NotEmpty(t, kinds)
	return kinds, types
}

func TestPageFrontMatter(t *testing.T) {
	p := readPage(t)
	require.Equal(t, "API reference", p.front["title"])
	require.NotEmpty(t, p.front["description"])
}

func TestPageAnchorsResolve(t *testing.T) {
	p := readPage(t)
	require.NotEmpty(t, p.anchors)
	for _, a := range p.anchors {
		require.Contains(t, p.sections, a)
	}
}

// TestPageCoversEveryType pins the page to the types: a section per kind
// and per type a kind reaches, a row per field in declaration order, and a
// row per value for a string type with constants.
func TestPageCoversEveryType(t *testing.T) {
	sections := readPage(t).sections
	kinds, types := apiTypes(t)
	for name, typ := range types {
		s, ok := sections[name]
		require.True(t, ok, "%s has a section", name)
		switch typ.Kind() {
		case reflect.Struct:
			require.Equal(t, jsonFields(typ), s.rows, "%s: a row per field", name)
		case reflect.String:
			require.NotEmpty(t, s.rows, "%s: a row per value", name)
		}
	}
	for kind := range kinds {
		require.Contains(t, sections, kind)
	}
}

func decls(t *testing.T) []*ast.GenDecl {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(apiDir, "*", "v1beta1", "*.go"))
	require.NoError(t, err)
	var out []*ast.GenDecl
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasPrefix(filepath.Base(f), "zz_generated") || strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, parser.ParseComments)
		require.NoError(t, err)
		for _, d := range file.Decls {
			if g, ok := d.(*ast.GenDecl); ok {
				out = append(out, g)
			}
		}
	}
	return out
}

// prose returns a comment group's text with marker lines left out.
func prose(groups ...*ast.CommentGroup) string {
	var lines []string
	for _, g := range groups {
		for line := range strings.Lines(g.Text()) {
			if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "+") {
				lines = append(lines, line)
			}
		}
	}
	return strings.Join(lines, " ")
}

// mirrors are the types whose doc names the nats-server or nats.go config
// they copy; a field of theirs without a doc means what it means there.
var mirrors = []string{"StreamConfig", "StreamConsumerLimits", "ConsumerConfig", "KeyValueConfig", "ObjectStoreConfig"}

// undocumented reports whether a field needs no doc of its own: embedded
// fields, whose members carry theirs; a kind's or a template's metadata,
// spec, status and items, which the types they name document; and a
// mirror's fields.
func undocumented(typ string, f *ast.Field) bool {
	if len(f.Names) == 0 || slices.Contains(mirrors, typ) {
		return true
	}
	return slices.Contains([]string{"ObjectMeta", "ListMeta", "Metadata", "Spec", "Status", "Items"}, f.Names[0].Name)
}

// TestEveryTypeAndFieldDocumented pins that the page has prose for every
// type and field: an undocumented one renders as an empty cell.
func TestEveryTypeAndFieldDocumented(t *testing.T) {
	var missing []string
	for _, g := range decls(t) {
		if g.Tok != token.TYPE {
			continue
		}
		for _, spec := range g.Specs {
			ts := spec.(*ast.TypeSpec)
			if !ts.Name.IsExported() || strings.HasSuffix(ts.Name.Name, "List") {
				continue
			}
			doc := ts.Doc
			if doc == nil {
				doc = g.Doc
			}
			if prose(doc) == "" {
				missing = append(missing, ts.Name.Name)
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}
			for _, f := range st.Fields.List {
				if undocumented(ts.Name.Name, f) {
					continue
				}
				if prose(f.Doc) == "" {
					missing = append(missing, ts.Name.Name+"."+f.Names[0].Name)
				}
			}
		}
	}
	require.Empty(t, missing, "types and fields without a doc")
}

// TestEnumValuesAreConstants pins that a string type's enum marker and its
// constants name the same values, since the page lists the constants.
func TestEnumValuesAreConstants(t *testing.T) {
	const marker = "+kubebuilder:validation:Enum="
	enums := map[string][]string{}
	consts := map[string][]string{}
	for _, g := range decls(t) {
		switch g.Tok {
		case token.TYPE:
			for _, spec := range g.Specs {
				ts := spec.(*ast.TypeSpec)
				doc := ts.Doc
				if doc == nil {
					doc = g.Doc
				}
				if doc == nil {
					continue
				}
				for _, c := range doc.List {
					if v, ok := strings.CutPrefix(strings.TrimPrefix(c.Text, "// "), marker); ok {
						enums[ts.Name.Name] = strings.Split(v, ";")
					}
				}
			}
		case token.CONST:
			for _, spec := range g.Specs {
				vs := spec.(*ast.ValueSpec)
				id, ok := vs.Type.(*ast.Ident)
				if !ok || len(vs.Values) != 1 {
					continue
				}
				lit, ok := vs.Values[0].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				consts[id.Name] = append(consts[id.Name], constant.StringVal(constant.MakeFromLiteral(lit.Value, lit.Kind, 0)))
			}
		}
	}
	require.NotEmpty(t, enums)
	for name, values := range enums {
		require.ElementsMatch(t, values, consts[name], "%s: enum marker and constants", name)
	}
}
