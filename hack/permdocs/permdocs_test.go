package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuin/goldmark"
	gast "github.com/yuin/goldmark/ast"
	gparser "github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

const root = "../.."

// scanned are the packages whose subjects the page must account for.
var scanned = []string{
	"internal/authctl",
	"internal/balance",
	"internal/balancectl",
	"internal/lifecycle",
	"internal/natscluster",
	"internal/natsconn",
	"internal/streamctl",
	"internal/sysobs",
}

// subjectBuilders are the jwtplane functions that return a subject, by name.
var subjectBuilders = map[string]string{
	"StreamStepdownSubject":   jwtplane.StreamStepdownSubject("*", "*"),
	"ConsumerStepdownSubject": jwtplane.ConsumerStepdownSubject("*", "*", "*"),
}

// used is a subject a package's code sends to. Prefixed is whether code
// puts an unknown string before it, which may be empty.
type used struct {
	Subject  string
	Prefixed bool
	Pos      string
}

func (u used) matches(subject string) bool {
	return subject == u.Subject || (u.Prefixed && subject == jwtplane.StepdownPrefix("*")+u.Subject)
}

// scan returns the subjects the non-test files of dir send to: string
// literals and concatenations starting "$SYS." or "$JS.", with each "%s" and
// each non-literal operand read as "*", and calls to subjectBuilders.
func scan(t *testing.T, dir string) []used {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, dir, "*.go"))
	require.NoError(t, err)
	fset := token.NewFileSet()
	var out []used
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		ast.Inspect(f, func(n ast.Node) bool {
			pos := func() string { return fset.Position(n.Pos()).String() }
			switch n := n.(type) {
			case *ast.BinaryExpr:
				if n.Op != token.ADD {
					return true
				}
				u, ok := concatenation(t, n)
				if !ok {
					return true
				}
				u.Pos = pos()
				out = append(out, u)
				return false
			case *ast.BasicLit:
				if v, ok := subjectLiteral(t, n); ok {
					out = append(out, used{Subject: shape(t, v, pos()), Pos: pos()})
				}
			case *ast.CallExpr:
				sel, ok := n.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "jwtplane" || !strings.HasSuffix(sel.Sel.Name, "Subject") {
					return true
				}
				subject, ok := subjectBuilders[sel.Sel.Name]
				require.True(t, ok, "%s: jwtplane.%s is missing from subjectBuilders", pos(), sel.Sel.Name)
				out = append(out, used{Subject: subject, Pos: pos()})
				return false
			}
			return true
		})
	}
	return out
}

func subjectLiteral(t *testing.T, e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	v, err := strconv.Unquote(lit.Value)
	require.NoError(t, err)
	return v, strings.HasPrefix(v, "$SYS.") || strings.HasPrefix(v, "$JS.")
}

// concatenation reads e as a subject when one of its operands is a subject
// literal.
func concatenation(t *testing.T, e *ast.BinaryExpr) (used, bool) {
	var operands []ast.Expr
	var flatten func(ast.Expr)
	flatten = func(e ast.Expr) {
		switch x := e.(type) {
		case *ast.BinaryExpr:
			if x.Op == token.ADD {
				flatten(x.X)
				flatten(x.Y)
				return
			}
		case *ast.ParenExpr:
			flatten(x.X)
			return
		}
		operands = append(operands, e)
	}
	flatten(e)
	var b strings.Builder
	found := false
	for _, op := range operands {
		if lit, ok := op.(*ast.BasicLit); !ok || lit.Kind != token.STRING {
			b.WriteString("\x00")
			continue
		}
		v, ok := subjectLiteral(t, op)
		found = found || ok
		b.WriteString(v)
	}
	if !found {
		return used{}, false
	}
	s := b.String()
	prefixed := strings.HasPrefix(s, "\x00$")
	if prefixed {
		s = s[1:]
	}
	return used{Subject: shape(t, s, ""), Prefixed: prefixed}, true
}

// shape turns each "%s" and each "\x00" token of s into "*".
func shape(t *testing.T, s, pos string) string {
	tokens := strings.Split(s, ".")
	for i, tok := range tokens {
		if tok == "%s" || tok == "\x00" {
			tokens[i] = "*"
			continue
		}
		require.NotContains(t, tok, "\x00", "%s: %q has a computed part inside a token", pos, s)
		require.NotContains(t, tok, "%", "%s: %q has a verb inside a token", pos, s)
	}
	return strings.Join(tokens, ".")
}

// covers is whether a permission on grant allows publishing to every subject
// that subject stands for.
func covers(grant, subject string) bool {
	g, s := strings.Split(grant, "."), strings.Split(subject, ".")
	for i, tok := range g {
		if tok == ">" {
			return i < len(s)
		}
		if i >= len(s) || (tok != "*" && tok != s[i]) {
			return false
		}
	}
	return len(g) == len(s)
}

func TestCovers(t *testing.T) {
	tests := []struct {
		grant, subject string
		want           bool
	}{
		{"$SYS.REQ.SERVER.*.VARZ", "$SYS.REQ.SERVER.*.VARZ", true},
		{"$SYS.REQ.SERVER.PING.STATSZ", "$SYS.REQ.SERVER.*.STATSZ", false},
		{"_INBOX.>", "_INBOX.>", true},
		{"_INBOX.>", "_INBOX", false},
		{"$JS.API.>", "$JS.API.STREAM.INFO.*", true},
		{"$JS.API.STREAM.INFO.*", "$JS.API.STREAM.INFO.*.*", false},
		{"$JS.API.STREAM.INFO.*.*", "$JS.API.STREAM.INFO.*", false},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, covers(tt.grant, tt.subject), "%s covers %s", tt.grant, tt.subject)
	}
}

func TestCallsMatchCode(t *testing.T) {
	for _, dir := range scanned {
		t.Run(dir, func(t *testing.T) {
			found := scan(t, dir)
			var listed []string
			for _, id := range identities {
				for _, c := range id.Calls {
					if slices.Contains(c.Sources, dir) {
						listed = append(listed, c.Subject)
					}
				}
			}
			for _, u := range found {
				require.True(t, slices.ContainsFunc(listed, u.matches), "%s: %s is on no call the page lists under %s", u.Pos, u.Subject, dir)
			}
			for _, subject := range listed {
				require.True(t, slices.ContainsFunc(found, func(u used) bool { return u.matches(subject) }), "the page lists %s under %s, whose code does not send to it", subject, dir)
			}
		})
	}
	for _, id := range identities {
		for _, c := range id.Calls {
			for _, src := range c.Sources {
				require.True(t, src == natsGo || slices.Contains(scanned, src), "%s names %s, which is not scanned", c.Subject, src)
			}
		}
	}
}

func TestPresetsGrantCalls(t *testing.T) {
	for _, id := range identities {
		if id.Preset == "" {
			continue
		}
		t.Run(id.Heading, func(t *testing.T) {
			g, ok := jwtplane.UserPresetGrant(id.Preset)
			require.True(t, ok)
			require.True(t, g.SystemAccount, "%s is not a system account preset", id.Preset)
			inbox := jwtplane.InboxPrefix(id.Preset) + ".>"
			require.True(t, slices.ContainsFunc(g.Subscribe, func(grant string) bool { return covers(grant, inbox) }), "%s does not grant %s", id.Preset, inbox)
			for _, c := range id.Calls {
				require.True(t, slices.ContainsFunc(g.Publish, func(grant string) bool { return covers(grant, c.Subject) }), "%s does not grant %s", id.Preset, c.Subject)
			}
		})
	}
}

func TestPresetsGrantNoMore(t *testing.T) {
	calls := map[jwtplane.UserPreset][]string{}
	for _, id := range identities {
		if id.Preset == "" {
			continue
		}
		for _, c := range id.Calls {
			calls[id.Preset] = append(calls[id.Preset], c.Subject)
		}
	}
	for preset, subjects := range calls {
		t.Run(string(preset), func(t *testing.T) {
			g, ok := jwtplane.UserPresetGrant(preset)
			require.True(t, ok)
			for _, grant := range g.Publish {
				require.True(t, slices.ContainsFunc(subjects, func(s string) bool { return covers(s, grant) }), "%s grants %s, beyond every call of the page", preset, grant)
			}
			require.Equal(t, []string{jwtplane.InboxPrefix(preset) + ".>"}, g.Subscribe)
		})
	}
}

func TestStepdownCallsAreImports(t *testing.T) {
	var local []string
	for _, i := range jwtplane.StepdownImports("*") {
		local = append(local, i.LocalSubject)
	}
	for _, id := range identities {
		for _, c := range id.Calls {
			if strings.HasPrefix(c.Subject, jwtplane.StepdownPrefix("*")) {
				require.Contains(t, local, c.Subject)
			}
		}
	}
}

func TestPageCurrent(t *testing.T) {
	want, err := render()
	require.NoError(t, err)
	got, err := os.ReadFile(filepath.Join(root, page))
	require.NoError(t, err)
	require.Equal(t, want, string(got), "%s is stale: run just perm-docs", page)
}

// TestUserPresetUnknown pins that a preset without a grant fails the page
// rather than rendering as unrestricted.
func TestUserPresetUnknown(t *testing.T) {
	var b strings.Builder
	require.ErrorContains(t, userPreset(&b, jwtplane.UserPreset("no-such-preset")), "no-such-preset")
	require.Empty(t, b.String())
}

// TestPresetLinks pins every in-page link to the heading that carries its
// text, so an identity's preset link lands on the preset and not on a
// heading of the same name.
func TestPresetLinks(t *testing.T) {
	page, err := render()
	require.NoError(t, err)
	src := []byte(strings.TrimPrefix(page, frontMatter))
	doc := goldmark.New(goldmark.WithParserOptions(gparser.WithAutoHeadingID())).Parser().Parse(text.NewReader(src))
	headings := map[string]string{}
	var links []*gast.Link
	require.NoError(t, gast.Walk(doc, func(n gast.Node, entering bool) (gast.WalkStatus, error) {
		if !entering {
			return gast.WalkContinue, nil
		}
		switch n := n.(type) {
		case *gast.Heading:
			id, ok := n.AttributeString("id")
			require.True(t, ok)
			headings[string(id.([]byte))] = plain(n, src)
		case *gast.Link:
			if strings.HasPrefix(string(n.Destination), "#") {
				links = append(links, n)
			}
		}
		return gast.WalkContinue, nil
	}))
	require.NotEmpty(t, links)
	for _, l := range links {
		anchor := strings.TrimPrefix(string(l.Destination), "#")
		require.Equal(t, plain(l, src), headings[anchor], "link #%s", anchor)
	}
}

// plain returns the text of n's inline children, code spans included.
func plain(n gast.Node, src []byte) string {
	var b strings.Builder
	for c := n.FirstChild(); c != nil; c = c.NextSibling() {
		switch c := c.(type) {
		case *gast.Text:
			b.Write(c.Value(src))
		case *gast.CodeSpan:
			b.WriteString(plain(c, src))
		}
	}
	return b.String()
}
