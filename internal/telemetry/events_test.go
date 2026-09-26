package telemetry

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/events"
)

func TestEmit(t *testing.T) {
	rec := events.NewFakeRecorder(1)
	Emit(rec, &corev1.ConfigMap{}, UserKicked, "closed %d connections of %s", 2, "UABC")
	require.Equal(t, "Normal UserKicked closed 2 connections of UABC", <-rec.Events)
	require.NotPanics(t, func() { Emit(nil, &corev1.ConfigMap{}, UserKicked, "") })
}

// TestEventsEmitted pins Events, which the telemetry page lists, to the
// code: every Emit outside this package passes one of them by name, and
// each of them is passed somewhere.
func TestEventsEmitted(t *testing.T) {
	listed := eventsListed(t)
	var emitted []string
	fset := token.NewFileSet()
	for _, dir := range []string{"../../internal", "../../cmd"} {
		require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			if filepath.Dir(path) == "../../internal/telemetry" {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || !isSelector(call.Fun, "telemetry", "Emit") {
					return true
				}
				require.GreaterOrEqual(t, len(call.Args), 3, fset.Position(call.Pos()).String())
				sel, ok := call.Args[2].(*ast.SelectorExpr)
				require.True(t, ok && isSelector(sel, "telemetry", sel.Sel.Name), "%s: the event is not a telemetry variable", fset.Position(call.Pos()))
				require.Contains(t, listed, sel.Sel.Name, "%s: %s is not in Events", fset.Position(call.Pos()), sel.Sel.Name)
				emitted = append(emitted, sel.Sel.Name)
				return true
			})
			return nil
		}))
	}
	for _, name := range listed {
		require.Contains(t, emitted, name, "%s is in Events and emitted nowhere", name)
	}
}

// eventsListed are the names in the Events composite literal, each checked
// to be one of the Event variables of events.go.
func eventsListed(t *testing.T) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "events.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err)
	var declared, listed []string
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, name := range vs.Names {
				lit, ok := vs.Values[i].(*ast.CompositeLit)
				require.True(t, ok, name.Name)
				if name.Name == "Events" {
					for _, el := range lit.Elts {
						listed = append(listed, el.(*ast.Ident).Name)
					}
					continue
				}
				declared = append(declared, name.Name)
			}
		}
	}
	slices.Sort(declared)
	sorted := slices.Sorted(slices.Values(listed))
	require.Equal(t, declared, sorted, "every Event variable is in Events, once")
	require.Len(t, listed, len(Events))
	return listed
}

func isSelector(e ast.Expr, pkg, name string) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg
}
