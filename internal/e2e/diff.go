package e2e

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// Mismatch is one field where Diff's got fails to contain its want.
type Mismatch struct {
	Path string
	Want string
	// Got is "" when the field is absent.
	Got string
}

func (m Mismatch) String() string {
	got := m.Got
	if got == "" {
		got = "<absent>"
	}
	return fmt.Sprintf("%s: want %s, got %s", m.Path, m.Want, got)
}

// Diff returns every way got fails to contain want by the matching rules
// hack/e2e/README.md states, ordered by path; none means it does.
func Diff(want, got map[string]any) []Mismatch {
	return diffValue("", want, got, true)
}

func diffValue(path string, want, got any, present bool) []Mismatch {
	switch w := want.(type) {
	case placeholder:
		return nil
	case map[string]any:
		g, _ := got.(map[string]any)
		if present && g == nil {
			return []Mismatch{{Path: path, Want: "an object", Got: encode(got)}}
		}
		var out []Mismatch
		for _, k := range sortedKeys(w) {
			gv, ok := g[k]
			if k == "conditions" {
				out = append(out, diffConditions(path+"."+k, w[k], gv, ok)...)
				continue
			}
			out = append(out, diffValue(path+"."+k, w[k], gv, ok)...)
		}
		return out
	case []any:
		g, isList := got.([]any)
		if present && !isList {
			return []Mismatch{{Path: path, Want: "a list", Got: encode(got)}}
		}
		if present && len(g) != len(w) {
			return []Mismatch{{Path: path, Want: fmt.Sprintf("%d items", len(w)), Got: fmt.Sprintf("%d items", len(g))}}
		}
		var out []Mismatch
		for i, item := range w {
			var gv any
			if present {
				gv = g[i]
			}
			out = append(out, diffValue(fmt.Sprintf("%s[%d]", path, i), item, gv, present)...)
		}
		return out
	default:
		if !present {
			if isZero(want) {
				return nil
			}
			return []Mismatch{{Path: path, Want: encode(want)}}
		}
		if encode(want) != encode(got) {
			return []Mismatch{{Path: path, Want: encode(want), Got: encode(got)}}
		}
		return nil
	}
}

func diffConditions(path string, want, got any, present bool) []Mismatch {
	w, ok := want.([]any)
	if !ok {
		return diffValue(path, want, got, present)
	}
	live := map[string]map[string]any{}
	if g, ok := got.([]any); ok {
		for _, c := range g {
			if m, ok := c.(map[string]any); ok {
				live[fmt.Sprint(m["type"])] = m
			}
		}
	}
	var out []Mismatch
	for _, c := range w {
		m, _ := c.(map[string]any)
		typ := fmt.Sprint(m["type"])
		lc, ok := live[typ]
		p := fmt.Sprintf("%s[type=%s].status", path, typ)
		switch {
		case !ok:
			out = append(out, Mismatch{Path: p, Want: encode(m["status"])})
		case m["status"] == (placeholder{}):
		case encode(m["status"]) != encode(lc["status"]):
			out = append(out, Mismatch{Path: p, Want: encode(m["status"]), Got: encode(lc["status"])})
		}
	}
	return out
}

func isZero(v any) bool {
	switch v := v.(type) {
	case nil:
		return true
	case bool:
		return !v
	case string:
		return v == ""
	case int:
		return v == 0
	case int64:
		return v == 0
	case float64:
		return v == 0
	}
	return false
}

func encode(v any) string {
	if v == (placeholder{}) {
		return "any value"
	}

	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return fmt.Sprintf("%v", v)
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// FormatMismatches renders ms one per line, each prefixed by indent.
func FormatMismatches(ms []Mismatch, indent string) string {
	var b strings.Builder
	for _, m := range ms {
		b.WriteString(indent)
		b.WriteString(m.String())
		b.WriteByte('\n')
	}
	return b.String()
}
