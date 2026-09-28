package authctl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMergeRoster pins the roster's memory: a server stays in it until it
// has missed rosterMisses polls in a row, and answering again clears its
// misses.
func TestMergeRoster(t *testing.T) {
	answered := func(ids ...string) map[string]bool {
		out := map[string]bool{}
		for _, id := range ids {
			out[id] = true
		}
		return out
	}
	tests := []struct {
		name        string
		prev        map[string]int
		answered    map[string]bool
		want        map[string]int
		wantChanged bool
	}{
		{"first poll", nil, answered("a", "b"), map[string]int{"a": 0, "b": 0}, true},
		{"all answer", map[string]int{"a": 0, "b": 0}, answered("a", "b"), map[string]int{"a": 0, "b": 0}, false},
		{"one misses", map[string]int{"a": 0, "b": 0}, answered("a"), map[string]int{"a": 0, "b": 1}, false},
		{"one misses again", map[string]int{"a": 0, "b": 1}, answered("a"), map[string]int{"a": 0, "b": 2}, false},
		{"one gone", map[string]int{"a": 0, "b": rosterMisses - 1}, answered("a"), map[string]int{"a": 0}, true},
		{"one back", map[string]int{"a": 0, "b": 2}, answered("a", "b"), map[string]int{"a": 0, "b": 0}, true},
		{"one joins", map[string]int{"a": 0}, answered("a", "c"), map[string]int{"a": 0, "c": 0}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, changed := mergeRoster(tt.prev, tt.answered)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantChanged, changed)
		})
	}
}
