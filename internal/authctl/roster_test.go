package authctl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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

// TestSetRoster_Trust pins which servers count as known to trust a key, and
// that a change in what a server reports on VARZ is a roster change.
func TestSetRoster_Trust(t *testing.T) {
	r := &Resolvers{}
	st := &resolverState{}
	all := map[string]bool{"a": true, "b": true, "c": true}
	trust := func(st *resolverState) [2]int { return [2]int{st.distrusting(all, "K"), st.unknown(all)} }

	require.True(t, r.setRoster(st, all, map[string][]string{"a": {"K"}, "b": nil}))
	require.Equal(t, [2]int{0, 2}, trust(st), "b lists no key and c did not answer VARZ")

	require.False(t, r.setRoster(st, all, map[string][]string{"a": {"K"}}), "a server that misses a VARZ poll keeps what it reported")
	require.Equal(t, [2]int{0, 2}, trust(st))

	require.True(t, r.setRoster(st, all, map[string][]string{"c": {"J"}}), "c answered VARZ")
	require.Equal(t, [2]int{1, 1}, trust(st))

	require.True(t, r.setRoster(st, all, map[string][]string{"a": nil}), "a reports no NATS operator JWT")
	require.Equal(t, [2]int{1, 2}, trust(st))
}
