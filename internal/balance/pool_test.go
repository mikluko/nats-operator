package balance

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPoolers(t *testing.T) {
	id := func(account, stream string) StreamID { return StreamID{account, stream} }
	groups := []Group{
		{Account: "a", Stream: "req_1"}, {Account: "a", Stream: "req_1", Consumer: "c"},
		{Account: "a", Stream: "req_2"}, {Account: "a", Stream: "res_1"}, {Account: "a", Stream: "kv"},
		{Account: "b", Stream: "orders"},
	}
	for _, tc := range []struct {
		name  string
		pools Pooler
		want  []Pool
	}{
		{"the whole cluster is one pool over every account", WholeCluster, []Pool{{DefaultPool, []StreamID{
			id("a", "kv"), id("a", "req_1"), id("a", "req_2"), id("a", "res_1"), id("b", "orders"),
		}}}},
		{"no pools declared is one default pool of the account", Declared("a", nil), []Pool{{DefaultPool, []StreamID{
			id("a", "kv"), id("a", "req_1"), id("a", "req_2"), id("a", "res_1"),
		}}}},
		{"a stream in several pools belongs to the first", Declared("a", []Pool{
			{"requests", []StreamID{id("a", "req_1"), id("a", "req_2")}},
			{"responses", []StreamID{id("a", "res_1"), id("a", "req_2")}},
		}), []Pool{
			{"requests", []StreamID{id("a", "req_1"), id("a", "req_2")}},
			{"responses", []StreamID{id("a", "res_1")}},
			{DefaultPool, []StreamID{id("a", "kv")}},
		}},
		{"another account's stream and an emptied pool are dropped", Declared("a", []Pool{
			{"requests", []StreamID{id("a", "req_1"), id("a", "req_2"), id("a", "res_1"), id("a", "kv")}},
			{"theirs", []StreamID{id("b", "orders")}},
		}), []Pool{
			{"requests", []StreamID{id("a", "req_1"), id("a", "req_2"), id("a", "res_1"), id("a", "kv")}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, tc.pools(groups)) })
	}
}

func TestPool_Split(t *testing.T) {
	p := Pool{Name: "requests", Streams: []StreamID{{"a", "req_0"}, {"a", "req_1"}}}
	groups := []Group{
		{Account: "a", Stream: "req_0"}, {Account: "a", Stream: "req_1"}, {Account: "a", Stream: "other"},
		{Account: "a", Stream: "req_0", Consumer: "tick"}, {Account: "b", Stream: "req_0"},
	}
	streams, consumers := p.split(groups)
	require.Equal(t, []Group{groups[0], groups[1]}, streams)
	require.Equal(t, []Group{groups[3]}, consumers)
}
