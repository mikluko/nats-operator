package balance

import (
	"cmp"
	"slices"
)

// DefaultPool is the name of the pool of an account's streams that no declared
// pool claims.
const DefaultPool = "(default)"

// A Pool is a set of streams judged for evenness apart from every other; each
// stream's consumers go with it.
type Pool struct {
	Name    string
	Streams []StreamID
}

// A Pooler partitions the groups a pass observed into pools.
type Pooler func(groups []Group) []Pool

// WholeCluster is the system balancer's partition: every stream the NATS
// cluster holds, over every account, in one pool.
func WholeCluster(groups []Group) []Pool {
	return []Pool{{Name: DefaultPool, Streams: streamsOf(groups, func(StreamID) bool { return true })}}
}

// Declared is the account balancer's partition: the declared pools, each
// stream in the first that names it, then [DefaultPool] holding the account's
// observed streams no pool names. Pools left empty are dropped.
func Declared(account string, declared []Pool) Pooler {
	return func(groups []Group) []Pool {
		claimed := map[StreamID]bool{}
		var out []Pool
		for _, p := range declared {
			var streams []StreamID
			for _, s := range p.Streams {
				if s.Account != account || claimed[s] {
					continue
				}
				claimed[s] = true
				streams = append(streams, s)
			}
			if len(streams) > 0 {
				out = append(out, Pool{Name: p.Name, Streams: streams})
			}
		}
		rest := streamsOf(groups, func(s StreamID) bool { return s.Account == account && !claimed[s] })
		if len(rest) > 0 {
			out = append(out, Pool{Name: DefaultPool, Streams: rest})
		}
		return out
	}
}

// streamsOf is every stream groups names that keep admits, sorted.
func streamsOf(groups []Group, keep func(StreamID) bool) []StreamID {
	var out []StreamID
	for _, g := range groups {
		if id := g.ID(); keep(id) && !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	slices.SortFunc(out, compareIDs)
	return out
}

func compareIDs(a, b StreamID) int {
	return cmp.Or(cmp.Compare(a.Account, b.Account), cmp.Compare(a.Stream, b.Stream))
}

// split is the groups of p: its streams' own, and their consumers'.
func (p Pool) split(groups []Group) (streams, consumers []Group) {
	for _, g := range groups {
		switch {
		case !slices.Contains(p.Streams, g.ID()):
		case g.Consumer == "":
			streams = append(streams, g)
		default:
			consumers = append(consumers, g)
		}
	}
	return streams, consumers
}
