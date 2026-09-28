package balance

import (
	"cmp"
	"slices"
	"strings"
)

// A PlacementMove is one stream's copy leaving From. The server picks where
// the copy lands; the balancer cannot name it.
type PlacementMove struct {
	Group Group
	From  string
	// Cluster is the NATS cluster the move stays in.
	Cluster string
	// Unsure marks a move that only evens the pool if the server picks well.
	Unsure bool
}

// Target is how many copies each of servers should hold of copies: the floor
// of the share, with the remainder at the ceiling one apiece.
func Target(copies, servers int) (floor, ceiling, atCeiling int) {
	if servers <= 0 {
		return 0, 0, 0
	}
	floor = copies / servers
	atCeiling = copies % servers
	ceiling = floor
	if atCeiling > 0 {
		ceiling++
	}
	return floor, ceiling, atCeiling
}

// Landings is every server of roster that could take a copy of g in a move
// within cluster: not already holding one, and carrying every tag g's placement
// declares. It is empty for a stream whose declared cluster is another, since
// no balancer overrides declared placement.
//
// A placement move is requested only where this is not empty: with no eligible
// server in its own NATS cluster the server moves the stream to another NATS
// cluster rather than refusing.
func Landings(g Group, roster []Server, cluster string) []string {
	var tags []string
	if p := g.Placement; p != nil {
		if p.Cluster != "" && p.Cluster != cluster {
			return nil
		}
		tags = p.Tags
	}
	var out []string
	for _, s := range roster {
		if !g.holds(s.Name) && carries(s.Tags, tags) {
			out = append(out, s.Name)
		}
	}
	return out
}

// carries reports whether have holds every tag of want. Tags compare
// case-insensitively, as the server compares them.
func carries(have, want []string) bool {
	for _, w := range want {
		if !slices.ContainsFunc(have, func(h string) bool { return strings.EqualFold(h, w) }) {
			return false
		}
	}
	return true
}

// PlanPlacement returns the next placement move that evens out which servers
// of roster hold copies of streams, and how many copies sit above an even
// share. A group skip admits is counted and never moved.
//
// A move that evens the pool whatever the server picks comes before an Unsure
// one, and a group tried admits is never offered an Unsure one.
func PlanPlacement(streams []Group, roster []Server, cluster string, skip, tried func(Group) bool) (*PlacementMove, int) {
	holds := make(map[string]int, len(roster))
	for _, s := range roster {
		holds[s.Name] = 0
	}
	var copies int
	for _, g := range streams {
		for _, h := range g.Holders() {
			if _, ok := holds[h]; ok {
				holds[h]++
				copies++
			}
		}
	}
	_, ceiling, _ := Target(copies, len(holds))
	var pending int
	for _, n := range holds {
		pending += max(0, n-ceiling)
	}

	streams = slices.SortedFunc(slices.Values(streams), func(a, b Group) int { return cmp.Compare(a.String(), b.String()) })
	var best, unsure *PlacementMove
	var gain, hope int
	for _, g := range streams {
		if g.Leader == "" || g.Offline || (skip != nil && skip(g)) {
			continue
		}
		var landings []int
		for _, s := range Landings(g, roster, cluster) {
			landings = append(landings, holds[s])
		}
		if len(landings) == 0 {
			continue
		}
		lightest, heaviest := slices.Min(landings), slices.Max(landings)
		for _, from := range slices.Sorted(slices.Values(g.Holders())) {
			if _, ok := holds[from]; !ok {
				continue
			}
			worst, most := holds[from]-heaviest, holds[from]-lightest
			switch {
			case worst >= 2 && worst > gain:
				best, gain = &PlacementMove{Group: g, From: from, Cluster: cluster}, worst
			case worst == 1 && most >= 2 && most > hope && (tried == nil || !tried(g)):
				unsure, hope = &PlacementMove{Group: g, From: from, Cluster: cluster, Unsure: true}, most
			}
		}
	}
	if best == nil {
		best = unsure
	}
	return best, pending
}
