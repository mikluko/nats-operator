package balance

import (
	"cmp"
	"slices"
)

// A LeaderMove is one group's leadership leaving its leader for To.
type LeaderMove struct {
	Group Group
	To    string
}

// PlanLeaders returns, in order, the leader moves of groups movable admits
// that even out who leads every group with a leader, each onto a member
// [Member.Takes] admits. The plan ends: even wherever every server is a
// member of every group, otherwise possibly one exchange short of even.
func PlanLeaders(groups []Group, movable func(Group) bool) []LeaderMove {
	leads := map[string]int{}
	var led []Group
	for _, g := range groups {
		if g.Leader == "" {
			continue
		}
		led = append(led, g)
		leads[g.Leader]++
	}
	slices.SortFunc(led, func(a, b Group) int { return cmp.Compare(a.String(), b.String()) })

	var moves []LeaderMove
	for {
		best, gain := -1, 1
		var to string
		for i, g := range led {
			if movable != nil && !movable(g) {
				continue
			}
			members := slices.SortedFunc(slices.Values(g.Members), func(a, b Member) int { return cmp.Compare(a.Name, b.Name) })
			for _, m := range members {
				if !m.Takes() {
					continue
				}
				if d := leads[g.Leader] - leads[m.Name]; d > gain {
					best, gain, to = i, d, m.Name
				}
			}
		}
		if best < 0 {
			return moves
		}
		g := led[best]
		moves = append(moves, LeaderMove{Group: g, To: to})
		leads[g.Leader]--
		leads[to]++
		led[best] = g.ledBy(to)
	}
}

// ledBy is g after its leadership moved to member to.
func (g Group) ledBy(to string) Group {
	out := g
	out.Leader = to
	out.Members = []Member{{Name: g.Leader, Current: true}}
	for _, m := range g.Members {
		if m.Name != to {
			out.Members = append(out.Members, m)
		}
	}
	return out
}
