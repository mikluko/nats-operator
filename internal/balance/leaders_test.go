package balance

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// group is account a's stream led by leader, every other server of servers a
// current member.
func group(stream, leader string, servers ...string) Group {
	g := Group{Account: "a", Stream: stream, Leader: leader}
	for _, s := range servers {
		if s != leader {
			g.Members = append(g.Members, Member{Name: s, Current: true})
		}
	}
	return g
}

// leadsAfter is how many of groups each server leads once moves are made,
// every member at zero.
func leadsAfter(groups []Group, moves []LeaderMove) map[string]int {
	leader := map[string]string{}
	out := map[string]int{}
	for _, g := range groups {
		leader[g.String()] = g.Leader
		for _, h := range g.Holders() {
			out[h] += 0
		}
	}
	for _, m := range moves {
		leader[m.Group.String()] = m.To
	}
	for _, s := range leader {
		out[s]++
	}
	return out
}

func even(leads map[string]int) bool {
	var l []Load
	for _, n := range leads {
		l = append(l, Load{Leaders: n})
	}
	return spread(l, func(l Load) int { return l.Leaders }) <= 1
}

func TestPlanLeaders(t *testing.T) {
	three := []string{"n0", "n1", "n2"}
	var sixteen []Group
	for i := range 16 {
		sixteen = append(sixteen, group(fmt.Sprintf("req_%d", i), "n0", three...))
	}
	behind := func(stream, leader, lagging, ahead string) Group {
		return Group{Account: "a", Stream: stream, Leader: leader, Members: []Member{{Name: lagging, Lag: 1 << 20}, {Name: ahead, Current: true}}}
	}
	for _, tc := range []struct {
		name    string
		groups  []Group
		movable func(Group) bool
		moves   int
		even    bool
		to      []string
	}{
		{name: "sixteen on one of three is ten moves from 6/5/5", groups: sixteen, moves: 10, even: true},
		{name: "an even pool is left alone", groups: []Group{
			group("a", "n0", three...), group("b", "n1", three...), group("c", "n2", three...), group("d", "n0", three...),
		}, even: true},
		{name: "a leader moves only onto its own members", groups: []Group{
			group("a", "n0", "n0", "n1", "n2"), group("b", "n0", "n0", "n1", "n2"),
			group("c", "n0", "n0", "n3", "n4"), group("d", "n0", "n0", "n3", "n4"),
			group("e", "n0", "n0", "n1", "n4"),
		}, moves: 4, even: true},
		{name: "one server has nothing to even out", groups: []Group{group("a", "n0", "n0"), group("b", "n0", "n0")}, even: true},
		{name: "a peer far behind is passed over", groups: []Group{behind("a", "n0", "n1", "n2"), behind("b", "n0", "n1", "n2")},
			moves: 1, even: true, to: []string{"n2"}},
		{name: "no peer can take it", groups: []Group{
			{Account: "a", Stream: "a", Leader: "n0", Members: []Member{{Name: "n1", Lag: 1 << 20}}},
			{Account: "a", Stream: "b", Leader: "n0", Members: []Member{{Name: "n1", Lag: 1 << 20}}},
		}},
		{name: "an immovable group is counted and not moved", groups: []Group{
			group("a", "n0", three...), group("b", "n0", three...), group("c", "n0", three...),
		}, movable: func(g Group) bool { return g.Stream != "a" }, moves: 2, even: true},
		{name: "a leaderless group is neither counted nor moved", groups: []Group{
			group("a", "n0", three...), {Account: "a", Stream: "b", Members: []Member{{Name: "n1", Current: true}}},
		}, even: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			moves := PlanLeaders(tc.groups, tc.movable)
			require.Len(t, moves, tc.moves)
			require.Equal(t, tc.even, even(leadsAfter(tc.groups, moves)), "%v", leadsAfter(tc.groups, moves))
			require.Equal(t, moves, PlanLeaders(tc.groups, tc.movable), "the plan is the same every time")
			for i, m := range moves {
				require.Contains(t, m.Group.Holders()[1:], m.To, "%s moves onto a member", m.Group)
				if tc.movable != nil {
					require.True(t, tc.movable(m.Group))
				}
				if tc.to != nil {
					require.Equal(t, tc.to[i], m.To)
				}
			}
		})
	}
}

func TestUnsettled(t *testing.T) {
	three := []string{"n0", "n1", "n2"}
	consumer := Group{Account: "a", Stream: "a", Consumer: "tick", Leader: "n1", Members: []Member{{Name: "n0", Current: true}}}
	behind := group("c", "n0", three...)
	behind.Members[1].Current, behind.Members[1].Lag = false, 1<<20
	down := group("d", "n0", three...)
	down.Members[0] = Member{Name: "n1", Offline: true}
	for _, tc := range []struct {
		name   string
		groups []Group
		want   string
	}{
		{"settled", []Group{group("a", "n0", three...), consumer}, ""},
		{"leaderless", []Group{{Account: "a", Stream: "b", Members: []Member{{Name: "n1"}}}}, "a/b has no leader"},
		{"a member behind is still settled", []Group{behind}, ""},
		{"a member offline", []Group{down}, "n1 is offline for a/d"},
		{"an offline stream ahead of its missing leader", []Group{{Account: "a", Stream: "lone", Offline: true}}, "a/lone is offline"},
		{"a consumer names its stream", []Group{{Account: "a", Stream: "s", Consumer: "c"}}, "a/s > c has no leader"},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, Unsettled(tc.groups)) })
	}
}

func TestMember_Takes(t *testing.T) {
	for _, tc := range []struct {
		m    Member
		want bool
	}{
		{Member{Current: true}, true},
		{Member{Lag: SettledLag}, true},
		{Member{Lag: SettledLag + 1}, false},
		{Member{Current: true, Offline: true}, false},
	} {
		require.Equal(t, tc.want, tc.m.Takes(), "%+v", tc.m)
	}
}
