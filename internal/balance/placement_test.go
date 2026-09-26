package balance

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func roster(names ...string) []Server {
	var out []Server
	for _, n := range names {
		out = append(out, Server{Name: n})
	}
	return out
}

func placed(g Group, cluster string, tags ...string) Group {
	g.Placement = &Placement{Cluster: cluster, Tags: tags}
	return g
}

func TestTarget(t *testing.T) {
	for _, tc := range []struct{ copies, servers, floor, ceiling, atCeiling int }{
		{12, 5, 2, 3, 2},
		{9, 3, 3, 3, 0},
		{2, 5, 0, 1, 2},
		{4, 0, 0, 0, 0},
	} {
		floor, ceiling, atCeiling := Target(tc.copies, tc.servers)
		require.Equal(t, []int{tc.floor, tc.ceiling, tc.atCeiling}, []int{floor, ceiling, atCeiling}, "%+v", tc)
	}
}

func TestLandings(t *testing.T) {
	servers := []Server{{Name: "n0", Tags: []string{"ssd"}}, {Name: "n1", Tags: []string{"SSD", "eu"}}, {Name: "n2"}, {Name: "n3", Tags: []string{"ssd"}}}
	g := group("s", "n0", "n0")
	for _, tc := range []struct {
		name string
		g    Group
		want []string
	}{
		{"no placement lands on every non-member", g, []string{"n1", "n2", "n3"}},
		{"its own cluster declared", placed(g, "c1"), []string{"n1", "n2", "n3"}},
		{"tags narrow the landings, case-insensitively", placed(g, "", "ssd"), []string{"n1", "n3"}},
		{"every tag must match", placed(g, "c1", "ssd", "eu"), []string{"n1"}},
		{"no server carries the tags", placed(g, "c1", "nvme"), nil},
		{"another cluster declared is never overridden", placed(g, "c2"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, Landings(tc.g, servers, "c1")) })
	}
}

func TestPlanPlacement(t *testing.T) {
	five := roster("n0", "n1", "n2", "n3", "n4")
	var crowded []Group
	for i := range 4 {
		crowded = append(crowded, group(fmt.Sprintf("req_%d", i), "n0", "n0", "n1", "n2"))
	}
	singles := []Group{
		group("a", "n0", "n0"), group("b", "n0", "n0"), group("c", "n0", "n0"),
		group("d", "n1", "n1"), group("e", "n1", "n1"),
		group("f", "n2", "n2"), group("g", "n2", "n2"),
		group("h", "n3", "n3"), group("i", "n3", "n3"),
	}
	pinned := func(cluster string, tags ...string) []Group {
		var out []Group
		for _, g := range crowded {
			out = append(out, placed(g, cluster, tags...))
		}
		return out
	}
	for _, tc := range []struct {
		name    string
		streams []Group
		roster  []Server
		skip    func(Group) bool
		tried   func(Group) bool
		want    *PlacementMove
		pending int
	}{
		{
			name: "twelve copies on three of five servers", streams: crowded, roster: five, pending: 3,
			want: &PlacementMove{Group: crowded[0], From: "n0", Cluster: "c1"},
		},
		{
			name: "a skipped stream is counted and not moved", streams: crowded, roster: five, pending: 3,
			skip: func(g Group) bool { return g.Stream == "req_0" },
			want: &PlacementMove{Group: crowded[1], From: "n0", Cluster: "c1"},
		},
		{
			name: "even", roster: five,
			streams: []Group{group("a", "n0", "n0", "n1", "n2"), group("b", "n3", "n3", "n4", "n0")},
		},
		{
			name: "three copies on three servers is every server there is", roster: roster("n0", "n1", "n2"),
			streams: []Group{group("a", "n0", "n0", "n1", "n2"), group("b", "n0", "n0", "n1", "n2")},
		},
		{
			name: "3/2/2/2/0 moves only if the server picks n4", streams: singles, roster: five, pending: 1,
			want: &PlacementMove{Group: singles[0], From: "n0", Cluster: "c1", Unsure: true},
		},
		{
			name: "a stream tried on an unsure move is not tried again", streams: singles, roster: five, pending: 1,
			tried: func(g Group) bool { return g.Stream == "a" },
			want:  &PlacementMove{Group: singles[1], From: "n0", Cluster: "c1", Unsure: true},
		},
		{
			name: "every stream tried rests one copy short of even", streams: singles, roster: five, pending: 1,
			tried: func(Group) bool { return true },
		},
		{
			name: "declared in another NATS cluster: never moved", streams: pinned("c2"), roster: five, pending: 3,
		},
		{
			name: "no eligible non-member in its own NATS cluster: not requested", streams: pinned("c1", "ssd"), pending: 3,
			roster: []Server{{Name: "n0", Tags: []string{"ssd"}}, {Name: "n1", Tags: []string{"ssd"}}, {Name: "n2", Tags: []string{"ssd"}}, {Name: "n3"}, {Name: "n4"}},
		},
		{
			name: "tags confine the landings the plan weighs", streams: pinned("c1", "ssd"), pending: 3,
			roster: []Server{{Name: "n0", Tags: []string{"ssd"}}, {Name: "n1", Tags: []string{"ssd"}}, {Name: "n2", Tags: []string{"ssd"}}, {Name: "n3", Tags: []string{"ssd"}}, {Name: "n4"}},
			want:   &PlacementMove{Group: pinned("c1", "ssd")[0], From: "n0", Cluster: "c1"},
		},
		{
			name: "a server off the roster is neither counted nor vacated", roster: roster("n0", "n1"), pending: 1,
			streams: []Group{group("a", "x9", "x9", "n0"), group("b", "n0", "n0")},
			want:    &PlacementMove{Group: group("a", "x9", "x9", "n0"), From: "n0", Cluster: "c1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, pending := PlanPlacement(tc.streams, tc.roster, "c1", tc.skip, tc.tried)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.pending, pending)
		})
	}
}
