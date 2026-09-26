package balancectl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	"github.com/mikluko/nats-operator/internal/balance"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

func TestPlanEvacuation(t *testing.T) {
	now := time.Now()
	stream := func(name, leader string, members ...string) sysobs.Group {
		g := sysobs.Group{Kind: sysobs.KindStream, Account: "A", Stream: name, Leader: leader}
		for _, m := range append([]string{leader}, members...) {
			if m != "" {
				g.Members = append(g.Members, sysobs.Member{Server: m, Current: true})
			}
		}
		return g
	}
	ownedBy := func(g sysobs.Group, uid string) sysobs.Group {
		g.Metadata = map[string]string{lifecycle.OwnerKey: uid}
		return g
	}
	leaderless := stream("NAMED", "", "C1-0")
	leaderless.NamedLeader = "C2-1"
	snap := &sysobs.Snapshot{
		Servers: []sysobs.Server{{Name: "C1-0"}, {Name: "C1-1"}, {Name: "C1-2"}},
		Groups: []sysobs.Group{
			{Kind: sysobs.KindMeta, Leader: "C1-0"},
			stream("FRESH", "C1-0", "C1-1"),
			{Kind: sysobs.KindConsumer, Account: "A", Stream: "FRESH", Consumer: "D", Leader: "C1-0"},
			stream("GROWING", "C1-0", "C2-0"),
			leaderless,
			ownedBy(stream("PINNED", "C1-1"), "uid-pinned"),
			ownedBy(stream("ELSEWHERE", "C1-1"), "uid-elsewhere"),
			ownedBy(stream("UNPINNED", "C1-2"), "uid-free"),
			ownedBy(stream("ORPHAN", "C1-2"), "uid-gone"),
			stream("ASKED", "C1-2"),
			stream("STALLED", "C1-2"),
		},
	}
	owners := map[types.UID]owner{"uid-pinned": {cluster: "C1"}, "uid-elsewhere": {cluster: "C2"}, "uid-free": {}}
	id := func(name string) balance.StreamID { return balance.StreamID{Account: "A", Stream: name} }
	requested := map[balance.StreamID]time.Time{
		id("ASKED"):   now.Add(-time.Second),
		id("STALLED"): now.Add(-2 * requestGrace),
		id("LANDED"):  now.Add(-time.Second),
	}

	tests := []struct {
		name    string
		budget  int
		move    []string
		waiting int
	}{
		{"BudgetLeft", 5, []string{"FRESH", "UNPINNED"}, 2},
		{"BudgetSpent", 3, nil, 4},
		{"BudgetWide", 10, []string{"FRESH", "UNPINNED", "ORPHAN", "STALLED"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := planEvacuation(snap, owners, requested, now, tt.budget)
			require.Equal(t, []balance.StreamID{id("LANDED")}, p.completed)
			require.ElementsMatch(t, []balance.StreamID{id("GROWING"), id("NAMED"), id("ASKED")}, p.inFlight)
			require.Equal(t, 2, p.owned, "PINNED and ELSEWHERE are their owners' to move")
			var move []string
			for _, g := range p.move {
				move = append(move, g.Stream)
			}
			require.Equal(t, tt.move, move)
			require.Equal(t, tt.waiting, p.waiting)
		})
	}
}

func TestStale(t *testing.T) {
	owners := map[types.UID]owner{"uid": {}}
	tests := []struct {
		name  string
		group sysobs.Group
		want  bool
	}{
		{"NamesSource", sysobs.Group{Placement: &sysobs.Placement{Cluster: "C1"}}, true},
		{"NamesNothing", sysobs.Group{}, false},
		{"NamesTagsOnly", sysobs.Group{Placement: &sysobs.Placement{Tags: []string{"ssd"}}}, false},
		{"Owned", sysobs.Group{Placement: &sysobs.Placement{Cluster: "C1"}, Metadata: map[string]string{lifecycle.OwnerKey: "uid"}}, false},
		{"OwnerGone", sysobs.Group{Placement: &sysobs.Placement{Cluster: "C1"}, Metadata: map[string]string{lifecycle.OwnerKey: "gone"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, stale(tt.group, "C1", owners))
		})
	}
}

func TestCarrier(t *testing.T) {
	servers := []sysobs.Server{{Name: "C1-0", Tags: []string{"old"}}, {Name: "C1-1", Tags: []string{"new", "SSD"}}}
	tests := []struct {
		name string
		tags []string
		want string
	}{
		{"None", []string{"cluster:C2"}, ""},
		{"One", []string{"new"}, "C1-1"},
		{"AllOfThem", []string{"new", "ssd"}, "C1-1"},
		{"NotAll", []string{"old", "ssd"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, carrier(servers, tt.tags))
		})
	}
}
