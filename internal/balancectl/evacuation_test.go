package balancectl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
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

func TestPinnedIn(t *testing.T) {
	marked := func(stream, uid string) sysobs.Group {
		return sysobs.Group{Kind: sysobs.KindStream, Account: "A", Stream: stream, Metadata: map[string]string{lifecycle.OwnerKey: uid}}
	}
	obj := func(kind, name string) js.PinnedObject {
		return js.PinnedObject{Kind: kind, Namespace: "ns", Name: name}
	}
	owners := map[types.UID]owner{
		"uid-kv":     {object: obj("NatsKeyValue", "b"), cluster: "C1"},
		"uid-stream": {object: obj("NatsStream", "a"), cluster: "C1"},
		"uid-away":   {object: obj("NatsStream", "away"), cluster: "C2"},
		"uid-free":   {object: obj("NatsStream", "free")},
		"uid-absent": {object: obj("NatsStream", "absent"), cluster: "C1"},
	}
	snap := &sysobs.Snapshot{Groups: []sysobs.Group{
		{Kind: sysobs.KindMeta},
		marked("KV_b", "uid-kv"),
		{Kind: sysobs.KindConsumer, Account: "A", Stream: "KV_b", Consumer: "D", Metadata: map[string]string{lifecycle.OwnerKey: "uid-kv"}},
		marked("A", "uid-stream"),
		marked("AWAY", "uid-away"),
		marked("FREE", "uid-free"),
		{Kind: sysobs.KindStream, Account: "A", Stream: "BARE"},
	}}
	require.Equal(t, []js.PinnedObject{obj("NatsStream", "a"), obj("NatsKeyValue", "b")}, pinnedIn(snap, owners, "C1"),
		"absent's stream is not in the source, whatever its spec declares")
}

func TestRequestedRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 26, 11, 31, 2, 0, time.UTC)
	requested := map[balance.StreamID]time.Time{
		{Account: "B", Stream: "S"}: at,
		{Account: "A", Stream: "T"}: at.Add(time.Second),
		{Account: "A", Stream: "S"}: at.Add(2 * time.Second),
	}
	list := requestedList(requested)
	require.Equal(t, []js.RequestedMove{
		{Account: "A", Stream: "S", Time: metav1.NewTime(at.Add(2 * time.Second))},
		{Account: "A", Stream: "T", Time: metav1.NewTime(at.Add(time.Second))},
		{Account: "B", Stream: "S", Time: metav1.NewTime(at)},
	}, list)
	require.Equal(t, requested, requestedOf(list))
	require.Empty(t, requestedList(nil))
}

func TestEvacuees(t *testing.T) {
	marked := func(stream, uid string) balance.Group {
		return balance.Group{Account: "A", Stream: stream, Metadata: map[string]string{lifecycle.OwnerKey: uid}}
	}
	obs := balance.Observation{Groups: []balance.Group{
		marked("PINNED", "uid-pinned"),
		marked("OWNED", "uid-free"),
		marked("ORPHAN", "uid-gone"),
		{Account: "A", Stream: "BARE"},
		{Account: "A", Stream: "PINNED", Consumer: "D", Metadata: map[string]string{lifecycle.OwnerKey: "uid-pinned"}},
	}}
	owners := map[types.UID]owner{"uid-pinned": {cluster: "C1"}, "uid-free": {}}
	id := func(name string) balance.StreamID { return balance.StreamID{Account: "A", Stream: name} }
	moves := "NatsClusterEvacuation nats-system/retire moves it"

	tests := []struct {
		name string
		evac string
		want map[string]string
	}{
		{"NoEvacuation", "", map[string]string{"PINNED": "", "OWNED": "", "ORPHAN": "", "BARE": ""}},
		{"Evacuation", "nats-system/retire", map[string]string{"PINNED": "", "OWNED": moves, "ORPHAN": moves, "BARE": moves}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &evacuees{Observer: fixedObserver{obs}, evac: tt.evac, owners: owners}
			got, err := e.Observe(t.Context())
			require.NoError(t, err)
			require.Equal(t, obs, got)
			for stream, want := range tt.want {
				require.Equal(t, want, e.Yield(id(stream)), stream)
			}
		})
	}
}

type fixedObserver struct{ obs balance.Observation }

func (f fixedObserver) Observe(context.Context) (balance.Observation, error) { return f.obs, nil }

func TestServersDown(t *testing.T) {
	meta := sysobs.Group{Kind: sysobs.KindMeta}
	tests := []struct {
		name string
		snap sysobs.Snapshot
		want string
	}{
		{"one silent", sysobs.Snapshot{Silent: []string{"C1-2"}}, "server C1-2 of C1 does not answer"},
		{"two silent", sysobs.Snapshot{Silent: []string{"C1-1", "C1-2"}}, "servers C1-1, C1-2 of C1 do not answer"},
		{"no meta group", sysobs.Snapshot{}, "the meta group has no leader"},
		{"no meta leader", sysobs.Snapshot{Groups: []sysobs.Group{meta}}, "the meta group has no leader"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := serversDown(t.Context(), nil, &tt.snap, "C1")
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
