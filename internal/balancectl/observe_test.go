package balancectl

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/balance"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

func TestObservation(t *testing.T) {
	servers := []sysobs.Server{{Name: "s1", Tags: []string{"ssd"}}, {Name: "s2"}}
	meta := sysobs.Group{Kind: sysobs.KindMeta, Leader: "s1", Members: []sysobs.Member{{Server: "s1", Current: true}, {Server: "s2", Current: true}}}
	stream := func(members ...sysobs.Member) sysobs.Group {
		return sysobs.Group{
			Kind: sysobs.KindStream, Account: "A", Stream: "S", Leader: "s1",
			Members:   append([]sysobs.Member{{Server: "s1", Current: true}}, members...),
			Placement: &sysobs.Placement{Cluster: "C1", Tags: []string{"ssd"}},
		}
	}
	tests := []struct {
		name          string
		snap          sysobs.Snapshot
		wantUnsettled string
	}{
		{"settled", sysobs.Snapshot{Servers: servers, Groups: []sysobs.Group{meta, stream(sysobs.Member{Server: "s2", Current: true})}}, ""},
		{"behind by one", sysobs.Snapshot{Servers: servers, Groups: []sysobs.Group{meta, stream(sysobs.Member{Server: "s2", Lag: 1})}}, "s2 is 1 behind for A/S"},
		{"not current with no lag", sysobs.Snapshot{Servers: servers, Groups: []sysobs.Group{meta, stream(sysobs.Member{Server: "s2"})}}, "s2 is not current for A/S"},
		{"offline", sysobs.Snapshot{Servers: servers, Groups: []sysobs.Group{meta, stream(sysobs.Member{Server: "s2", Offline: true})}}, "s2 is offline for A/S"},
		{"silent", sysobs.Snapshot{Servers: servers, Silent: []string{"s2"}, Groups: []sysobs.Group{meta, stream(sysobs.Member{Server: "s2", Current: true})}}, "s2 did not answer"},
		{"meta leaderless", sysobs.Snapshot{Servers: servers, Groups: []sysobs.Group{{Kind: sysobs.KindMeta}, stream(sysobs.Member{Server: "s2", Current: true})}}, "the meta group has no leader"},
		{"no meta group", sysobs.Snapshot{Servers: servers, Groups: []sysobs.Group{stream(sysobs.Member{Server: "s2", Current: true})}}, "the meta group has no leader"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := observation("C1", &tt.snap)
			require.Equal(t, tt.wantUnsettled, got.Unsettled)
			require.Equal(t, "C1", got.Cluster)
			require.Equal(t, []balance.Server{{Name: "s1", Tags: []string{"ssd"}}, {Name: "s2"}}, got.Servers)
			require.Len(t, got.Groups, 1, "the meta group is not the balancer's")
			g := got.Groups[0]
			require.Equal(t, "s1", g.Leader)
			require.Len(t, g.Members, 1, "the leader is not its own member")
			require.Equal(t, "s2", g.Members[0].Name)
			require.Equal(t, &balance.Placement{Cluster: "C1", Tags: []string{"ssd"}}, g.Placement)
		})
	}
}

func TestPinned(t *testing.T) {
	obs := balance.Observation{Cluster: "C1", Groups: []balance.Group{
		{Account: "A", Stream: "HOME", Placement: &balance.Placement{Cluster: "C1"}},
		{Account: "A", Stream: "AWAY", Placement: &balance.Placement{Cluster: "C2"}},
		{Account: "A", Stream: "AWAY", Consumer: "D", Placement: &balance.Placement{Cluster: "C2"}},
		{Account: "A", Stream: "TAGGED", Placement: &balance.Placement{Tags: []string{"ssd"}}},
		{Account: "B", Stream: "FREE"},
	}}
	require.Equal(t, map[balance.StreamID]string{
		{Account: "A", Stream: "AWAY"}: "pinned to NATS cluster C2",
	}, pinned(obs))
}
