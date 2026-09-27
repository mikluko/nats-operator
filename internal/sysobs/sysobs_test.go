package sysobs

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func groupNames(s *Snapshot) []string {
	var out []string
	for _, g := range s.Groups {
		out = append(out, string(g.Kind)+":"+g.Account+"/"+g.Stream+"/"+g.Consumer)
	}
	return out
}

func memberNames(g Group) []string {
	var out []string
	for _, m := range g.Members {
		out = append(out, m.Server)
	}
	return out
}

func findGroup(t *testing.T, s *Snapshot, kind Kind, stream, consumer string) Group {
	t.Helper()
	i := slices.IndexFunc(s.Groups, func(g Group) bool {
		return g.Kind == kind && g.Stream == stream && g.Consumer == consumer
	})
	require.GreaterOrEqual(t, i, 0, "no %s group %s/%s in %v", kind, stream, consumer, groupNames(s))
	return s.Groups[i]
}

// settledSnapshot polls until the cluster is Settled with want groups.
func settledSnapshot(t *testing.T, o *SystemClient, want int) *Snapshot {
	t.Helper()
	var snap *Snapshot
	require.Eventually(t, func() bool {
		s, err := o.Observe(context.Background())
		if err != nil || len(s.Groups) != want || !s.Verdict().Settled() {
			return false
		}
		snap = s
		return true
	}, 30*time.Second, 200*time.Millisecond)
	return snap
}

func TestObserve_SuperclusterFiltersToOneCluster(t *testing.T) {
	c1 := newTestCluster(t, "C1", 3)
	c2 := newTestCluster(t, "C2", 1)
	srvs := startSupercluster(t, c1, c2)

	js, err := connect(t, c1.clientPort[0], "a").JetStream()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := js.AddStream(&nats.StreamConfig{Name: "R3", Subjects: []string{"r3"}, Replicas: 3, Placement: &nats.Placement{Cluster: "C1"}})
		return err == nil
	}, 30*time.Second, 200*time.Millisecond)
	_, err = js.AddConsumer("R3", &nats.ConsumerConfig{Durable: "D", AckPolicy: nats.AckExplicitPolicy})
	require.NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "R1", Subjects: []string{"r1"}, Replicas: 1, Placement: &nats.Placement{Cluster: "C1"}})
	require.NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "ELSEWHERE", Subjects: []string{"e"}, Replicas: 1, Placement: &nats.Placement{Cluster: "C2"}})
	require.NoError(t, err)

	o := New(connect(t, c2.clientPort[0], "sys"), "C1")

	roster, err := o.Roster(context.Background())
	require.NoError(t, err)
	var names []string
	for _, s := range roster {
		names = append(names, s.Name)
		require.Equal(t, srvs[s.Name].ID(), s.ID)
		require.True(t, s.JetStream)
	}
	require.Equal(t, []string{"C1-0", "C1-1", "C1-2"}, names)

	snap := settledSnapshot(t, o, 4)
	require.Empty(t, snap.Silent)
	require.Equal(t, []string{
		"meta://",
		"stream:A/R1/",
		"stream:A/R3/",
		"consumer:A/R3/D",
	}, groupNames(snap))

	r3 := findGroup(t, snap, KindStream, "R3", "")
	require.NotEmpty(t, r3.RaftGroup)
	require.Equal(t, []string{"C1-0", "C1-1", "C1-2"}, memberNames(r3))
	require.Contains(t, memberNames(r3), r3.Leader)
	require.Equal(t, &Placement{Cluster: "C1"}, r3.Placement)
	require.Equal(t, &Placement{Cluster: "C1"}, findGroup(t, snap, KindConsumer, "R3", "D").Placement)
	require.Len(t, findGroup(t, snap, KindConsumer, "R3", "D").Members, 3)
	require.Len(t, findGroup(t, snap, KindStream, "R1", "").Members, 1)

	load := snap.Load()
	require.Len(t, load, 3)
	var total Load
	for _, l := range load {
		total.StreamLeaders += l.StreamLeaders
		total.StreamReplicas += l.StreamReplicas
		total.ConsumerLeaders += l.ConsumerLeaders
		total.ConsumerReplicas += l.ConsumerReplicas
	}
	require.Equal(t, Load{StreamLeaders: 2, StreamReplicas: 4, ConsumerLeaders: 1, ConsumerReplicas: 3}, total)
}

func TestObserve_ServerDownIsNotSettled(t *testing.T) {
	c1 := newTestCluster(t, "C1", 3)
	srvs := startSupercluster(t, c1)

	js, err := connect(t, c1.clientPort[0], "a").JetStream()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := js.AddStream(&nats.StreamConfig{Name: "R3", Subjects: []string{"r3"}, Replicas: 3})
		return err == nil
	}, 30*time.Second, 200*time.Millisecond)

	o := New(connect(t, c1.clientPort[0], "sys"), "C1")
	snap := settledSnapshot(t, o, 2)
	r3 := findGroup(t, snap, KindStream, "R3", "")
	down := "C1-1"
	if r3.Leader == down {
		down = "C1-2"
	}
	srvs[down].Shutdown()

	require.Eventually(t, func() bool {
		s, err := o.Observe(context.Background())
		if err != nil {
			return false
		}
		v := s.Verdict()
		if v.Settled() {
			return false
		}
		i := slices.IndexFunc(v.Unsettled, func(u Unsettled) bool {
			return u.Group.Kind == KindStream && u.Group.Stream == "R3"
		})
		return i >= 0 && v.Unsettled[i].Reason == ReasonMemberOffline &&
			slices.Equal(v.Unsettled[i].Servers, []string{down}) && len(s.Servers) == 2
	}, 30*time.Second, 200*time.Millisecond)
}

func TestReload(t *testing.T) {
	c1 := newTestCluster(t, "C1", 3)
	srvs := startSupercluster(t, c1)
	target := srvs["C1-0"]
	o := New(connect(t, c1.clientPort[1], "sys"), "C1")

	var before ConfigState
	require.Eventually(t, func() bool {
		var err error
		before, err = o.Config(context.Background(), target.ID())
		return err == nil
	}, 10*time.Second, 100*time.Millisecond, "the target's system subscriptions never reached the observer")
	require.NotEmpty(t, before.Digest)

	raw, err := os.ReadFile(target.conf)
	require.NoError(t, err)
	base := string(raw)
	tests := []struct {
		name       string
		extra      string
		wantErr    error
		wantChange bool
	}{
		{name: "reloadable field", extra: "max_payload: 2MB\n", wantChange: true},
		{name: "restart-only field", extra: "max_payload: 2MB\nlame_duck_duration: \"45s\"\n", wantErr: ErrServer},
	}
	prev := before
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(target.conf, []byte(base+tt.extra), 0o600))
			got, err := o.Reload(context.Background(), target.ID())
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				now, err := o.Config(context.Background(), target.ID())
				require.NoError(t, err)
				require.Equal(t, prev, now)
				return
			}
			require.NoError(t, err)
			require.True(t, got.LoadTime.After(prev.LoadTime))
			require.Equal(t, tt.wantChange, got.Digest != prev.Digest)
			prev = got
		})
	}
}

// TestObserve_Gateways pins what each server reports of its gateway
// connections: an outbound connection to every other NATS cluster, and one
// inbound connection per server of the other cluster, spread over the
// observed cluster's servers.
func TestObserve_Gateways(t *testing.T) {
	c1 := newTestCluster(t, "C1", 3)
	c2 := newTestCluster(t, "C2", 2)
	startSupercluster(t, c1, c2)

	o := New(connect(t, c1.clientPort[0], "sys"), "C1", WithGateways())
	require.Eventually(t, func() bool {
		s, err := o.Observe(context.Background())
		if err != nil || len(s.Servers) != 3 {
			return false
		}
		inbound := 0
		for _, srv := range s.Servers {
			if srv.Gateways == nil || !slices.Equal(srv.Gateways.Outbound, []string{"C2"}) {
				return false
			}
			inbound += srv.Gateways.Inbound["C2"]
		}
		return inbound == 2
	}, 30*time.Second, 200*time.Millisecond)
}
