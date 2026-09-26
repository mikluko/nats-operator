package sysobs

import (
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

func streamOn(leader string, replicas ...wirePeer) wireAccount {
	return wireAccount{ID: "A", Streams: []wireStream{{
		Name:    "S",
		Cluster: &wireCluster{RaftGroup: "S-rg", Leader: leader, Replicas: replicas},
	}}}
}

func info(meta *wireMeta, accounts ...wireAccount) *wireJSInfo {
	return &wireJSInfo{Meta: meta, Accounts: accounts}
}

var ok = func(n string) wirePeer { return wirePeer{Name: n, Current: true} }

func current(n string) Member { return Member{Server: n, Current: true} }

func TestMerge_PlacementAndLag(t *testing.T) {
	acct := streamOn("s1", wirePeer{Name: "s2", Lag: 7})
	acct.Streams[0].Config = &wireStreamConfig{Placement: &wirePlacement{Cluster: "C1", Tags: []string{"ssd"}}, Metadata: map[string]string{"k": "v"}}
	acct.Streams[0].Consumers = []wireConsumer{{Name: "C", Cluster: &wireCluster{RaftGroup: "C-rg", Leader: "s1", Replicas: []wirePeer{ok("s2")}}}}
	snap := merge(roster("s1", "s2"), map[string]*wireJSInfo{"s1": info(nil, acct)})

	want := &Placement{Cluster: "C1", Tags: []string{"ssd"}}
	require.Len(t, snap.Groups, 2)
	for _, g := range snap.Groups {
		require.Equal(t, want, g.Placement, "%s %s", g.Kind, g.Consumer)
		require.Equal(t, map[string]string{"k": "v"}, g.Metadata, "%s %s", g.Kind, g.Consumer)
	}
	require.Equal(t, []Member{current("s1"), {Server: "s2", Lag: 7}}, snap.Groups[0].Members)
}

func TestMerge_Verdict(t *testing.T) {
	meta := func(leader string) *wireMeta {
		m := &wireMeta{Leader: leader}
		for _, n := range []string{"s1", "s2", "s3"} {
			if n != leader {
				m.Replicas = append(m.Replicas, ok(n))
			}
		}
		return m
	}
	tests := []struct {
		name    string
		roster  []Server
		reports map[string]*wireJSInfo
		want    Verdict
	}{
		{
			name:   "every group led and current",
			roster: roster("s1", "s2", "s3"),
			reports: map[string]*wireJSInfo{
				"s1": info(meta("s1"), streamOn("s1", ok("s2"), ok("s3"))),
				"s2": info(meta("s1"), streamOn("s1")),
				"s3": info(meta("s1"), streamOn("s1")),
			},
			want: Verdict{},
		},
		{
			name:   "member behind",
			roster: roster("s1", "s2", "s3"),
			reports: map[string]*wireJSInfo{
				"s1": info(meta("s1"), streamOn("s1", ok("s2"), wirePeer{Name: "s3"})),
				"s2": info(meta("s1")),
				"s3": info(meta("s1")),
			},
			want: Verdict{Unsettled: []Unsettled{{Reason: ReasonMemberBehind, Servers: []string{"s3"}}}},
		},
		{
			name:   "offline outranks behind",
			roster: roster("s1", "s2", "s3"),
			reports: map[string]*wireJSInfo{
				"s1": info(meta("s1"), streamOn("s1", wirePeer{Name: "s2", Offline: true}, wirePeer{Name: "s3"})),
				"s2": info(meta("s1")),
				"s3": info(meta("s1")),
			},
			want: Verdict{Unsettled: []Unsettled{{Reason: ReasonMemberOffline, Servers: []string{"s2"}}}},
		},
		{
			name:   "no server leads the stream",
			roster: roster("s1", "s2", "s3"),
			reports: map[string]*wireJSInfo{
				"s1": info(meta("s1")),
				"s2": info(meta("s1"), streamOn("")),
				"s3": info(meta("s1"), streamOn("")),
			},
			want: Verdict{Unsettled: []Unsettled{{Reason: ReasonNoLeader}}},
		},
		{
			name:   "named leader did not answer",
			roster: roster("s1", "s2", "s3"),
			reports: map[string]*wireJSInfo{
				"s2": info(meta("s2"), streamOn("s1")),
				"s3": info(meta("s2"), streamOn("s1")),
			},
			want: Verdict{Unsettled: []Unsettled{{Reason: ReasonNoLeader}}, Silent: []string{"s1"}},
		},
		{
			name:   "meta leader in another cluster, agreed",
			roster: roster("s1", "s2"),
			reports: map[string]*wireJSInfo{
				"s1": info(&wireMeta{Leader: "x1"}),
				"s2": info(&wireMeta{Leader: "x1"}),
			},
			want: Verdict{},
		},
		{
			name:   "meta leader in another cluster, disputed",
			roster: roster("s1", "s2"),
			reports: map[string]*wireJSInfo{
				"s1": info(&wireMeta{Leader: "x1"}),
				"s2": info(&wireMeta{Leader: "x2"}),
			},
			want: Verdict{Unsettled: []Unsettled{{Reason: ReasonNoLeader}}},
		},
		{
			name:   "meta replicas outside the roster are not judged",
			roster: roster("s1"),
			reports: map[string]*wireJSInfo{
				"s1": info(&wireMeta{Leader: "s1", Replicas: []wirePeer{{Name: "x1", Offline: true}}}),
			},
			want: Verdict{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := merge(tt.roster, tt.reports).Verdict()
			for i := range got.Unsettled {
				got.Unsettled[i].Group = Group{}
			}
			require.Equal(t, tt.want, got)
			require.Equal(t, len(tt.want.Unsettled) == 0 && len(tt.want.Silent) == 0, got.Settled())
		})
	}
}

func TestMerge_GroupsAndLoad(t *testing.T) {
	cons := wireConsumer{Name: "C", Cluster: &wireCluster{RaftGroup: "C-rg", Leader: "s2", Replicas: []wirePeer{ok("s1")}}}
	acct := func(leader string, consumers ...wireConsumer) wireAccount {
		a := streamOn(leader, ok("s2"))
		a.Streams[0].Consumers = consumers
		return a
	}
	snap := merge(roster("s1", "s2", "s3"), map[string]*wireJSInfo{
		"s1": info(&wireMeta{Leader: "s1", Replicas: []wirePeer{ok("s2"), ok("s3")}}, acct("s1", cons)),
		"s2": info(&wireMeta{Leader: "s1"}, acct("s1", cons)),
		"s3": info(&wireMeta{Leader: "s1"}),
	})

	require.Equal(t, []Group{
		{Kind: KindMeta, Leader: "s1", Members: []Member{current("s1"), current("s2"), current("s3")}},
		{Kind: KindStream, Account: "A", Stream: "S", RaftGroup: "S-rg", Leader: "s1", Members: []Member{current("s1"), current("s2")}},
		{Kind: KindConsumer, Account: "A", Stream: "S", Consumer: "C", RaftGroup: "C-rg", Leader: "s2", Members: []Member{current("s1"), current("s2")}},
	}, snap.Groups)

	require.Equal(t, map[string]Load{
		"s1": {StreamLeaders: 1, StreamReplicas: 1, ConsumerReplicas: 1},
		"s2": {StreamReplicas: 1, ConsumerLeaders: 1, ConsumerReplicas: 1},
		"s3": {},
	}, snap.Load())
}

func TestMerge_LeaderlessGroupListsHolders(t *testing.T) {
	snap := merge(roster("s1", "s2"), map[string]*wireJSInfo{
		"s1": info(nil, streamOn("s9")),
		"s2": info(nil, streamOn("s9")),
	})
	require.Equal(t, []Group{{
		Kind: KindStream, Account: "A", Stream: "S", RaftGroup: "S-rg", NamedLeader: "s9",
		Members: []Member{{Server: "s1"}, {Server: "s2"}},
	}}, snap.Groups)
	require.Equal(t, map[string]Load{"s1": {}, "s2": {}}, snap.Load())
}
