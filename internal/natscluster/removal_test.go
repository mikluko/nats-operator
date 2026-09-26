package natscluster

import (
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// fakeNats is a NATS cluster as removal decisions see it, moved along by
// the actions they take: an evacuation moves a server's replicas to the
// first server of the plan that lacks one, a removal drops it from the
// meta group, a deletion stops it, and a replaced server comes back outside
// the meta group until readmit.
type fakeNats struct {
	replicas  int
	jetStream bool
	noAdmin   string
	servers   []string
	meta      []string
	leader    string
	streams   map[string][]string
	removing  string
	phase     removalPhase
	replace   []string
	restart   []string
	paused    bool
	prev      *clusterv1beta1.RolloutStatus
	now       time.Time
	log       []string
}

// newFakeNats is n servers in one meta group led by leader, holding R3
// streams A and B on demo-0..2 and C on demo-2..4 where n allows, and an
// R1 stream D on the last server; spec.replicas is 3.
func newFakeNats(n int, leader string) *fakeNats {
	f := &fakeNats{replicas: 3, jetStream: true, leader: leader, streams: map[string][]string{},
		now: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)}
	for i := range n {
		f.servers = append(f.servers, fmt.Sprintf("demo-%d", i))
	}
	f.meta = slices.Clone(f.servers)
	f.streams["A"] = slices.Clone(f.servers[:3])
	f.streams["B"] = slices.Clone(f.servers[:3])
	if n >= 5 {
		f.streams["C"] = slices.Clone(f.servers[2:5])
	}
	f.streams["D"] = []string{f.servers[n-1]}
	return f
}

func (f *fakeNats) snapshot() *sysobs.Snapshot {
	snap := &sysobs.Snapshot{}
	for _, s := range f.servers {
		snap.Servers = append(snap.Servers, sysobs.Server{Name: s, JetStream: true, Metadata: map[string]string{MetadataConfigRevision: "r"}})
	}
	members := func(ss []string) []sysobs.Member {
		var out []sysobs.Member
		for _, s := range ss {
			out = append(out, sysobs.Member{Server: s, Current: true, Offline: !slices.Contains(f.servers, s)})
		}
		return out
	}
	if f.jetStream {
		snap.Groups = append(snap.Groups, sysobs.Group{Kind: sysobs.KindMeta, Leader: f.leader, Members: members(f.meta)})
	}
	for _, name := range []string{"A", "B", "C", "D"} {
		if ms, ok := f.streams[name]; ok {
			snap.Groups = append(snap.Groups, sysobs.Group{Kind: sysobs.KindStream, Account: "$G", Stream: name, Leader: ms[0], Members: members(ms)})
		}
	}
	return snap
}

func (f *fakeNats) plan() []string {
	var out []string
	for i := range f.replicas {
		out = append(out, fmt.Sprintf("demo-%d", i))
	}
	return out
}

func (f *fakeNats) state() rolloutState {
	snap := f.snapshot()
	v := snap.Verdict()
	st := rolloutState{Target: "r", MetaLeader: f.leader, Verdict: &v, Paused: f.paused, Prev: f.prev, Now: f.now}
	for _, s := range f.plan() {
		up := slices.Contains(f.servers, s)
		st.Servers = append(st.Servers, rolloutServer{Name: s, OnTarget: up, Ready: up, Revision: "r",
			Restart: slices.Contains(f.restart, s)})
		if up && !slices.Contains(f.meta, s) {
			st.NotInMeta = append(st.NotInMeta, s)
		}
	}
	st.Removal = removalState{Removing: f.removing, Phase: f.phase, Replace: slices.Clone(f.replace),
		Replicas: f.replicas, JetStream: f.jetStream, NoAdmin: f.noAdmin, Snapshot: snap}
	for _, s := range f.servers {
		if !slices.Contains(f.plan(), s) {
			st.Removal.Surplus = append(st.Removal.Surplus, s)
		}
	}
	return st
}

// decide takes one decision, logs its action and applies it.
func (f *fakeNats) decide() rolloutDecision {
	d := decide(f.state())
	f.prev = d.Status
	if d.Step != "" {
		f.log = append(f.log, "restart "+d.Step)
		f.restart = slices.DeleteFunc(f.restart, func(s string) bool { return s == d.Step })
	}
	if d.Remove == nil {
		return d
	}
	x := d.Remove.Server
	f.log = append(f.log, string(d.Remove.Action)+" "+x)
	switch d.Remove.Action {
	case actionEvacuate:
		for name, ms := range f.streams {
			i := slices.Index(ms, x)
			if i < 0 {
				continue
			}
			ms = slices.Delete(ms, i, i+1)
			for _, s := range f.plan() {
				if s != x && !slices.Contains(ms, s) {
					ms = append(ms, s)
					break
				}
			}
			f.streams[name] = ms
		}
		f.removing, f.phase = x, phaseEvacuating
	case actionStepDown:
		f.leader = f.meta[slices.IndexFunc(f.meta, func(s string) bool { return s != x })]
	case actionRemovePeer:
		f.meta = slices.DeleteFunc(f.meta, func(s string) bool { return s == x })
		f.phase = phaseRemoved
	case actionDelete:
		f.removing, f.phase = "", ""
		f.replace = slices.DeleteFunc(f.replace, func(s string) bool { return s == x })
		f.restart = slices.DeleteFunc(f.restart, func(s string) bool { return s == x })
		if !slices.Contains(f.plan(), x) {
			f.servers = slices.DeleteFunc(f.servers, func(s string) bool { return s == x })
		}
	}
	return d
}

// readmit lets every running server back into the meta group, as the
// meta leader does once a removal tombstone lapses.
func (f *fakeNats) readmit() {
	for _, s := range f.servers {
		if !slices.Contains(f.meta, s) {
			f.meta = append(f.meta, s)
		}
	}
}

// run decides until nothing is left to do, readmitting servers whenever
// the gate waits on the meta group.
func (f *fakeNats) run(t *testing.T) {
	t.Helper()
	for range 50 {
		d := f.decide()
		if d.Status == nil {
			return
		}
		if d.Step == "" && d.Remove == nil {
			f.readmit()
		}
		f.tick(time.Second)
	}
	t.Fatalf("did not finish: %v", f.log)
}

func (f *fakeNats) tick(d time.Duration) { f.now = f.now.Add(d) }

func TestDecide_ScaleDown(t *testing.T) {
	tests := []struct {
		name   string
		leader string
		want   []string
	}{
		{"follower", "demo-0", []string{
			"Evacuate demo-4", "RemovePeer demo-4", "Delete demo-4",
			"Evacuate demo-3", "RemovePeer demo-3", "Delete demo-3",
		}},
		{"meta leader steps down before its removal", "demo-4", []string{
			"Evacuate demo-4", "StepDownMeta demo-4", "RemovePeer demo-4", "Delete demo-4",
			"Evacuate demo-3", "RemovePeer demo-3", "Delete demo-3",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeNats(5, tt.leader)
			f.run(t)
			require.Equal(t, tt.want, f.log)
			require.Equal(t, []string{"demo-0", "demo-1", "demo-2"}, f.servers)
			for name, ms := range f.streams {
				require.Subset(t, f.plan(), ms, name)
			}
			require.Len(t, f.streams["C"], 3, "an R3 stream lost a replica")
		})
	}
}

func TestDecide_ScaleDownWaits(t *testing.T) {
	f := newFakeNats(5, "demo-0")
	d := f.decide()
	require.Equal(t, &removalStep{Server: "demo-4", Action: actionEvacuate}, d.Remove)
	require.Equal(t, ReasonScalingDown, d.Progressing.Reason)
	require.Equal(t, "removing demo-4 (4 of 5); waiting for Evacuated", d.Progressing.Message)
	require.Equal(t, clusterv1beta1.RolloutStatus{
		TargetRevision: "r", Updated: []string{"demo-0", "demo-1", "demo-2"}, Current: "demo-4", Pending: []string{"demo-3"},
		Gate: &clusterv1beta1.RolloutGate{WaitingFor: GateEvacuated, Since: &metav1.Time{Time: f.now}},
	}, *d.Status)

	f.streams["C"] = append(f.streams["C"], "demo-4")
	f.paused = true
	d = f.decide()
	require.Nil(t, d.Remove, "removed a server still holding a group")
	require.Equal(t, GateEvacuated, d.Status.Gate.WaitingFor)

	f.streams["C"] = f.streams["C"][:3]
	f.silent("demo-1")
	d = f.decide()
	require.Nil(t, d.Remove, "removed a server while demo-1 was silent")
	require.Equal(t, GateSettled, d.Status.Gate.WaitingFor)

	f.servers = append(f.servers, "demo-1")
	slices.Sort(f.servers)
	d = f.decide()
	require.Equal(t, &removalStep{Server: "demo-4", Action: actionRemovePeer}, d.Remove, "a begun removal stopped for paused")

	d = f.decide()
	require.Equal(t, &removalStep{Server: "demo-4", Action: actionDelete}, d.Remove)
	d = f.decide()
	require.Nil(t, d.Remove, "started the next removal while paused")
	require.Equal(t, ReasonRolloutPaused, d.Progressing.Reason)
	require.Equal(t, "paused before removing demo-3 (4 of 4)", d.Progressing.Message)
}

// silent stops server answering while it stays a member of everything.
func (f *fakeNats) silent(server string) {
	f.servers = slices.DeleteFunc(f.servers, func(s string) bool { return s == server })
}

func TestDecide_ScaleDownOfDeadServer(t *testing.T) {
	f := newFakeNats(4, "demo-0")
	f.silent("demo-3")
	d := f.decide()
	require.Nil(t, d.Remove, "started a removal with a server silent")

	f.removing, f.phase = "demo-3", phaseEvacuating
	f.streams["D"] = []string{"demo-0"}
	d = f.decide()
	require.Equal(t, &removalStep{Server: "demo-3", Action: actionRemovePeer}, d.Remove, "the removed server's own silence held its removal")
}

func TestDecide_ScaleDownBlocked(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*fakeNats)
		reason  string
		message string
	}{
		{"stream wider than the new size", func(f *fakeNats) { f.streams["C"] = slices.Clone(f.servers) },
			ReasonScaleDownBlocked, "cannot remove demo-4, demo-3: $G/C has 5 replicas; 3 servers cannot hold them"},
		{"no system user", func(f *fakeNats) { f.noAdmin = ErrNoSystemUser.Error() },
			ReasonScaleDownBlocked, "cannot remove demo-4, demo-3: cannot evacuate servers: " + ErrNoSystemUser.Error()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeNats(5, "demo-0")
			tt.mutate(f)
			d := f.decide()
			require.Nil(t, d.Remove)
			require.Nil(t, d.Status)
			require.NotNil(t, d.Blocked)
			require.Equal(t, metav1.ConditionFalse, d.Blocked.Status)
			require.Equal(t, tt.reason, d.Blocked.Reason)
			require.Equal(t, tt.message, d.Blocked.Message)
		})
	}

	t.Run("a blocked shrink holds no restart", func(t *testing.T) {
		f := newFakeNats(5, "demo-0")
		f.streams["C"] = slices.Clone(f.servers)
		f.restart = []string{"demo-1"}
		d := f.decide()
		require.Equal(t, "demo-1", d.Step)
		require.NotNil(t, d.Blocked)
	})
}

func TestDecide_ScaleDownWithoutJetStream(t *testing.T) {
	f := newFakeNats(5, "")
	f.jetStream, f.noAdmin, f.meta, f.streams = false, "no system user", nil, nil
	f.run(t)
	require.Equal(t, []string{"Delete demo-4", "Delete demo-3"}, f.log)
}

func TestDecide_Replace(t *testing.T) {
	t.Run("restarts first, then replacements, the meta leader last", func(t *testing.T) {
		f := newFakeNats(3, "demo-2")
		f.replace = []string{"demo-0", "demo-2"}
		f.restart = []string{"demo-0", "demo-1"}
		f.run(t)
		require.Equal(t, []string{
			"restart demo-1",
			"Evacuate demo-0", "RemovePeer demo-0", "Delete demo-0",
			"Evacuate demo-2", "StepDownMeta demo-2", "RemovePeer demo-2", "Delete demo-2",
		}, f.log, "a server to be replaced was restarted")
	})

	t.Run("a replaced server holds the gate until readmitted", func(t *testing.T) {
		f := newFakeNats(3, "demo-0")
		f.replace = []string{"demo-2"}
		for _, want := range []removalAction{actionEvacuate, actionRemovePeer, actionDelete} {
			d := f.decide()
			require.Equal(t, want, d.Remove.Action)
			require.Equal(t, ReasonReplacingServer, d.Progressing.Reason)
		}
		f.replace = []string{"demo-1"}
		d := f.decide()
		require.Nil(t, d.Remove, "replaced the next server inside the tombstone")
		require.Equal(t, "demo-2", d.Status.Current)
		require.Equal(t, GateSettled, d.Status.Gate.WaitingFor)
		f.tick(gateBlockedAfter)
		d = f.decide()
		require.Equal(t, ReasonGateBlocked, d.Progressing.Reason)
		require.Contains(t, d.Progressing.Message, "demo-2 not in the meta group")

		f.readmit()
		d = f.decide()
		require.Equal(t, &removalStep{Server: "demo-1", Action: actionEvacuate}, d.Remove)
	})

	t.Run("blocked without a system user", func(t *testing.T) {
		f := newFakeNats(3, "demo-0")
		f.replace, f.noAdmin = []string{"demo-2"}, "no system user"
		d := f.decide()
		require.Nil(t, d.Remove)
		require.Equal(t, ReasonReplacementBlocked, d.Blocked.Reason)
		require.Equal(t, "cannot replace demo-2: cannot evacuate servers: no system user", d.Blocked.Message)
	})
}

func TestUnsettledWithout(t *testing.T) {
	g := sysobs.Group{Kind: sysobs.KindStream, Stream: "A"}
	tests := []struct {
		name string
		v    sysobs.Verdict
		want string
	}{
		{"settled", sysobs.Verdict{}, ""},
		{"only the server itself silent and offline", sysobs.Verdict{
			Silent:    []string{"demo-3"},
			Unsettled: []sysobs.Unsettled{{Group: sysobs.Group{Kind: sysobs.KindMeta}, Reason: sysobs.ReasonMemberOffline, Servers: []string{"demo-3"}}},
		}, ""},
		{"another server silent", sysobs.Verdict{Silent: []string{"demo-3", "demo-1"}}, "demo-1 did not answer"},
		{"a group behind elsewhere", sysobs.Verdict{
			Unsettled: []sysobs.Unsettled{{Group: g, Reason: sysobs.ReasonMemberBehind, Servers: []string{"demo-3", "demo-0"}}},
		}, "stream A MemberBehind on demo-3, demo-0"},
		{"a leaderless group", sysobs.Verdict{Unsettled: []sysobs.Unsettled{{Group: g, Reason: sysobs.ReasonNoLeader}}}, "stream A NoLeader"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, unsettledWithout(tt.v, "demo-3"))
		})
	}
}

// TestDeletingCondition_Story11 pins the status story 11 shows for a
// deleted NatsCluster whose NATS cluster still holds stream groups.
func TestDeletingCondition_Story11(t *testing.T) {
	b, err := os.ReadFile("../../docs/content/docs/stories/11-evacuation/02-status-natscluster-prod-east.yaml")
	require.NoError(t, err)
	var want clusterv1beta1.NatsCluster
	require.NoError(t, yaml.UnmarshalStrict(b, &want))
	require.Len(t, want.Status.Conditions, 1)

	members := []sysobs.Member{{Server: "prod-east-0", Current: true}}
	snap := &sysobs.Snapshot{Groups: []sysobs.Group{
		{Kind: sysobs.KindMeta, Leader: "prod-east-0", Members: members},
		{Kind: sysobs.KindStream, Account: "AORDERS", AccountName: "orders", Stream: "ORDERS", Leader: "prod-east-0", Members: members},
		{Kind: sysobs.KindConsumer, Account: "AORDERS", AccountName: "orders", Stream: "ORDERS", Consumer: "c", Leader: "prod-east-0", Members: members},
		{Kind: sysobs.KindStream, Account: "APAYMENTS", AccountName: "payments", Stream: "KV_sessions", Leader: "prod-east-0", Members: members},
	}}
	got := deletingCondition("prod-east", snap, nil)
	w := want.Status.Conditions[0]
	require.Equal(t, w.Type, got.Type)
	require.Equal(t, w.Status, got.Status)
	require.Equal(t, w.Reason, got.Reason)
	require.Equal(t, w.Message, got.Message)
}

func TestDeletingCondition(t *testing.T) {
	tests := []struct {
		name    string
		snap    *sysobs.Snapshot
		err     error
		status  metav1.ConditionStatus
		reason  string
		message string
	}{
		{"no stream groups", &sysobs.Snapshot{Groups: []sysobs.Group{{Kind: sysobs.KindMeta, Leader: "demo-0"}}},
			nil, metav1.ConditionFalse, ReasonNoJetStreamData, ""},
		{"not observed", nil, sysobs.ErrNoServers, metav1.ConditionTrue, ReasonObservationFailed,
			"cannot tell whether JetStream data remains: " + sysobs.ErrNoServers.Error()},
		{"an untagged account by its ID", &sysobs.Snapshot{Groups: []sysobs.Group{{Kind: sysobs.KindStream, Account: "$G", Stream: "A"}}},
			nil, metav1.ConditionTrue, ReasonJetStreamDataRemains, "1 stream groups still placed in demo ($G/A)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := deletingCondition("demo", tt.snap, tt.err)
			require.Equal(t, tt.status, c.Status)
			require.Equal(t, tt.reason, c.Reason)
			require.Equal(t, tt.message, c.Message)
		})
	}
}
