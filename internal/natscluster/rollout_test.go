package natscluster

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// fakeCluster is a NATS cluster as decide sees it, moved along by the
// decisions it takes: a stepped server goes down and comes back only when
// the test says so.
type fakeCluster struct {
	target    string
	servers   []rolloutServer
	meta      string
	unsettled []sysobs.Unsettled
	silent    []string
	paused    bool
	force     string
	prev      *clusterv1beta1.RolloutStatus
	now       time.Time
}

// newFakeCluster is n servers, all Ready and Settled on revision "old",
// each waiting for a restart to "new".
func newFakeCluster(n int, meta string) *fakeCluster {
	f := &fakeCluster{target: "new", meta: meta, now: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)}
	for i := range n {
		f.servers = append(f.servers, rolloutServer{Name: "demo-" + string(rune('0'+i)), Restart: true, Ready: true, Revision: "old"})
	}
	return f
}

func (f *fakeCluster) server(name string) *rolloutServer {
	i := slices.IndexFunc(f.servers, func(s rolloutServer) bool { return s.Name == name })
	return &f.servers[i]
}

func (f *fakeCluster) state() rolloutState {
	v := sysobs.Verdict{Unsettled: f.unsettled, Silent: f.silent}
	return rolloutState{
		Target: f.target, Servers: slices.Clone(f.servers), MetaLeader: f.meta, Verdict: &v,
		Paused: f.paused, ForceStep: f.force, Prev: f.prev, Now: f.now,
	}
}

// decide takes one decision and applies it: the stepped server goes on the
// target revision, down and silent, its groups unsettled.
func (f *fakeCluster) decide() rolloutDecision {
	d := decide(f.state())
	f.prev = d.Status
	if d.ClearForceStep {
		f.force = ""
	}
	if d.Step != "" {
		s := f.server(d.Step)
		s.OnTarget, s.Restart, s.Ready, s.Revision = true, false, false, ""
		f.silent = []string{d.Step}
	}
	return d
}

// recover brings server back on the target revision with the NATS
// cluster settled.
func (f *fakeCluster) recover(server string) {
	s := f.server(server)
	s.Ready, s.Revision = true, f.target
	f.silent, f.unsettled = nil, nil
}

func (f *fakeCluster) tick(d time.Duration) { f.now = f.now.Add(d) }

func requireProgressing(t *testing.T, d rolloutDecision, reason, message string) {
	t.Helper()
	require.NotNil(t, d.Status)
	require.Equal(t, metav1.ConditionTrue, d.Progressing.Status)
	require.Equal(t, reason, d.Progressing.Reason, d.Progressing.Message)
	require.Equal(t, message, d.Progressing.Message)
}

// runRollout steps f to completion, recovering each server two decisions
// after its step, and returns the order the servers were stepped in.
// moveMeta, when set, is called after each step to move the meta leader.
func runRollout(t *testing.T, f *fakeCluster, moveMeta func(stepped string) string) []string {
	t.Helper()
	var order []string
	for range 20 {
		d := f.decide()
		if d.Status == nil {
			return order
		}
		if d.Step == "" {
			t.Fatalf("no step with an open gate: %+v", d.Progressing)
		}
		order = append(order, d.Step)
		held := f.decide()
		require.Empty(t, held.Step, "stepped %s while %s was down", held.Step, d.Step)
		require.Equal(t, d.Step, held.Status.Current)
		f.recover(d.Step)
		if moveMeta != nil {
			f.meta = moveMeta(d.Step)
		}
	}
	t.Fatal("rollout did not finish")
	return nil
}

func TestDecide_Order(t *testing.T) {
	tests := []struct {
		name     string
		n        int
		meta     string
		moveMeta func(string) string
		want     []string
	}{
		{"meta leader on the lowest ordinal", 3, "demo-0", nil, []string{"demo-2", "demo-1", "demo-0"}},
		{"meta leader on the highest ordinal", 3, "demo-2", nil, []string{"demo-1", "demo-0", "demo-2"}},
		{"meta leader in the middle", 5, "demo-3", nil, []string{"demo-4", "demo-2", "demo-1", "demo-0", "demo-3"}},
		{"meta leader moves onto the next in line", 3, "demo-0", func(string) string { return "demo-1" },
			[]string{"demo-2", "demo-0", "demo-1"}},
		{"no meta leader", 3, "", nil, []string{"demo-2", "demo-1", "demo-0"}},
		{"one server", 1, "demo-0", nil, []string{"demo-0"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeCluster(tt.n, tt.meta)
			require.Equal(t, tt.want, runRollout(t, f, tt.moveMeta))
			for _, s := range f.servers {
				require.True(t, s.OnTarget, s.Name)
			}
		})
	}
}

func TestDecide_Status(t *testing.T) {
	f := newFakeCluster(3, "demo-0")
	d := f.decide()
	require.Equal(t, "demo-2", d.Step)
	require.Equal(t, &clusterv1beta1.RolloutStatus{
		TargetRevision: "new",
		Current:        "demo-2",
		Pending:        []string{"demo-1", "demo-0"},
		Gate:           &clusterv1beta1.RolloutGate{WaitingFor: GateSettled, Since: &metav1.Time{Time: f.now}},
	}, d.Status)
	requireProgressing(t, d, ReasonRollingRestart, "restarting demo-2 (1 of 3); waiting for Settled")

	f.recover("demo-2")
	d = f.decide()
	require.Equal(t, "demo-1", d.Step)
	require.Equal(t, []string{"demo-2"}, d.Status.Updated)
	require.Equal(t, "demo-1", d.Status.Current)
	require.Equal(t, []string{"demo-0"}, d.Status.Pending)
	requireProgressing(t, d, ReasonRollingRestart, "restarting demo-1 (2 of 3); waiting for Settled")

	f.recover("demo-1")
	require.Equal(t, "demo-0", f.decide().Step)
	f.recover("demo-0")
	d = f.decide()
	require.Nil(t, d.Status, "a finished rollout has no status")
	require.Empty(t, d.Step)
}

// TestDecide_Gate pins each thing that holds the gate after a step, in
// the order they are judged.
func TestDecide_Gate(t *testing.T) {
	behind := sysobs.Unsettled{Group: sysobs.Group{Kind: sysobs.KindStream, Stream: "ORDERS"}, Reason: sysobs.ReasonMemberBehind, Servers: []string{"demo-2"}}
	tests := []struct {
		name       string
		mutate     func(*fakeCluster)
		unobserved bool
		waitingFor string
	}{
		{"server silent", func(f *fakeCluster) {}, false, GateSettled},
		{"unobserved", func(f *fakeCluster) { f.recover("demo-2") }, true, GateSettled},
		{"groups catching up", func(f *fakeCluster) {
			f.silent, f.unsettled = nil, []sysobs.Unsettled{behind}
			f.server("demo-2").Ready, f.server("demo-2").Revision = true, "new"
		}, false, GateSettled},
		{"settled, pod not ready", func(f *fakeCluster) { f.silent = nil }, false, GateReady},
		{"ready, old revision reported", func(f *fakeCluster) {
			f.silent = nil
			f.server("demo-2").Ready, f.server("demo-2").Revision = true, "old"
		}, false, GateTargetRevision},
		{"another server not ready", func(f *fakeCluster) {
			f.recover("demo-2")
			f.server("demo-0").Ready = false
		}, false, GateReady},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeCluster(3, "demo-0")
			require.Equal(t, "demo-2", f.decide().Step)
			tt.mutate(f)
			st := f.state()
			if tt.unobserved {
				st.Verdict = nil
			}
			d := decide(st)
			require.Empty(t, d.Step)
			require.Equal(t, "demo-2", d.Status.Current)
			require.NotNil(t, d.Status.Gate)
			require.Equal(t, tt.waitingFor, d.Status.Gate.WaitingFor)
		})
	}
}

// TestDecide_GateBlocked pins that a closed gate holds with no timeout,
// reading GateBlocked with the lagging groups once it has been closed for
// gateBlockedAfter.
func TestDecide_GateBlocked(t *testing.T) {
	f := newFakeCluster(3, "demo-0")
	require.Equal(t, "demo-2", f.decide().Step)
	since := f.now
	s := f.server("demo-2")
	s.Ready, s.Revision = true, "new"
	f.silent = nil
	for i := range 7 {
		f.unsettled = append(f.unsettled, sysobs.Unsettled{
			Group:  sysobs.Group{Kind: sysobs.KindConsumer, Stream: "ORDERS", Consumer: "C" + string(rune('0'+i))},
			Reason: sysobs.ReasonMemberBehind, Servers: []string{"demo-2"},
		})
	}

	f.tick(gateBlockedAfter - time.Second)
	d := f.decide()
	require.Empty(t, d.Step)
	requireProgressing(t, d, ReasonRollingRestart, "restarting demo-2 (1 of 3); waiting for Settled")

	f.tick(time.Second)
	d = f.decide()
	require.Empty(t, d.Step)
	require.Equal(t, since, d.Status.Gate.Since.Time)
	requireProgressing(t, d, ReasonGateBlocked, "restarting demo-2 (1 of 3); waiting for Settled: "+
		"consumer ORDERS/C0 MemberBehind on demo-2; consumer ORDERS/C1 MemberBehind on demo-2; "+
		"consumer ORDERS/C2 MemberBehind on demo-2; consumer ORDERS/C3 MemberBehind on demo-2; "+
		"consumer ORDERS/C4 MemberBehind on demo-2; and 2 more")

	blocked := d.Progressing.Message
	f.tick(24 * time.Hour)
	d = f.decide()
	require.Empty(t, d.Step, "a closed gate timed out")
	require.Equal(t, ReasonGateBlocked, d.Progressing.Reason)
	require.Equal(t, blocked, d.Progressing.Message, "the message changes with the gate's age, so each status patch re-enqueues")

	f.recover("demo-2")
	d = f.decide()
	require.Equal(t, "demo-1", d.Step)
	require.Equal(t, f.now, d.Status.Gate.Since.Time)
	require.Equal(t, ReasonRollingRestart, d.Progressing.Reason)
}

func TestDecide_Paused(t *testing.T) {
	f := newFakeCluster(3, "demo-0")
	f.paused = true
	d := f.decide()
	require.Empty(t, d.Step)
	require.Nil(t, d.Status.Gate)
	requireProgressing(t, d, ReasonRolloutPaused, "paused before restarting demo-2 (1 of 3)")

	f.paused = false
	require.Equal(t, "demo-2", f.decide().Step)

	f.paused = true
	d = f.decide()
	require.Empty(t, d.Step)
	requireProgressing(t, d, ReasonRollingRestart, "restarting demo-2 (1 of 3); waiting for Settled")

	f.recover("demo-2")
	d = f.decide()
	require.Empty(t, d.Step, "stepped while paused")
	requireProgressing(t, d, ReasonRolloutPaused, "paused before restarting demo-1 (2 of 3)")
	require.Equal(t, []string{"demo-2"}, d.Status.Updated)
}

func TestDecide_ForceStep(t *testing.T) {
	t.Run("through a closed gate", func(t *testing.T) {
		f := newFakeCluster(3, "demo-0")
		require.Equal(t, "demo-2", f.decide().Step)
		f.force = "demo-0"
		d := f.decide()
		require.Equal(t, "demo-0", d.Step, "the named server, not the next in order")
		require.True(t, d.ClearForceStep)
		require.Equal(t, "demo-0", d.Status.Current)
		require.Equal(t, []string{"demo-1"}, d.Status.Pending)

		d = f.decide()
		require.Empty(t, d.Step, "a force-step acted more than once")
		require.False(t, d.ClearForceStep)
	})
	t.Run("through paused", func(t *testing.T) {
		f := newFakeCluster(3, "demo-0")
		f.paused, f.force = true, "demo-1"
		d := f.decide()
		require.Equal(t, "demo-1", d.Step)
		require.True(t, d.ClearForceStep)
	})
	t.Run("naming a server not waiting is cleared", func(t *testing.T) {
		f := newFakeCluster(3, "demo-0")
		require.Equal(t, "demo-2", f.decide().Step)
		for _, name := range []string{"demo-2", "demo-9"} {
			f.force = name
			d := f.decide()
			require.Empty(t, d.Step, name)
			require.True(t, d.ClearForceStep, name)
		}
	})
	t.Run("without a rollout is cleared", func(t *testing.T) {
		f := newFakeCluster(1, "demo-0")
		f.servers[0] = rolloutServer{Name: "demo-0", OnTarget: true, Ready: true, Revision: "new"}
		f.force = "demo-0"
		d := f.decide()
		require.Nil(t, d.Status)
		require.True(t, d.ClearForceStep)
	})
}

// TestDecide_InvoluntaryDisruption pins that a server lost without the
// rollout asking holds the next step exactly as a restarted one does.
func TestDecide_InvoluntaryDisruption(t *testing.T) {
	t.Run("before the first step", func(t *testing.T) {
		f := newFakeCluster(3, "demo-0")
		f.server("demo-1").Ready = false
		f.unsettled = []sysobs.Unsettled{{Group: sysobs.Group{Kind: sysobs.KindMeta}, Reason: sysobs.ReasonMemberOffline, Servers: []string{"demo-1"}}}
		d := f.decide()
		require.Empty(t, d.Step)
		require.Equal(t, GateSettled, d.Status.Gate.WaitingFor)
		requireProgressing(t, d, ReasonRollingRestart, "before restarting demo-2 (1 of 3); waiting for Settled")

		f.unsettled = nil
		d = f.decide()
		require.Empty(t, d.Step)
		require.Equal(t, GateReady, d.Status.Gate.WaitingFor)

		f.tick(gateBlockedAfter)
		d = f.decide()
		requireProgressing(t, d, ReasonGateBlocked, "before restarting demo-2 (1 of 3); waiting for Ready: demo-1 not ready")

		f.server("demo-1").Ready = true
		require.Equal(t, "demo-2", f.decide().Step)
	})
	t.Run("between steps", func(t *testing.T) {
		f := newFakeCluster(3, "demo-0")
		require.Equal(t, "demo-2", f.decide().Step)
		f.recover("demo-2")
		f.server("demo-0").Ready = false
		f.silent = []string{"demo-0"}
		d := f.decide()
		require.Empty(t, d.Step)
		require.Equal(t, "demo-2", d.Status.Current, "the restarted server's gate is the whole cluster's")
		require.Empty(t, d.Status.Updated)
		require.Equal(t, GateSettled, d.Status.Gate.WaitingFor)

		f.recover("demo-0")
		require.Equal(t, "demo-1", f.decide().Step)
	})
}

func TestDecide_NewTargetMidRollout(t *testing.T) {
	f := newFakeCluster(3, "demo-0")
	require.Equal(t, "demo-2", f.decide().Step)
	f.target = "newer"
	for i := range f.servers {
		f.servers[i].OnTarget, f.servers[i].Restart = false, true
	}
	d := f.decide()
	require.Empty(t, d.Step, "stepped while the restarted server is down")
	require.Empty(t, d.Status.Current)
	require.Equal(t, "newer", d.Status.TargetRevision)
	require.Equal(t, []string{"demo-2", "demo-1", "demo-0"}, d.Status.Pending)
	require.Equal(t, f.now, d.Status.Gate.Since.Time, "the gate of another revision carried over")
}

func TestPodReady(t *testing.T) {
	sts := func(gen, observed int64, updated, ready int32) *appsv1.StatefulSet {
		s := &appsv1.StatefulSet{}
		s.Generation = gen
		s.Status.ObservedGeneration, s.Status.UpdatedReplicas, s.Status.ReadyReplicas = observed, updated, ready
		return s
	}
	require.False(t, podReady(nil))
	require.True(t, podReady(sts(2, 2, 1, 1)))
	require.False(t, podReady(sts(2, 1, 1, 1)), "status of the previous template")
	require.False(t, podReady(sts(2, 2, 0, 1)), "the ready pod is on the previous template")
	require.False(t, podReady(sts(2, 2, 1, 0)))
}
