package natscluster

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/e2e"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

func readySets(plan *Plan, ready ...bool) map[string]*appsv1.StatefulSet {
	out := map[string]*appsv1.StatefulSet{}
	for i, s := range plan.Servers {
		sts := s.StatefulSet.DeepCopy()
		if i < len(ready) && ready[i] {
			sts.Status.ReadyReplicas = 1
		}
		out[s.Name] = sts
	}
	return out
}

// settledSnapshot is a Settled observation of plan's servers on revision,
// with a meta group led by leader and one R3 stream.
func settledSnapshot(plan *Plan, revision, leader string) *sysobs.Snapshot {
	snap := &sysobs.Snapshot{}
	var members []sysobs.Member
	for _, s := range plan.Servers {
		snap.Servers = append(snap.Servers, sysobs.Server{
			Name: s.Name, Version: "2.15.0", JetStream: true,
			Metadata: map[string]string{MetadataConfigRevision: revision},
		})
		members = append(members, sysobs.Member{Server: s.Name, Current: true})
	}
	snap.Groups = []sysobs.Group{
		{Kind: sysobs.KindMeta, Leader: leader, Members: members},
		{Kind: sysobs.KindStream, Account: "$G", Stream: "ORDERS", Leader: leader, Members: members},
	}
	return snap
}

// TestComputeStatus_AtRest pins the status story 1 shows at rest, its
// revision aside.
func TestComputeStatus_AtRest(t *testing.T) {
	nc := storyCluster(t)
	nc.Generation = 1
	plan, err := Render(nc, Inputs{})
	require.NoError(t, err)

	got := computeStatus(nc, plan, Observed{
		StatefulSets: readySets(plan, true, true, true),
		Snapshot:     settledSnapshot(plan, plan.Revision, "demo-1"),
	})

	b, err := os.ReadFile("../../docs/content/docs/stories/01-quickstart/01-status-natscluster-at-rest.yaml")
	require.NoError(t, err)
	var want clusterv1beta1.NatsCluster
	require.NoError(t, yaml.UnmarshalStrict(b, &want))
	want.Status.Config.Revision = plan.Revision
	for i := range want.Status.Servers {
		want.Status.Servers[i].ConfigRevision = plan.Revision
	}

	require.Len(t, got.Conditions, len(want.Status.Conditions))
	for _, w := range want.Status.Conditions {
		c := meta.FindStatusCondition(got.Conditions, w.Type)
		require.NotNil(t, c, w.Type)
		require.Equal(t, w.Status, c.Status, w.Type)
		require.Equal(t, w.Reason, c.Reason, w.Type)
		if w.Message != "" {
			require.Equal(t, w.Message, c.Message, w.Type)
		}
		require.Equal(t, int64(1), c.ObservedGeneration)
	}
	require.Equal(t, want.Status.JetStream.Limits.MaxMemoryStore.String(), got.JetStream.Limits.MaxMemoryStore.String())
	require.Equal(t, want.Status.JetStream.Limits.MaxFileStore.String(), got.JetStream.Limits.MaxFileStore.String())
	got.Conditions, want.Status.Conditions = nil, nil
	require.Equal(t, want.Status, got)
}

// TestComputeStatus_MidRollout pins the status story 1 shows in the middle
// of a memory raise, its revisions aside, as the rollout decides it.
func TestComputeStatus_MidRollout(t *testing.T) {
	b, err := os.ReadFile("../../docs/content/docs/stories/01-quickstart/02-status-natscluster-mid-rollout.yaml")
	require.NoError(t, err)
	b, err = e2e.StripPlaceholders(b)
	require.NoError(t, err)
	var want clusterv1beta1.NatsCluster
	require.NoError(t, yaml.UnmarshalStrict(b, &want))

	nc := storyCluster(t)
	nc.Generation = 2
	eight := resource.MustParse("8Gi")
	nc.Spec.Resources.Requests[corev1.ResourceMemory] = eight
	nc.Spec.Resources.Limits[corev1.ResourceMemory] = eight
	plan, err := Render(nc, Inputs{})
	require.NoError(t, err)
	const old = "3f9a1c"
	storyRevision := want.Status.Rollout.TargetRevision
	want.Status.Config.Revision = plan.Revision
	want.Status.Rollout.TargetRevision = plan.Revision
	for i := range want.Status.Servers {
		if want.Status.Servers[i].ConfigRevision == storyRevision {
			want.Status.Servers[i].ConfigRevision = plan.Revision
		}
	}
	nc.Status.Version = "2.15.0"
	nc.Status.Config = want.Status.Config.DeepCopy()
	nc.Status.Rollout = want.Status.Rollout.DeepCopy()

	sets := map[string]*appsv1.StatefulSet{}
	for _, s := range plan.Servers {
		sts := s.StatefulSet.DeepCopy()
		sts.Status.ObservedGeneration, sts.Status.UpdatedReplicas, sts.Status.ReadyReplicas = sts.Generation, 1, 1
		sets[s.Name] = sts
	}
	sets["demo-0"].Annotations[AnnotationConfigRevision] = old
	sets["demo-1"].Status.ReadyReplicas = 0

	snap := &sysobs.Snapshot{}
	for _, s := range []struct{ name, version, revision string }{
		{"demo-0", "2.15.0", old}, {"demo-1", "2.15.0", plan.Revision}, {"demo-2", "2.15.0", plan.Revision},
	} {
		snap.Servers = append(snap.Servers, sysobs.Server{Name: s.name, Version: s.version, JetStream: true,
			Metadata: map[string]string{MetadataConfigRevision: s.revision}})
	}
	members := []sysobs.Member{{Server: "demo-0", Current: true}, {Server: "demo-1"}, {Server: "demo-2", Current: true}}
	snap.Groups = []sysobs.Group{{Kind: sysobs.KindMeta, Leader: "demo-0", Members: members}}
	for _, stream := range []string{"A", "B", "C"} {
		snap.Groups = append(snap.Groups, sysobs.Group{Kind: sysobs.KindStream, Stream: stream, Leader: "demo-0", Members: members})
	}

	o := Observed{
		StatefulSets: sets,
		Snapshot:     snap,
		Apply:        configApply{Restart: map[string]string{"demo-0": "the StatefulSet spec is restart-only"}},
	}
	r := &Reconciler{Now: func() time.Time { return want.Status.Rollout.Gate.Since.Add(time.Minute) }}
	o.Rollout = decide(r.rolloutState(nc, plan, o))
	require.Empty(t, o.Rollout.Step)
	got := computeStatus(nc, plan, o)

	require.Len(t, got.Conditions, len(want.Status.Conditions))
	for _, w := range want.Status.Conditions {
		c := meta.FindStatusCondition(got.Conditions, w.Type)
		require.NotNil(t, c, w.Type)
		require.Equal(t, w.Status, c.Status, w.Type)
		require.Equal(t, w.Reason, c.Reason, w.Type)
		require.Equal(t, w.Message, c.Message, w.Type)
		require.Equal(t, int64(2), c.ObservedGeneration)
	}
	require.Equal(t, want.Status.Rollout, got.Rollout)
	require.Equal(t, want.Status.Config, got.Config)
	require.Equal(t, want.Status.Servers, got.Servers)
	require.Equal(t, want.Status.Version, got.Version)
	require.Equal(t, want.Status.ReadyReplicas, got.ReadyReplicas)
	require.Equal(t, want.Status.JetStream.MetaLeader, got.JetStream.MetaLeader)
}

func TestComputeStatus_KeepsUnobservedVersion(t *testing.T) {
	nc := storyCluster(t)
	nc.Status.Version = "2.15.0"
	plan, err := Render(nc, Inputs{})
	require.NoError(t, err)
	got := computeStatus(nc, plan, Observed{StatefulSets: readySets(plan), ObserveErr: sysobs.ErrNoServers})
	require.Equal(t, "2.15.0", got.Version)
	require.Empty(t, got.JetStream.MetaLeader)
	for _, s := range got.Servers {
		require.Empty(t, s.Version)
		require.Empty(t, s.ConfigRevision)
	}
}

func TestReadyCondition(t *testing.T) {
	tests := []struct {
		ready, want int32
		status      metav1.ConditionStatus
		reason      string
	}{
		{3, 3, metav1.ConditionTrue, ReasonAllServersReady},
		{2, 3, metav1.ConditionTrue, ReasonQuorumAvailable},
		{1, 3, metav1.ConditionFalse, ReasonQuorumUnavailable},
		{0, 1, metav1.ConditionFalse, ReasonQuorumUnavailable},
		{2, 4, metav1.ConditionFalse, ReasonQuorumUnavailable},
	}
	for _, tt := range tests {
		c := readyCondition(tt.ready, tt.want)
		require.Equal(t, tt.status, c.Status, "%d of %d", tt.ready, tt.want)
		require.Equal(t, tt.reason, c.Reason, "%d of %d", tt.ready, tt.want)
	}
}

func TestSettledCondition(t *testing.T) {
	stream := func(leader string, members ...sysobs.Member) sysobs.Group {
		return sysobs.Group{Kind: sysobs.KindStream, Stream: "S" + leader, Leader: leader, Members: members}
	}
	tests := []struct {
		name    string
		obs     Observed
		status  metav1.ConditionStatus
		reason  string
		message string
	}{
		{"no observation", Observed{ObserveErr: errors.New("boom")}, metav1.ConditionUnknown, ReasonObservationFailed, "boom"},
		{"settled", Observed{Snapshot: &sysobs.Snapshot{}}, metav1.ConditionTrue, ReasonAllGroupsCurrent,
			"every Raft group has a leader and every member is current"},
		{"silent outranks the rest", Observed{Snapshot: &sysobs.Snapshot{Silent: []string{"demo-2"}, Groups: []sysobs.Group{{Kind: sysobs.KindStream}}}},
			metav1.ConditionFalse, ReasonServersSilent, "demo-2 did not answer"},
		{"leaderless", Observed{Snapshot: &sysobs.Snapshot{Groups: []sysobs.Group{{Kind: sysobs.KindStream}, stream("demo-0", sysobs.Member{Server: "demo-1", Offline: true})}}},
			metav1.ConditionFalse, ReasonGroupsLeaderless, "1 Raft groups have no leader"},
		{"offline", Observed{Snapshot: &sysobs.Snapshot{Groups: []sysobs.Group{
			stream("demo-0", sysobs.Member{Server: "demo-1", Offline: true}),
			stream("demo-2", sysobs.Member{Server: "demo-0"}),
		}}}, metav1.ConditionFalse, ReasonMembersOffline, "1 Raft groups have a member offline on demo-1"},
		{"catching up", Observed{Snapshot: &sysobs.Snapshot{Groups: []sysobs.Group{
			stream("demo-0", sysobs.Member{Server: "demo-1"}),
			stream("demo-2", sysobs.Member{Server: "demo-1"}),
		}}}, metav1.ConditionFalse, ReasonGroupsCatchingUp, "2 Raft groups have a member on demo-1 that is not current"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := settledCondition(tt.obs)
			require.Equal(t, tt.status, c.Status)
			require.Equal(t, tt.reason, c.Reason)
			require.Equal(t, tt.message, c.Message)
		})
	}
}

func TestProgressingCondition(t *testing.T) {
	nc := storyCluster(t)
	plan, err := Render(nc, Inputs{})
	require.NoError(t, err)
	stale := readySets(plan)
	stale["demo-1"].Annotations[AnnotationConfigRevision] = "old"
	twoStale := readySets(plan)
	twoStale["demo-1"].Annotations[AnnotationConfigRevision] = "old"
	twoStale["demo-2"].Annotations[AnnotationConfigRevision] = "old"
	extra := readySets(plan)
	extra["demo-3"] = &appsv1.StatefulSet{}
	rolling := rolloutDecision{
		Status: &clusterv1beta1.RolloutStatus{TargetRevision: plan.Revision, Current: "demo-1"},
		Progressing: metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionTrue, Reason: ReasonRollingRestart,
			Message: "restarting demo-1 (1 of 1)"},
	}

	blocked := metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: ReasonScaleDownBlocked, Message: "cannot remove demo-3: x"}
	held := metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: ReasonTrustNotFound, Message: "NatsOperatorTrust demo/t does not exist"}

	tests := []struct {
		name    string
		obs     Observed
		status  metav1.ConditionStatus
		reason  string
		message string
	}{
		{"up to date", Observed{StatefulSets: readySets(plan)}, metav1.ConditionFalse, ReasonUpToDate, ""},
		{"creating", Observed{StatefulSets: readySets(plan), Created: []string{"demo-0", "demo-2"}}, metav1.ConditionTrue, ReasonCreating, "creating demo-0, demo-2"},
		{"rolling out", Observed{StatefulSets: stale, Apply: configApply{Restart: map[string]string{"demo-1": "x"}}, Rollout: rolling}, metav1.ConditionTrue, ReasonRollingRestart, "restarting demo-1 (1 of 1)"},
		{"reloading", Observed{StatefulSets: stale, Apply: configApply{Reloading: []string{"demo-1"}}}, metav1.ConditionTrue, ReasonReloadPending, "reloading demo-1 to revision " + plan.Revision},
		{"a rollout outranks a reload", Observed{StatefulSets: twoStale, Apply: configApply{Reloading: []string{"demo-1"}, Restart: map[string]string{"demo-2": "x"}}, Rollout: rolling}, metav1.ConditionTrue, ReasonRollingRestart, "restarting demo-1 (1 of 1)"},
		{"beyond replicas", Observed{StatefulSets: extra}, metav1.ConditionTrue, ReasonScaleDownPending, "demo-3 beyond 3 replicas"},
		{"blocked", Observed{StatefulSets: readySets(plan), Rollout: rolloutDecision{Blocked: &blocked}}, metav1.ConditionFalse, ReasonScaleDownBlocked, blocked.Message},
		{"a reload outranks a blocked scale-down", Observed{StatefulSets: stale, Rollout: rolloutDecision{Blocked: &blocked}}, metav1.ConditionTrue, ReasonReloadPending, "reloading demo-1 to revision " + plan.Revision},
		{"a blocked scale-down outranks servers beyond replicas", Observed{StatefulSets: extra, Rollout: rolloutDecision{Blocked: &blocked}}, metav1.ConditionFalse, ReasonScaleDownBlocked, blocked.Message},
		{"waiting for a claim", Observed{StatefulSets: readySets(plan), ClaimTerminating: []string{"demo-1"}}, metav1.ConditionTrue, ReasonClaimTerminating, "waiting for the deletion of " + dataClaimName("demo-1")},
		{"creating outranks waiting for a claim", Observed{StatefulSets: readySets(plan), Created: []string{"demo-0"}, ClaimTerminating: []string{"demo-1"}}, metav1.ConditionTrue, ReasonCreating, "creating demo-0"},
		{"waiting for a claim outranks a rollout", Observed{StatefulSets: stale, ClaimTerminating: []string{"demo-1"}, Rollout: rolling}, metav1.ConditionTrue, ReasonClaimTerminating, "waiting for the deletion of " + dataClaimName("demo-1")},
		{"creating outranks a rollout", Observed{StatefulSets: stale, Created: []string{"demo-0"}, Rollout: rolling}, metav1.ConditionTrue, ReasonCreating, "creating demo-0"},
		{"a certificate wait outranks creating", Observed{StatefulSets: stale, CertWait: "waiting for demo-routes", CertReason: ReasonRouteCertNotReady, Created: []string{"demo-0"}, Rollout: rolling},
			metav1.ConditionTrue, ReasonRouteCertNotReady, "waiting for demo-routes"},
		{"held outranks everything", Observed{Held: &held, StatefulSets: stale, CertWait: "waiting for demo-routes", CertReason: ReasonRouteCertNotReady, Created: []string{"demo-0"}, Rollout: rolling},
			metav1.ConditionFalse, ReasonTrustNotFound, held.Message},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := progressingCondition(nc, plan, tt.obs)
			require.Equal(t, tt.status, c.Status)
			require.Equal(t, tt.reason, c.Reason)
			require.Equal(t, tt.message, c.Message)
		})
	}
}

func TestConfigStatus(t *testing.T) {
	plan, err := Render(storyCluster(t), Inputs{})
	require.NoError(t, err)
	reload, restart := clusterv1beta1.ConfigAppliedByReload, clusterv1beta1.ConfigAppliedByRestart
	tests := []struct {
		name      string
		prev      *clusterv1beta1.ConfigStatus
		apply     configApply
		appliedBy clusterv1beta1.ConfigApplyMethod
		reason    string
	}{
		{"created", nil, configApply{}, restart, ""},
		{"reloading", nil, configApply{Reloading: []string{"demo-0"}}, reload, ""},
		{"reloaded", nil, configApply{Reloaded: []string{"demo-0"}}, reload, ""},
		{"restart wins", nil, configApply{Reloaded: []string{"demo-0"}, Restart: map[string]string{"demo-1": "a", "demo-2": "b", "demo-0": "a"}}, restart, "a; b"},
		{"kept for the same revision", &clusterv1beta1.ConfigStatus{Revision: plan.Revision, AppliedBy: reload}, configApply{}, reload, ""},
		{"not kept for another revision", &clusterv1beta1.ConfigStatus{Revision: "old", AppliedBy: reload}, configApply{}, restart, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := configStatus(tt.prev, plan, tt.apply)
			require.Equal(t, plan.Revision, got.Revision)
			require.Equal(t, tt.appliedBy, got.AppliedBy)
			require.Equal(t, tt.reason, got.RestartReason)
		})
	}
}
