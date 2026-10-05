package natscluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

func TestJoinsMetaGroup(t *testing.T) {
	remotes := func(names ...string) *clusterv1beta1.Gateway {
		g := &clusterv1beta1.Gateway{}
		for _, n := range names {
			g.Remotes = append(g.Remotes, clusterv1beta1.GatewayRemote{Name: n, URL: "nats://" + n + ":7222"})
		}
		return g
	}
	tests := []struct {
		name      string
		jetstream *clusterv1beta1.JetStream
		gateway   *clusterv1beta1.Gateway
		want      bool
	}{
		{"jetstream and a remote", &clusterv1beta1.JetStream{}, remotes("demo", "east"), true},
		{"only its own entry", &clusterv1beta1.JetStream{}, remotes("demo"), false},
		{"a domain", &clusterv1beta1.JetStream{Domain: "edge"}, remotes("demo", "east"), false},
		{"no jetstream", nil, remotes("demo", "east"), false},
		{"no gateway", &clusterv1beta1.JetStream{}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := &clusterv1beta1.NatsCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
			nc.Spec.JetStream, nc.Spec.Gateway = tt.jetstream, tt.gateway
			require.Equal(t, tt.want, joinsMetaGroup(nc))
		})
	}
}

func TestOwnMeta(t *testing.T) {
	servers := []sysobs.Server{{Name: "demo-0"}, {Name: "demo-1"}}
	snap := func(silent []string, groups ...sysobs.Group) *sysobs.Snapshot {
		return &sysobs.Snapshot{Servers: servers, Silent: silent, Groups: groups}
	}
	tests := []struct {
		name        string
		snap        *sysobs.Snapshot
		own, known  bool
		whyContains string
	}{
		{"not observed", nil, false, false, "not observed"},
		{"a moving server silent", snap([]string{"demo-1"}, sysobs.Group{Kind: sysobs.KindMeta, Leader: "demo-0"}), false, false, "demo-1 did not answer"},
		{"led here, no peer outside", snap(nil, sysobs.Group{Kind: sysobs.KindMeta, Leader: "demo-0"}), true, true, ""},
		{"led here, a peer outside", snap(nil, sysobs.Group{Kind: sysobs.KindMeta, Leader: "demo-0", Outside: []string{"east-0"}}), false, true, ""},
		{"led from another NATS cluster", snap(nil, sysobs.Group{Kind: sysobs.KindMeta, Leader: "east-0", FromFollowers: true}), false, true, ""},
		{"led here, its peers unread", snap(nil, sysobs.Group{Kind: sysobs.KindMeta, Leader: "demo-0", FromFollowers: true}), false, false, "demo-0 did not report"},
		{"leaderless", snap(nil, sysobs.Group{Kind: sysobs.KindMeta, NamedLeader: "demo-0"}), false, true, ""},
		{"no meta group", snap(nil), false, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			own, known, why := ownMeta(tt.snap, []string{"demo-0", "demo-1"})
			require.Equal(t, tt.own, own)
			require.Equal(t, tt.known, known)
			require.Contains(t, why, tt.whyContains)
		})
	}
}

// reconcileObserved reconciles nc once with observer and returns it as stored.
func reconcileObserved(t *testing.T, c client.Client, nc *clusterv1beta1.NatsCluster, observer Observer) *clusterv1beta1.NatsCluster {
	t.Helper()
	r := &Reconciler{Client: c, Observer: observer, AllowGatewayWithoutTLS: true}
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
	require.NoError(t, err)
	got := &clusterv1beta1.NatsCluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(nc), got))
	return got
}

// serverObjects maps every StatefulSet and ConfigMap in nc's namespace to its
// resource version.
func serverObjects(t *testing.T, c client.Client, nc *clusterv1beta1.NatsCluster) map[string]string {
	t.Helper()
	out := map[string]string{}
	var sets appsv1.StatefulSetList
	require.NoError(t, c.List(t.Context(), &sets, client.InNamespace(nc.Namespace)))
	for _, s := range sets.Items {
		out["StatefulSet/"+s.Name] = s.ResourceVersion
	}
	var cms corev1.ConfigMapList
	require.NoError(t, c.List(t.Context(), &cms, client.InNamespace(nc.Namespace)))
	for _, cm := range cms.Items {
		out["ConfigMap/"+cm.Name] = cm.ResourceVersion
	}
	return out
}

func requireProgressingCondition(t *testing.T, nc *clusterv1beta1.NatsCluster, status metav1.ConditionStatus, reason string, contains ...string) {
	t.Helper()
	c := meta.FindStatusCondition(nc.Status.Conditions, ConditionProgressing)
	require.NotNil(t, c)
	require.Equal(t, status, c.Status, c.Message)
	require.Equal(t, reason, c.Reason, c.Message)
	for _, s := range contains {
		require.Contains(t, c.Message, s)
	}
}

// TestReconcile_OwnMetaGroup pins that a NatsCluster joining a
// supercluster's meta group is held at its last render while its servers
// hold, or may hold, a meta group of their own, and proceeds once they share
// one with a server outside the NATS cluster.
func TestReconcile_OwnMetaGroup(t *testing.T) {
	addRemotes := func(t *testing.T, c client.Client, nc *clusterv1beta1.NatsCluster) {
		t.Helper()
		cur := &clusterv1beta1.NatsCluster{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(nc), cur))
		cur.Spec.Gateway = quickstartWithGateway(t).Spec.Gateway
		cur.Generation++
		require.NoError(t, c.Update(t.Context(), cur))
	}
	running := func(t *testing.T, snap func(*Plan) *sysobs.Snapshot) (client.Client, *clusterv1beta1.NatsCluster, *fakeObserver) {
		t.Helper()
		nc := storyCluster(t)
		nc.Generation = 1
		c := fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(nc).WithStatusSubresource(nc).Build()
		plan, err := Render(nc, Inputs{})
		require.NoError(t, err)
		obs := &fakeObserver{}
		obs.set(snap(plan))
		reconcileObserved(t, c, nc, obs)
		addRemotes(t, c, nc)
		return c, nc, obs
	}
	own := func(plan *Plan) *sysobs.Snapshot { return settledSnapshot(plan, plan.Revision, "demo-1") }

	t.Run("servers leading a meta group of their own", func(t *testing.T) {
		c, nc, obs := running(t, own)
		before := serverObjects(t, c, nc)
		got := reconcileObserved(t, c, nc, obs)
		requireProgressingCondition(t, got, metav1.ConditionFalse, ReasonOwnMetaGroup, "demo-0, demo-1, demo-2", "led by demo-1", "jetstream.domain")
		require.Equal(t, before, serverObjects(t, c, nc))
	})

	t.Run("servers in a meta group with peers outside the NATS cluster", func(t *testing.T) {
		c, nc, obs := running(t, func(plan *Plan) *sysobs.Snapshot {
			s := own(plan)
			s.Groups[0].Outside = []string{"east-0"}
			return s
		})
		got := reconcileObserved(t, c, nc, obs)
		requireProgressingCondition(t, got, metav1.ConditionTrue, ReasonRollingRestart, "before restarting")
	})

	t.Run("servers not observed", func(t *testing.T) {
		c, nc, _ := running(t, own)
		before := serverObjects(t, c, nc)
		got := reconcileObserved(t, c, nc, unobservable{})
		requireProgressingCondition(t, got, metav1.ConditionFalse, ReasonObservationFailed, "not observed", "no responders")
		require.Equal(t, before, serverObjects(t, c, nc))
	})

	t.Run("a change that keeps the remotes", func(t *testing.T) {
		nc := quickstartWithGateway(t)
		c := fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(nc).WithStatusSubresource(nc).Build()
		plan, err := Render(nc, Inputs{})
		require.NoError(t, err)
		obs := &fakeObserver{}
		obs.set(own(plan))
		reconcileObserved(t, c, nc, obs)
		cur := &clusterv1beta1.NatsCluster{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(nc), cur))
		cur.Spec.ServerTags = map[string]string{"zone": "a"}
		cur.Generation++
		require.NoError(t, c.Update(t.Context(), cur))
		got := reconcileObserved(t, c, nc, obs)
		c2 := meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing)
		require.NotEqual(t, ReasonOwnMetaGroup, c2.Reason, c2.Message)
	})
}

// TestReconcile_ClaimOlderThanNatsCluster pins that a NatsCluster joining a
// supercluster's meta group creates no server while a data volume claim of
// one is older than the NatsCluster, and creates them past a newer claim.
func TestReconcile_ClaimOlderThanNatsCluster(t *testing.T) {
	created := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	claim := func(at time.Time) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: dataClaimName("demo-2"), Namespace: "nats-system", CreationTimestamp: metav1.NewTime(at),
		}}
	}
	setup := func(t *testing.T, gateway bool, pvc *corev1.PersistentVolumeClaim) (client.Client, *clusterv1beta1.NatsCluster) {
		nc := quickstartWithGateway(t)
		if !gateway {
			nc.Spec.Gateway = nil
		}
		nc.CreationTimestamp = metav1.NewTime(created)
		c := fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(nc, pvc).WithStatusSubresource(nc).Build()
		return c, nc
	}
	sets := func(t *testing.T, c client.Client) int {
		var l appsv1.StatefulSetList
		require.NoError(t, c.List(t.Context(), &l))
		return len(l.Items)
	}

	t.Run("older", func(t *testing.T) {
		c, nc := setup(t, true, claim(created.Add(-time.Hour)))
		got := reconcileObserved(t, c, nc, unobservable{})
		requireProgressingCondition(t, got, metav1.ConditionFalse, ReasonOwnMetaGroup, "data-demo-2-0", "delete the claims")
		require.Zero(t, sets(t, c))
	})
	t.Run("newer", func(t *testing.T) {
		c, nc := setup(t, true, claim(created.Add(time.Second)))
		got := reconcileObserved(t, c, nc, unobservable{})
		requireProgressingCondition(t, got, metav1.ConditionTrue, ReasonCreating)
		require.Equal(t, 3, sets(t, c))
	})
	t.Run("older, no remotes", func(t *testing.T) {
		c, nc := setup(t, false, claim(created.Add(-time.Hour)))
		got := reconcileObserved(t, c, nc, unobservable{})
		requireProgressingCondition(t, got, metav1.ConditionTrue, ReasonCreating)
		require.Equal(t, 3, sets(t, c))
	})
}
