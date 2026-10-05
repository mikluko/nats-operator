package natscluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/natstest"
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

// TestJetStreamEnabledAfterGateways_Live pins why enabling JetStream is
// restart-only on nats-server 2.15.0: west's servers, started without it,
// join east's meta group with east's stream intact whether a restart or a
// reload enables it, but only after a restart can a stream be placed on west.
func TestJetStreamEnabledAfterGateways_Live(t *testing.T) {
	t.Run("enabled by restart", func(t *testing.T) { joinEast(t, false) })
	t.Run("enabled by reload", func(t *testing.T) { joinEast(t, true) })
}

// joinEast boots story 6's east and west, west without JetStream until its
// gateways connect and then with it, by reload when reload is set and by
// restart otherwise, and requires west's servers in east's meta group,
// east's stream intact, and a stream placed on west created after a restart
// and refused after a reload.
func joinEast(t *testing.T, reload bool) {
	east, west := supercluster(t, func(_, west *clusterv1beta1.NatsCluster) { west.Spec.JetStream = nil })
	west.eventuallyStatus(t, func(st clusterv1beta1.NatsClusterStatus) bool {
		return meta.IsStatusConditionTrue(st.Conditions, ConditionGatewaysConnected)
	})

	p := east.plane
	account := newTestKeys(t, nkeys.PrefixByteAccount)
	accJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: "orders", Keys: account,
		Limits: jwtplane.Limits{JetStream: &jwtplane.JetStreamLimits{}}}, p.op, time.Now())
	require.NoError(t, err)
	admin, err := natsconn.Dial(natsconn.Endpoint{Servers: []string{east.url}, Creds: p.systemCreds(t, jwtplane.PresetAuthController)}, nats.CustomInboxPrefix(jwtplane.InboxPrefix(jwtplane.PresetAuthController)))
	require.NoError(t, err)
	t.Cleanup(admin.Close)
	_, err = admin.Request("$SYS.REQ.CLAIMS.UPDATE", []byte(accJWT), 2*time.Second)
	require.NoError(t, err)
	userCreds := p.creds(t, account, jwtplane.User{Name: "orders"})
	jsAt := func(url string) nats.JetStreamContext {
		var conn *nats.Conn
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			var err error
			conn, err = natsconn.Dial(natsconn.Endpoint{Servers: []string{url}, Creds: userCreds})
			require.NoError(c, err)
		}, 15*time.Second, 200*time.Millisecond, "orders user not admitted at %s", url)
		t.Cleanup(conn.Close)
		js, err := conn.JetStream()
		require.NoError(t, err)
		return js
	}
	eastJS := jsAt(east.url)
	require.Eventually(t, func() bool {
		_, err := eastJS.AddStream(&nats.StreamConfig{Name: "OLD", Subjects: []string{"old.>"}, Replicas: 3, Placement: &nats.Placement{Cluster: "east"}})
		return err == nil
	}, 30*time.Second, 200*time.Millisecond, "stream OLD not created on east")
	for i := range 10 {
		_, err := eastJS.Publish(fmt.Sprintf("old.%d", i), []byte("m"))
		require.NoError(t, err)
	}

	westJS := jsAt(west.url)
	withJS := west.nc.DeepCopy()
	withJS.Spec.JetStream = &clusterv1beta1.JetStream{Limits: &clusterv1beta1.JetStreamLimits{MaxMemoryStore: quantity("256Mi"), MaxFileStore: quantity("1Gi")}}
	for i, file := range west.files {
		before, err := os.ReadFile(file)
		require.NoError(t, err)
		var cfg map[string]any
		require.NoError(t, json.Unmarshal(before, &cfg))
		block := serverConfig(withJS, Inputs{}, west.srvs[i].Name(), Layout{StoreDir: filepath.Join(t.TempDir(), "jetstream")}, "").JetStream
		b, err := json.Marshal(block)
		require.NoError(t, err)
		var js map[string]any
		require.NoError(t, json.Unmarshal(b, &js))
		cfg["jetstream"] = js
		after, err := json.Marshal(cfg)
		require.NoError(t, err)
		require.Equal(t, "jetstream is restart-only", restartReason("2.15.0", before, after))
		if !reload {
			west.srvs[i].Shutdown()
			west.srvs[i].WaitForShutdown()
		}
		require.NoError(t, os.WriteFile(file, after, 0o600))
		if reload {
			require.NoError(t, west.srvs[i].Reload())
		} else {
			west.srvs[i] = natstest.Start(t, file).Server
		}
	}

	all := append(slices.Clone(east.srvs), west.srvs...)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		leaders := map[string]bool{}
		for _, s := range all {
			require.True(c, s.JetStreamEnabled(), s.Name())
			g, ok := metaRaftz(s)
			require.True(c, ok, "%s reports no meta group", s.Name())
			require.Equal(c, len(all), g.Size, s.Name())
			require.Len(c, g.Peers, len(all)-1, s.Name())
			leaders[g.Leader] = true
		}
		require.Len(c, leaders, 1)
	}, 60*time.Second, 250*time.Millisecond, "west did not join east's meta group")

	info, err := westJS.StreamInfo("OLD")
	require.NoError(t, err)
	require.Equal(t, uint64(10), info.State.Msgs)
	require.Equal(t, "east", info.Cluster.Name)
	add := func() error {
		_, err := westJS.AddStream(&nats.StreamConfig{Name: "NEW", Subjects: []string{"new.>"}, Replicas: 3, Placement: &nats.Placement{Cluster: "west"}})
		return err
	}
	if reload {
		require.Never(t, func() bool { return add() == nil }, 10*time.Second, 500*time.Millisecond, "stream NEW created on west after a reload")
		require.Error(t, add())
		return
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) { require.NoError(c, add()) }, 30*time.Second, 200*time.Millisecond, "stream NEW not created on west")
}

// metaRaftz is s's view of its JetStream meta group.
func metaRaftz(s *server.Server) (server.RaftzGroup, bool) {
	rz := s.Raftz(&server.RaftzOptions{})
	if rz == nil {
		return server.RaftzGroup{}, false
	}
	for _, groups := range *rz {
		if g, ok := groups["_meta_"]; ok {
			return g, true
		}
	}
	return server.RaftzGroup{}, false
}

// endpointObserver observes the in-process servers at eps over their
// monitoring ports.
type endpointObserver struct {
	unobservable
	eps []sysobs.Endpoint
}

func (o endpointObserver) Observe(ctx context.Context, _ *clusterv1beta1.NatsCluster) (*sysobs.Snapshot, error) {
	return sysobs.NewMonitor(http.DefaultClient, 0).Observe(ctx, o.eps)
}

// TestMetaJoinHold_Live pins the hold against nats-server 2.15.0: story 1's
// servers, running JetStream with no gateway, lead a meta group of their
// own, and a reconcile adding gateway remotes holds them; story 6's west,
// in a meta group with east, is not judged own.
func TestMetaJoinHold_Live(t *testing.T) {
	t.Run("standalone", func(t *testing.T) {
		nc := storyCluster(t)
		nc.Generation = 1
		nc.Spec.JetStream.Limits = &clusterv1beta1.JetStreamLimits{MaxMemoryStore: quantity("256Mi"), MaxFileStore: quantity("1Gi")}
		eps, _, _, _ := startRendered(t, nc, nil, "r1")
		obs := endpointObserver{eps: eps}
		require.Eventually(t, func() bool {
			s, err := obs.Observe(t.Context(), nc)
			return err == nil && s.MetaLeader() != ""
		}, 30*time.Second, 200*time.Millisecond, "no meta leader")

		c := fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(nc).WithStatusSubresource(nc).Build()
		reconcileObserved(t, c, nc, obs)
		cur := &clusterv1beta1.NatsCluster{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(nc), cur))
		cur.Spec.Gateway = quickstartWithGateway(t).Spec.Gateway
		cur.Generation++
		require.NoError(t, c.Update(t.Context(), cur))
		before := serverObjects(t, c, nc)
		got := reconcileObserved(t, c, nc, obs)
		requireProgressingCondition(t, got, metav1.ConditionFalse, ReasonOwnMetaGroup, "demo-0, demo-1, demo-2")
		require.Equal(t, before, serverObjects(t, c, nc))
	})

	t.Run("supercluster member", func(t *testing.T) {
		east, west := supercluster(t, func(_, _ *clusterv1beta1.NatsCluster) {})
		west.eventuallyStatus(t, gatewaysConnected)
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			var outside []string
			for _, m := range []*member{east, west} {
				snap, err := m.sys.Observe(t.Context(), m.nc)
				require.NoError(c, err)
				own, known, why := ownMeta(snap, serverNames(m.nc))
				require.True(c, known, "%s: %s", m.nc.Name, why)
				require.False(c, own, "%s: %+v", m.nc.Name, snap.Groups)
				outside = append(outside, snap.Groups[0].Outside...)
			}
			require.NotEmpty(c, outside, "the leader reports no peer of the other member")
		}, 60*time.Second, 250*time.Millisecond)
	})
}
