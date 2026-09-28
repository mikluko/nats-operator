package balancectl

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

func evacuation(name, conn string, tags ...string) *js.NatsClusterEvacuation {
	return &js.NatsClusterEvacuation{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID("uid-" + name)},
		Spec: js.NatsClusterEvacuationSpec{
			ConnectionRef: natsv1beta1.ObjectReference{Name: conn},
			From:          js.EvacuationSource{Cluster: "C1"},
			To:            js.EvacuationTarget{ServerTags: tags},
		},
	}
}

// restarted is r as a controller restart leaves it: its configuration alone.
func restarted(r *EvacuationReconciler) *EvacuationReconciler {
	return &EvacuationReconciler{Client: r.Client, Dialer: r.Dialer, MaxInFlight: r.MaxInFlight, PendingPoll: r.PendingPoll, Recorder: r.Recorder}
}

// evacuated reconciles the evacuation name, each time as after a restart,
// until its status satisfies want, and returns it.
func evacuated(t *testing.T, ctx context.Context, r *EvacuationReconciler, name string, want func(*assert.CollectT, *js.NatsClusterEvacuation), msg string) *js.NatsClusterEvacuation {
	t.Helper()
	var e js.NatsClusterEvacuation
	key := client.ObjectKey{Namespace: ns, Name: name}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		_, err := restarted(r).Reconcile(ctx, reconcile.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, r.Client.Get(ctx, key, &e))
		want(ct, &e)
	}, 2*time.Minute, 100*time.Millisecond, msg)
	return &e
}

func evacCondition(ct *assert.CollectT, e *js.NatsClusterEvacuation, typ string, status metav1.ConditionStatus, reason string) {
	c := meta.FindStatusCondition(e.Status.Conditions, typ)
	if assert.NotNil(ct, c, typ) {
		assert.Equal(ct, status, c.Status, "%s: %s", typ, c.Message)
		assert.Equal(ct, reason, c.Reason, "%s: %s", typ, c.Message)
	}
}

// deleteEvacuation deletes name and reconciles it until it is gone.
func deleteEvacuation(t *testing.T, ctx context.Context, r *EvacuationReconciler, name string) {
	t.Helper()
	key := client.ObjectKey{Namespace: ns, Name: name}
	var e js.NatsClusterEvacuation
	require.NoError(t, r.Client.Get(ctx, key, &e))
	require.NoError(t, r.Client.Delete(ctx, &e))
	_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	require.True(t, isGone(r.Client.Get(ctx, key, &e)), "the evacuation outlived its deletion")
}

func isGone(err error) bool { return client.IgnoreNotFound(err) == nil && err != nil }

// placedIn is the NATS cluster stream sits in once no move is in progress,
// "" while one is: every copy and the leader in one cluster, no desired
// state.
func placedIn(ctx context.Context, j jetstream.JetStream, stream string) string {
	s, err := j.Stream(ctx, stream)
	if err != nil {
		return ""
	}
	c := s.CachedInfo().Cluster
	if c == nil || c.Leader == "" || c.Desired != nil {
		return ""
	}
	cluster, _, _ := strings.Cut(c.Leader, "-")
	for _, p := range c.Replicas {
		if !strings.HasPrefix(p.Name, cluster+"-") {
			return ""
		}
	}
	return cluster
}

// consumerIn is the NATS cluster consumer's leader and replicas sit in, ""
// while they span two.
func consumerIn(ctx context.Context, j jetstream.JetStream, stream, consumer string) string {
	c, err := j.Consumer(ctx, stream, consumer)
	if err != nil {
		return ""
	}
	info := c.CachedInfo().Cluster
	if info == nil || info.Leader == "" {
		return ""
	}
	cluster, _, _ := strings.Cut(info.Leader, "-")
	for _, p := range info.Replicas {
		if !strings.HasPrefix(p.Name, cluster+"-") {
			return ""
		}
	}
	return cluster
}

// TestEvacuation_Supercluster empties C1 of a two-cluster operator-mode
// supercluster into C2 while a system balancer and an account balancer of C1
// run beside the evacuation.
func TestEvacuation_Supercluster(t *testing.T) {
	t.Parallel()
	p := newPlane(t)
	sc := startTaggedSupercluster(t, p, map[string][]string{"C1": {"old"}, "C2": {"new"}}, "C1", "C2")
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	jsA := accountJS(t, sc["C1"][0], p.a)
	jsB := accountJS(t, sc["C1"][1], p.b)

	create := func(j jetstream.JetStream, cfg jetstream.StreamConfig) {
		t.Helper()
		require.Eventually(t, func() bool { _, err := j.CreateStream(ctx, cfg); return err == nil }, 30*time.Second, 200*time.Millisecond, "create %s", cfg.Name)
	}
	create(jsA, jetstream.StreamConfig{Name: "PLAIN", Subjects: []string{"plain.>"}, Replicas: 3})
	_, err := jsA.CreateOrUpdateConsumer(ctx, "PLAIN", jetstream.ConsumerConfig{Durable: "D", AckPolicy: jetstream.AckExplicitPolicy})
	require.NoError(t, err)
	_, err = jsA.Publish(ctx, "plain.x", []byte("kept"))
	require.NoError(t, err)
	create(jsA, jetstream.StreamConfig{Name: "STALE", Subjects: []string{"stale.>"}, Replicas: 3, Placement: &jetstream.Placement{Cluster: "C1"}})
	pinnedCfg := jetstream.StreamConfig{
		Name: "PINNED", Subjects: []string{"pinned.>"}, Replicas: 3,
		Placement: &jetstream.Placement{Cluster: "C1"},
		Metadata:  map[string]string{lifecycle.OwnerKey: "uid-orders"},
	}
	create(jsA, pinnedCfg)
	pinnedNames := []string{"PINNED", "PINNED_1", "PINNED_2"}
	for i, name := range pinnedNames[1:] {
		cfg := pinnedCfg
		cfg.Name, cfg.Subjects = name, []string{strings.ToLower(name) + ".>"}
		cfg.Metadata = map[string]string{lifecycle.OwnerKey: fmt.Sprintf("uid-orders-%d", i+1)}
		create(jsA, cfg)
	}
	create(jsB, jetstream.StreamConfig{Name: "LOG", Subjects: []string{"log.>"}, Replicas: 1})
	_, err = jsB.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "sessions", Replicas: 3})
	require.NoError(t, err)
	_, err = jsB.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "blobs"})
	require.NoError(t, err)
	movable := map[jetstream.JetStream][]string{jsA: {"PLAIN", "STALE"}, jsB: {"LOG", "KV_sessions", "OBJ_blobs"}}
	everywhere := func(want string) func(*assert.CollectT) {
		return func(ct *assert.CollectT) {
			for j, names := range movable {
				for _, name := range names {
					assert.Equal(ct, want, placedIn(ctx, j, name), name)
				}
			}
		}
	}
	placedAll := func(want string) bool {
		for j, names := range movable {
			for _, name := range names {
				if placedIn(ctx, j, name) != want {
					return false
				}
			}
		}
		return true
	}
	require.EventuallyWithT(t, everywhere("C1"), time.Minute, 200*time.Millisecond, "the streams did not settle in C1")

	owner := func(name, uid, stream string) *js.NatsStream {
		return &js.NatsStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: "orders", Name: name, UID: types.UID(uid)},
			Spec:       js.NatsStreamSpec{StreamConfig: js.StreamConfig{Name: stream, Placement: &js.Placement{Cluster: "C1"}}},
		}
	}
	owners := []*js.NatsStream{
		owner("orders", "uid-orders", "PINNED"),
		owner("orders-1", "uid-orders-1", "PINNED_1"),
		owner("orders-2", "uid-orders-2", "PINNED_2"),
	}
	ghost := owner("ghost", "uid-ghost", "GHOST")
	objs := append(connection("c1", sc["C1"][2].ClientURL(), p.sysCreds), connection("c2", sc["C2"][0].ClientURL(), p.sysCreds)...)
	objs = append(objs, connection("a", sc["C1"][0].ClientURL(), creds(t, jwtplane.User{}, p.a))...)
	objs = append(objs, ghost, balancer("demo", "c1", time.Now()), accountBalancer("payments", "a"))
	for _, o := range owners {
		objs = append(objs, o)
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithStatusSubresource(&js.NatsSystemBalancer{}, &js.NatsBalancer{}, &js.NatsClusterEvacuation{}).
		WithObjects(objs...).
		Build()
	pool := NewPool()
	t.Cleanup(pool.Close)
	dialer := &natsconn.Dialer{Reader: c, Pool: pool}
	rec := events.NewFakeRecorder(1000)
	r := &EvacuationReconciler{Client: c, Dialer: dialer, MaxInFlight: 2, PendingPoll: time.Millisecond, Recorder: rec}
	leases := &MoveLeases{}
	br := &SystemBalancerReconciler{Client: c, Dialer: dialer, PendingPoll: time.Millisecond, Leases: leases}
	ar := &BalancerReconciler{Client: c, Dialer: dialer, PendingPoll: time.Millisecond, Leases: leases}

	t.Run("RefusesTargetTagsInSource", func(t *testing.T) {
		require.NoError(t, c.Create(ctx, evacuation("wrong", "c2", "old")))
		e := evacuated(t, ctx, r, "wrong", func(ct *assert.CollectT, e *js.NatsClusterEvacuation) {
			evacCondition(ct, e, ConditionReady, metav1.ConditionFalse, ReasonTargetTagsInSource)
			evacCondition(ct, e, ConditionProgressing, metav1.ConditionFalse, ReasonTargetTagsInSource)
		}, "a target tag carried by C1 was not refused")
		require.Equal(t, "server C1-0 of C1 carries old", meta.FindStatusCondition(e.Status.Conditions, ConditionReady).Message)
		require.Zero(t, e.Status.Moved)
		require.Zero(t, e.Status.InFlight)
		require.Zero(t, e.Status.Remaining)
		require.Equal(t, []string{"server C1-0 of C1 carries old"}, notes(recorded(rec), "Warning", "EvacuationRefused"))
		require.Contains(t, e.Finalizers, lifecycle.Finalizer)
		require.Never(t, func() bool { return !placedAll("C1") }, 2*time.Second, 100*time.Millisecond, "a refused evacuation moved a stream")
		deleteEvacuation(t, ctx, r, "wrong")
	})

	t.Run("EmptiesC1ButThePinned", func(t *testing.T) {
		require.NoError(t, c.Create(ctx, evacuation("retire-c1", "c2", "new")))
		sawInFlight := false
		e := evacuated(t, ctx, r, "retire-c1", func(ct *assert.CollectT, e *js.NatsClusterEvacuation) {
			if e.Status.InFlight > 0 {
				sawInFlight = true
				assert.LessOrEqual(ct, e.Status.InFlight, int32(2), "MaxInFlight")
				evacCondition(ct, e, ConditionProgressing, metav1.ConditionTrue, ReasonMoving)
			}
			evacCondition(ct, e, ConditionReady, metav1.ConditionFalse, ReasonPinnedObjects)
			evacCondition(ct, e, ConditionProgressing, metav1.ConditionFalse, ReasonNothingMovable)
		}, "C1 was not emptied of its movable streams")
		require.True(t, sawInFlight, "no move was ever seen in flight")
		require.Equal(t, int32(5), e.Status.Moved)
		require.Zero(t, e.Status.InFlight)
		require.Zero(t, e.Status.Remaining)
		moved := []string{p.aPub + "/PLAIN", p.aPub + "/STALE", p.bPub + "/LOG", p.bPub + "/KV_sessions", p.bPub + "/OBJ_blobs"}
		evs := recorded(rec)
		var started, done []string
		for _, id := range moved {
			started = append(started, id+" off C1 to servers tagged new")
			done = append(done, id+" left C1")
		}
		require.Subset(t, notes(evs, "Normal", "MoveStarted"), started)
		require.ElementsMatch(t, done, notes(evs, "Normal", "MoveDone"))
		require.Empty(t, e.Status.Requested)
		require.Equal(t, "3 resources pin placement.cluster C1", meta.FindStatusCondition(e.Status.Conditions, ConditionReady).Message)
		require.Equal(t, []js.PinnedObject{
			{Kind: "NatsStream", Namespace: "orders", Name: "orders"},
			{Kind: "NatsStream", Namespace: "orders", Name: "orders-1"},
			{Kind: "NatsStream", Namespace: "orders", Name: "orders-2"},
		}, e.Status.Pinned, "ghost's stream is not in C1")
		require.Equal(t, []js.ServerStream{{Account: p.aPub, Name: "STALE"}}, e.Status.StalePlacement)

		require.EventuallyWithT(t, everywhere("C2"), time.Minute, 200*time.Millisecond, "a moved stream did not settle in C2")
		for _, name := range pinnedNames {
			require.Equal(t, "C1", placedIn(ctx, jsA, name), "a pinned stream was moved")
		}
		require.Eventually(t, func() bool { return consumerIn(ctx, jsA, "PLAIN", "D") == "C2" }, time.Minute, 200*time.Millisecond, "the consumer did not follow its stream")
		plain, err := jsA.Stream(ctx, "PLAIN")
		require.NoError(t, err)
		msg, err := plain.GetLastMsgForSubject(ctx, "plain.x")
		require.NoError(t, err)
		require.Equal(t, "kept", string(msg.Data))

	})

	t.Run("BalancersBalanceWhatThePinnedEvacuationLeaves", func(t *testing.T) {
		for _, name := range pinnedNames {
			stepToFirst(t, ctx, jsA, name)
		}
		reconciled(t, ctx, br, "demo", func(ct *assert.CollectT, b *js.NatsSystemBalancer) {
			condition(ct, b, ConditionHolding, metav1.ConditionFalse, ReasonSettled)
			assert.Empty(ct, b.Status.Pending)
			if assert.NotNil(ct, b.Status.Skew) {
				assert.Zero(ct, b.Status.Skew.Leaders, "leaders %v", b.Status.Servers)
			}
		}, "the system balancer did not even C1's leaders beside a pinned evacuation")
		leaders := map[string]bool{}
		for _, name := range pinnedNames {
			leaders[streamLeader(t, ctx, jsA, name)] = true
		}
		require.Len(t, leaders, 3, "the pinned streams are led by one server each")

		for _, name := range pinnedNames {
			stepToFirst(t, ctx, jsA, name)
		}
		reconciledAccount(t, ctx, ar, func(ct *assert.CollectT, b *js.NatsBalancer) {
			accountCondition(ct, b, ConditionHolding, metav1.ConditionFalse, ReasonSettled)
			if assert.Len(ct, b.Status.Pools, 1) {
				assert.Equal(ct, int32(3), b.Status.Pools[0].Streams)
				assert.Zero(ct, b.Status.Pools[0].LeaderSkew)
			}
		}, "the account balancer did not even C1's leaders beside a pinned evacuation")

		var e js.NatsClusterEvacuation
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "retire-c1"}, &e))
		require.False(t, meta.IsStatusConditionTrue(e.Status.Conditions, ConditionReady), "the evacuation became Ready")
	})

	t.Run("ReadyOnceTheOwnerMoves", func(t *testing.T) {
		for i, own := range owners {
			var o js.NatsStream
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(own), &o))
			o.Spec.Placement.Cluster = "C2"
			require.NoError(t, c.Update(ctx, &o))
			s, err := jsA.Stream(ctx, pinnedNames[i])
			require.NoError(t, err)
			cfg := s.CachedInfo().Config
			cfg.Placement = &jetstream.Placement{Cluster: "C2"}
			_, err = jsA.UpdateStream(ctx, cfg)
			require.NoError(t, err)
		}

		e := evacuated(t, ctx, r, "retire-c1", func(ct *assert.CollectT, e *js.NatsClusterEvacuation) {
			evacCondition(ct, e, ConditionReady, metav1.ConditionTrue, ReasonEvacuated)
			evacCondition(ct, e, ConditionProgressing, metav1.ConditionFalse, ReasonEvacuated)
		}, "the evacuation did not become Ready once the owner moved its stream")
		require.Empty(t, e.Status.Pinned)
		require.Equal(t, int32(5), e.Status.Moved, "an owner's move is not the evacuation's")
		for _, name := range pinnedNames {
			require.Eventually(t, func() bool { return placedIn(ctx, jsA, name) == "C2" }, time.Minute, 100*time.Millisecond, "the owner's move of %s did not settle", name)
		}
		reconciled(t, ctx, br, "demo", func(ct *assert.CollectT, b *js.NatsSystemBalancer) {
			condition(ct, b, ConditionHolding, metav1.ConditionFalse, ReasonSettled)
		}, "the system balancer did not resume once the evacuation was Ready")

		deleteEvacuation(t, ctx, r, "retire-c1")
		require.Never(t, func() bool { return !placedAll("C2") }, 2*time.Second, 100*time.Millisecond, "deleting a Ready evacuation moved a stream back")
	})

	t.Run("DeletionCancelsPendingMoves", func(t *testing.T) {
		const tries = 5
		for i := range tries {
			name := fmt.Sprintf("LATE_%d", i)
			create(jsA, jetstream.StreamConfig{Name: name, Subjects: []string{strings.ToLower(name) + ".>"}, Replicas: 3})
			for range 200 {
				_, err := jsA.Publish(ctx, strings.ToLower(name)+".x", make([]byte, 1024))
				require.NoError(t, err)
			}
			require.Eventually(t, func() bool { return placedIn(ctx, jsA, name) == "C1" }, time.Minute, 100*time.Millisecond)

			recorded(rec)
			evac := fmt.Sprintf("interrupted-%d", i)
			require.NoError(t, c.Create(ctx, evacuation(evac, "c2", "new")))
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: evac}})
			require.NoError(t, err)
			var e js.NatsClusterEvacuation
			require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: ns, Name: evac}, &e))
			require.Equal(t, int32(1), e.Status.InFlight, "the move was not requested")
			require.Equal(t, []string{name}, requestedStreams(e.Status.Requested))
			deleteEvacuation(t, ctx, restarted(r), evac)

			var landed string
			require.Eventually(t, func() bool { landed = placedIn(ctx, jsA, name); return landed != "" }, time.Minute, 100*time.Millisecond, "%s never settled", name)
			if landed == "C1" {
				require.Contains(t, notes(recorded(rec), "Normal", "MoveCancelled"), fmt.Sprintf("move of %s/%s off C1 cancelled", p.aPub, name))
				s, err := jsA.Stream(ctx, name)
				require.NoError(t, err)
				require.Equal(t, uint64(200), s.CachedInfo().State.Msgs)
				return
			}
			t.Logf("%s landed in C2 before the cancel reached it; trying again", name)
		}
		t.Fatalf("no move was still in flight when its evacuation was deleted, in %d tries", tries)
	})
}

func requestedStreams(list []js.RequestedMove) []string {
	var out []string
	for _, m := range list {
		out = append(out, m.Stream)
	}
	return out
}

// TestEvacuation_ServerDown evacuates C1 while the server holding an R1
// stream of it is stopped: the stream is in no snapshot of C1, and the
// evacuation holds rather than reading C1 as empty.
func TestEvacuation_ServerDown(t *testing.T) {
	t.Parallel()
	p := newPlane(t)
	sc := startTaggedSupercluster(t, p, map[string][]string{"C1": {"old"}, "C2": {"new"}}, "C1", "C2")
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	jsB := accountJS(t, sc["C1"][0], p.b)
	require.Eventually(t, func() bool {
		_, err := jsB.CreateStream(ctx, jetstream.StreamConfig{Name: "LOG", Subjects: []string{"log.>"}, Replicas: 1})
		return err == nil
	}, 30*time.Second, 200*time.Millisecond, "create LOG")
	require.Eventually(t, func() bool { return placedIn(ctx, jsB, "LOG") == "C1" }, time.Minute, 100*time.Millisecond, "LOG did not settle in C1")
	host := streamLeader(t, ctx, jsB, "LOG")
	for _, srv := range sc["C1"] {
		if srv.Name() == host {
			srv.Shutdown()
		}
	}

	objs := append(connection("sys", sc["C2"][0].ClientURL(), p.sysCreds), evacuation("retire-c1", "sys", "new"))
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&js.NatsClusterEvacuation{}).WithObjects(objs...).Build()
	pool := NewPool()
	t.Cleanup(pool.Close)
	r := &EvacuationReconciler{Client: c, Dialer: &natsconn.Dialer{Reader: c, Pool: pool}, PendingPoll: time.Millisecond}
	key := client.ObjectKey{Namespace: ns, Name: "retire-c1"}
	var e js.NatsClusterEvacuation
	pass := func() {
		t.Helper()
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, c.Get(ctx, key, &e))
		require.False(t, meta.IsStatusConditionTrue(e.Status.Conditions, ConditionReady), "C1 read as evacuated with %s down", host)
	}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		pass()
		evacCondition(ct, &e, ConditionReady, metav1.ConditionFalse, ReasonServersDown)
		evacCondition(ct, &e, ConditionProgressing, metav1.ConditionFalse, ReasonServersDown)
	}, 30*time.Second, 100*time.Millisecond, "the evacuation did not hold for %s", host)
	require.Equal(t, fmt.Sprintf("server %s is offline", host), meta.FindStatusCondition(e.Status.Conditions, ConditionReady).Message)
	for range 5 {
		pass()
	}
	require.Zero(t, e.Status.Moved)
	require.Empty(t, e.Status.Requested)
}
