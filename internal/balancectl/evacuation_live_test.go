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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
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

// evacuated reconciles the evacuation name until its status satisfies want,
// and returns it.
func evacuated(t *testing.T, ctx context.Context, r *EvacuationReconciler, name string, want func(*assert.CollectT, *js.NatsClusterEvacuation), msg string) *js.NatsClusterEvacuation {
	t.Helper()
	var e js.NatsClusterEvacuation
	key := client.ObjectKey{Namespace: ns, Name: name}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
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
// supercluster into C2, whose servers alone carry the tag "new", over two
// accounts: plain streams, a stream whose config names C1 without a
// resource, a consumer, a key-value bucket, an object store, and a stream
// whose NatsStream pins C1. The evacuation observes C1 through a connection
// to C2, and a system balancer of C1 runs beside it.
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

	orders := &js.NatsStream{
		ObjectMeta: metav1.ObjectMeta{Namespace: "orders", Name: "orders", UID: "uid-orders"},
		Spec:       js.NatsStreamSpec{StreamConfig: js.StreamConfig{Name: "PINNED", Placement: &js.Placement{Cluster: "C1"}}},
	}
	objs := append(connection("c1", sc["C1"][2].ClientURL(), p.sysCreds), connection("c2", sc["C2"][0].ClientURL(), p.sysCreds)...)
	objs = append(objs, orders, balancer("demo", "c1", time.Now()))
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithStatusSubresource(&js.NatsSystemBalancer{}, &js.NatsClusterEvacuation{}).
		WithObjects(objs...).
		Build()
	pool := natsconn.NewPool()
	t.Cleanup(pool.Close)
	dialer := &natsconn.Dialer{Reader: c, Pool: pool}
	r := &EvacuationReconciler{Client: c, Dialer: dialer, MaxInFlight: 2, PendingPoll: time.Millisecond}
	br := &SystemBalancerReconciler{Client: c, Dialer: dialer, PendingPoll: time.Millisecond}

	t.Run("RefusesTargetTagsInSource", func(t *testing.T) {
		require.NoError(t, c.Create(ctx, evacuation("wrong", "c2", "old")))
		e := evacuated(t, ctx, r, "wrong", func(ct *assert.CollectT, e *js.NatsClusterEvacuation) {
			evacCondition(ct, e, ConditionReady, metav1.ConditionFalse, ReasonTargetTagsInSource)
			evacCondition(ct, e, ConditionProgressing, metav1.ConditionFalse, ReasonTargetTagsInSource)
		}, "a target tag carried by C1 was not refused")
		require.Equal(t, "server C1-0 of C1 carries old", meta.FindStatusCondition(e.Status.Conditions, ConditionReady).Message)
		require.Zero(t, e.Status.Moved)
		require.Zero(t, e.Status.InFlight)
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
		require.Equal(t, "1 resource pins placement.cluster C1", meta.FindStatusCondition(e.Status.Conditions, ConditionReady).Message)
		require.Equal(t, []js.PinnedObject{{Kind: "NatsStream", Namespace: "orders", Name: "orders"}}, e.Status.Pinned)
		require.Equal(t, []js.ServerStream{{Account: p.aPub, Name: "STALE"}}, e.Status.StalePlacement)

		require.EventuallyWithT(t, everywhere("C2"), time.Minute, 200*time.Millisecond, "a moved stream did not settle in C2")
		require.Equal(t, "C1", placedIn(ctx, jsA, "PINNED"), "a pinned stream was moved")
		require.Eventually(t, func() bool { return consumerIn(ctx, jsA, "PLAIN", "D") == "C2" }, time.Minute, 200*time.Millisecond, "the consumer did not follow its stream")
		plain, err := jsA.Stream(ctx, "PLAIN")
		require.NoError(t, err)
		msg, err := plain.GetLastMsgForSubject(ctx, "plain.x")
		require.NoError(t, err)
		require.Equal(t, "kept", string(msg.Data))

		b := reconciled(t, ctx, br, "demo", func(ct *assert.CollectT, b *js.NatsSystemBalancer) {
			condition(ct, b, ConditionHolding, metav1.ConditionTrue, ReasonEvacuating)
		}, "the system balancer of C1 did not hold")
		require.Equal(t, "NatsClusterEvacuation nats-system/retire-c1 empties NATS cluster C1", meta.FindStatusCondition(b.Status.Conditions, ConditionHolding).Message)
	})

	t.Run("ReadyOnceTheOwnerMoves", func(t *testing.T) {
		var o js.NatsStream
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(orders), &o))
		o.Spec.Placement.Cluster = "C2"
		require.NoError(t, c.Update(ctx, &o))
		pinnedCfg.Placement = &jetstream.Placement{Cluster: "C2"}
		_, err := jsA.UpdateStream(ctx, pinnedCfg)
		require.NoError(t, err)

		e := evacuated(t, ctx, r, "retire-c1", func(ct *assert.CollectT, e *js.NatsClusterEvacuation) {
			evacCondition(ct, e, ConditionReady, metav1.ConditionTrue, ReasonEvacuated)
			evacCondition(ct, e, ConditionProgressing, metav1.ConditionFalse, ReasonEvacuated)
		}, "the evacuation did not become Ready once the owner moved its stream")
		require.Empty(t, e.Status.Pinned)
		require.Equal(t, int32(5), e.Status.Moved, "an owner's move is not the evacuation's")
		require.Eventually(t, func() bool { return placedIn(ctx, jsA, "PINNED") == "C2" }, time.Minute, 100*time.Millisecond, "the owner's move did not settle")
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

			evac := fmt.Sprintf("interrupted-%d", i)
			require.NoError(t, c.Create(ctx, evacuation(evac, "c2", "new")))
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: evac}})
			require.NoError(t, err)
			var e js.NatsClusterEvacuation
			require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: ns, Name: evac}, &e))
			require.Equal(t, int32(1), e.Status.InFlight, "the move was not requested")
			deleteEvacuation(t, ctx, r, evac)

			var landed string
			require.Eventually(t, func() bool { landed = placedIn(ctx, jsA, name); return landed != "" }, time.Minute, 100*time.Millisecond, "%s never settled", name)
			if landed == "C1" {
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
