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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

const ns = "nats-system"

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, natsv1beta1.AddToScheme(s))
	require.NoError(t, js.AddToScheme(s))
	return s
}

// connection is a NatsConnection to url whose credentials are creds, and
// the Secret holding them.
func connection(name, url string, creds []byte) []client.Object {
	return []client.Object{
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Data: map[string][]byte{natsconn.DefaultCredentialsKey: creds}},
		&natsv1beta1.NatsConnection{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: natsv1beta1.NatsConnectionSpec{
				Servers:     []string{url},
				Credentials: &natsv1beta1.Credentials{SecretKeyRef: natsv1beta1.CredentialsSecretKeySelector{Name: name}},
			},
		},
	}
}

func balancer(name, conn string, created time.Time) *js.NatsSystemBalancer {
	return &js.NatsSystemBalancer{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, CreationTimestamp: metav1.NewTime(created), UID: types.UID("uid-" + name)},
		Spec: js.NatsSystemBalancerSpec{
			ConnectionRef: natsv1beta1.ObjectReference{Name: conn},
			Interval:      &metav1.Duration{Duration: 10 * time.Millisecond},
		},
	}
}

// reconciled reconciles name until its status satisfies want, and returns it.
func reconciled(t *testing.T, ctx context.Context, r *SystemBalancerReconciler, name string, want func(*assert.CollectT, *js.NatsSystemBalancer), msg string) *js.NatsSystemBalancer {
	t.Helper()
	var b js.NatsSystemBalancer
	key := client.ObjectKey{Namespace: ns, Name: name}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, r.Client.Get(ctx, key, &b))
		want(ct, &b)
	}, 2*time.Minute, 50*time.Millisecond, msg)
	return &b
}

func condition(ct *assert.CollectT, b *js.NatsSystemBalancer, typ string, status metav1.ConditionStatus, reason string) {
	c := meta.FindStatusCondition(b.Status.Conditions, typ)
	if assert.NotNil(ct, c, typ) {
		assert.Equal(ct, status, c.Status, "%s: %s", typ, c.Message)
		assert.Equal(ct, reason, c.Reason, "%s: %s", typ, c.Message)
	}
}

// TestSystemBalancer_Supercluster runs the reconciler against a two-cluster
// supercluster in operator mode, where account A carries the
// jetstream-stepdown export and B does not, and every stream leader in C1
// starts on C1-0.
func TestSystemBalancer_Supercluster(t *testing.T) {
	t.Parallel()
	p := newPlane(t)
	sc := startSupercluster(t, p, "C1", "C2")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	jsA := accountJS(t, sc["C1"][1], p.a)
	jsB := accountJS(t, sc["C1"][2], p.b)
	skewedStreams(t, ctx, jsA, "A", 6)
	bStreams := skewedStreams(t, ctx, jsB, "B", 3)

	now := time.Now()
	objs := append(connection("c1", sc["C1"][1].ClientURL(), p.sysCreds), connection("c2", sc["C2"][0].ClientURL(), p.sysCreds)...)
	objs = append(objs, balancer("demo", "c1", now))
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithStatusSubresource(&js.NatsSystemBalancer{}).
		WithObjects(objs...).
		Build()
	pool := natsconn.NewPool()
	t.Cleanup(pool.Close)
	rec := events.NewFakeRecorder(1000)
	r := &SystemBalancerReconciler{Client: c, Dialer: &natsconn.Dialer{Reader: c, Pool: pool}, PendingPoll: time.Millisecond, Recorder: rec}

	t.Run("EvensLeadersItCanMove", func(t *testing.T) {
		b := reconciled(t, ctx, r, "demo", func(ct *assert.CollectT, b *js.NatsSystemBalancer) {
			condition(ct, b, ConditionReady, metav1.ConditionTrue, ReasonBalancing)
			condition(ct, b, ConditionHolding, metav1.ConditionFalse, ReasonSettled)
			assert.Empty(ct, b.Status.Pending)
			if assert.NotNil(ct, b.Status.Skew) {
				assert.Zero(ct, b.Status.Skew.Leaders, "leaders %v", b.Status.Servers)
			}
		}, "C1's leaders did not come even")
		require.Equal(t, []js.ServerLoad{
			{Name: "C1-0", Leaders: 3, Replicas: 9},
			{Name: "C1-1", Leaders: 3, Replicas: 9},
			{Name: "C1-2", Leaders: 3, Replicas: 9},
		}, b.Status.Servers)
		require.Equal(t, &js.Skew{}, b.Status.Skew)
		require.Equal(t, &js.Capabilities{
			Placement:    true,
			Leader:       js.LeaderCapabilityPartial,
			LeaderReason: "1 of 2 accounts carry no jetstream-stepdown export; their leaders are not moved",
		}, b.Status.Capabilities)
		require.NotNil(t, b.Status.LastMove)
		require.Equal(t, js.MoveLeader, b.Status.LastMove.Kind)
		require.Equal(t, p.aPub, b.Status.LastMove.Account)
		require.True(t, strings.HasPrefix(b.Status.LastMove.Stream, "A_"), b.Status.LastMove.Stream)
		require.Equal(t, "C1-0", b.Status.LastMove.From)
		for _, name := range bStreams {
			require.Equal(t, "C1-0", streamLeader(t, ctx, jsB, name), "%s belongs to an account without the export", name)
		}
		evs := recorded(rec)
		started := notes(evs, "Normal", "MoveStarted")
		require.NotEmpty(t, started)
		require.Equal(t, fmt.Sprintf("leader of %s/%s from %s to %s", p.aPub, b.Status.LastMove.Stream, b.Status.LastMove.From, b.Status.LastMove.To), started[len(started)-1])
		require.ElementsMatch(t, started, notes(evs, "Normal", "MoveDone"), "a move started was not seen done")
	})

	t.Run("OnePerNATSCluster", func(t *testing.T) {
		require.NoError(t, c.Create(ctx, balancer("second", "c1", now.Add(time.Second))))
		require.NoError(t, c.Create(ctx, balancer("other", "c2", now.Add(time.Second))))
		second := reconciled(t, ctx, r, "second", func(ct *assert.CollectT, b *js.NatsSystemBalancer) {
			condition(ct, b, ConditionReady, metav1.ConditionFalse, ReasonDuplicate)
		}, "a second balancer of C1 was not refused")
		require.Equal(t, "NatsSystemBalancer nats-system/demo balances NATS cluster C1", meta.FindStatusCondition(second.Status.Conditions, ConditionReady).Message)
		require.Empty(t, second.Status.Servers)

		other := reconciled(t, ctx, r, "other", func(ct *assert.CollectT, b *js.NatsSystemBalancer) {
			condition(ct, b, ConditionReady, metav1.ConditionTrue, ReasonBalancing)
			condition(ct, b, ConditionHolding, metav1.ConditionFalse, ReasonSettled)
		}, "C2's balancer was refused")
		var names []string
		for _, s := range other.Status.Servers {
			names = append(names, s.Name)
		}
		require.Equal(t, []string{"C2-0", "C2-1", "C2-2"}, names, "a balancer judges only its own NATS cluster")
		require.Equal(t, &js.Capabilities{Placement: true, Leader: js.LeaderCapabilityFull}, other.Status.Capabilities)
	})

	t.Run("EvacuationsScopedByNATSSystem", func(t *testing.T) {
		q := newPlane(t)
		other := startSupercluster(t, q, "C1")
		for _, o := range connection("other-c1", other["C1"][0].ClientURL(), q.sysCreds) {
			require.NoError(t, c.Create(ctx, o))
		}
		drain := func(name, conn string) *js.NatsClusterEvacuation {
			return &js.NatsClusterEvacuation{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
				Spec: js.NatsClusterEvacuationSpec{
					ConnectionRef: natsv1beta1.ObjectReference{Name: conn},
					From:          js.EvacuationSource{Cluster: "C1"},
					To:            js.EvacuationTarget{ServerTags: []string{"new"}},
				},
			}
		}
		nc, denied, err := r.Dialer.Reference(ctx, referrer(ns), natsv1beta1.ObjectReference{Name: "c1"})
		require.NoError(t, err)
		require.Nil(t, denied)

		elsewhere := drain("drain-other-c1", "other-c1")
		require.NoError(t, c.Create(ctx, elsewhere))
		got, err := evacuationOf(ctx, c, r.Dialer, nc)
		require.NoError(t, err)
		require.Empty(t, got, "an evacuation of another NATS system's C1 was taken for this one's")

		here := drain("drain-c1", "c2")
		require.NoError(t, c.Create(ctx, here))
		got, err = evacuationOf(ctx, c, r.Dialer, nc)
		require.NoError(t, err)
		require.Equal(t, "nats-system/drain-c1", got)

		reconciled(t, ctx, r, "demo", func(ct *assert.CollectT, b *js.NatsSystemBalancer) {
			condition(ct, b, ConditionReady, metav1.ConditionTrue, ReasonBalancing)
			condition(ct, b, ConditionHolding, metav1.ConditionFalse, ReasonSettled)
		}, "the balancer held for an evacuation instead of leaving it its streams")
		require.NoError(t, c.Delete(ctx, here))
		require.NoError(t, c.Delete(ctx, elsewhere))
	})

	t.Run("HoldsWhileUnsettled", func(t *testing.T) {
		sc["C1"][2].Shutdown()
		reconciled(t, ctx, r, "demo", func(ct *assert.CollectT, b *js.NatsSystemBalancer) {
			condition(ct, b, ConditionHolding, metav1.ConditionTrue, ReasonUnsettled)
		}, "a server down left the balancer free to move")
	})
}

// streamLeader is the leader of stream name as j sees it.
func streamLeader(t *testing.T, ctx context.Context, j jetstream.JetStream, name string) string {
	t.Helper()
	s, err := j.Stream(ctx, name)
	require.NoError(t, err)
	return s.CachedInfo().Cluster.Leader
}
