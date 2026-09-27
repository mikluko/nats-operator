package balancectl

import (
	"context"
	"fmt"
	"slices"
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
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// labelled is a NatsStream for stream on connection conn carrying labels.
func labelled(stream, conn string, labels map[string]string) *js.NatsStream {
	return &js.NatsStream{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: stream, Labels: labels},
		Spec:       js.NatsStreamSpec{ConnectionRef: natsv1beta1.ObjectReference{Name: conn}, StreamConfig: js.StreamConfig{Name: stream}},
	}
}

func accountBalancer(name, conn string, pools ...js.Pool) *js.NatsBalancer {
	return &js.NatsBalancer{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID("uid-account-" + name)},
		Spec: js.NatsBalancerSpec{
			ConnectionRef: natsv1beta1.ObjectReference{Name: conn},
			Pools:         pools,
			Interval:      &metav1.Duration{Duration: 10 * time.Millisecond},
		},
	}
}

// reconciledAccount reconciles the balancer "payments" until its status
// satisfies want, and returns it.
func reconciledAccount(t *testing.T, ctx context.Context, r *BalancerReconciler, want func(*assert.CollectT, *js.NatsBalancer), msg string) *js.NatsBalancer {
	t.Helper()
	var b js.NatsBalancer
	key := client.ObjectKey{Namespace: ns, Name: "payments"}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, r.Client.Get(ctx, key, &b))
		want(ct, &b)
	}, 2*time.Minute, 50*time.Millisecond, msg)
	return &b
}

func accountCondition(ct assert.TestingT, b *js.NatsBalancer, typ string, status metav1.ConditionStatus, reason string) {
	c := meta.FindStatusCondition(b.Status.Conditions, typ)
	if assert.NotNil(ct, c, typ) {
		assert.Equal(ct, status, c.Status, "%s: %s", typ, c.Message)
		assert.Equal(ct, reason, c.Reason, "%s: %s", typ, c.Message)
	}
}

// TestBalancer_Pools runs the reconciler against a NATS cluster in operator
// mode, on a connection of account A, whose streams in three pools, one of
// them the default, all start led by C1-0.
func TestBalancer_Pools(t *testing.T) {
	t.Parallel()
	p := newPlane(t)
	sc := startSupercluster(t, p, "C1", "C2")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	jsA := accountJS(t, sc["C1"][1], p.a)
	req := skewedStreams(t, ctx, jsA, "REQ", 2)
	res := skewedStreams(t, ctx, jsA, "RES", 3)
	skewedStreams(t, ctx, jsA, "DEF", 3)
	require.Eventually(t, func() bool {
		_, err := jsA.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "cfg", Replicas: 3, Placement: &jetstream.Placement{Cluster: "C1"}})
		return err == nil
	}, 30*time.Second, 200*time.Millisecond, "create bucket cfg")

	objs := connection("a", sc["C1"][0].ClientURL(), creds(t, jwtplane.User{}, p.a))
	for _, s := range req {
		objs = append(objs, labelled(s, "a", map[string]string{"traffic": "requests"}))
	}
	for _, s := range res {
		objs = append(objs, labelled(s, "a", map[string]string{"traffic": "responses"}))
	}
	objs = append(objs, &js.NatsKeyValue{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "cfg", Labels: map[string]string{"traffic": "requests"}},
		Spec:       js.NatsKeyValueSpec{ConnectionRef: natsv1beta1.ObjectReference{Name: "a"}},
	})
	requests, responses := pool("requests", "traffic", "requests"), pool("responses", "traffic", "responses")
	objs = append(objs, accountBalancer("payments", "a", requests, responses))
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithStatusSubresource(&js.NatsBalancer{}, &js.NatsSystemBalancer{}).
		WithObjects(objs...).
		Build()
	conns := natsconn.NewPool()
	t.Cleanup(conns.Close)
	rec := events.NewFakeRecorder(1000)
	r := &BalancerReconciler{Client: c, Dialer: &natsconn.Dialer{Reader: c, Pool: conns}, PendingPoll: time.Millisecond, Recorder: rec}

	even := []js.PoolStatus{
		{Name: "requests", Streams: 3},
		{Name: "responses", Streams: 3},
		{Name: "(default)", Streams: 3},
	}

	t.Run("EvensEachPool", func(t *testing.T) {
		reconciledAccount(t, ctx, r, func(ct *assert.CollectT, b *js.NatsBalancer) {
			accountCondition(ct, b, ConditionReady, metav1.ConditionTrue, ReasonBalancing)
			accountCondition(ct, b, ConditionHolding, metav1.ConditionFalse, ReasonSettled)
			accountCondition(ct, b, ConditionOverlapping, metav1.ConditionFalse, ReasonDisjoint)
			assert.Equal(ct, even, b.Status.Pools)
		}, "the pools did not come even")
		for _, pool := range [][]string{append(req, "KV_cfg"), res} {
			leaders := map[string]bool{}
			for _, s := range pool {
				leaders[streamLeader(t, ctx, jsA, s)] = true
			}
			require.Len(t, leaders, 3, "%v is led by one server each", pool)
		}
		started := notes(recorded(rec), "Normal", "MoveStarted")
		require.NotEmpty(t, started, "no move was recorded")
		for _, note := range started {
			require.True(t, strings.HasPrefix(note, "leader of "+p.aPub+"/"), note)
		}
	})

	t.Run("PacesFromStatusAcrossRestarts", func(t *testing.T) {
		requestsPool := append(slices.Clone(req), "KV_cfg")
		for _, s := range requestsPool {
			stepToFirst(t, ctx, jsA, s)
		}
		var b js.NatsBalancer
		key := client.ObjectKey{Namespace: ns, Name: "payments"}
		require.NoError(t, c.Get(ctx, key, &b))
		b.Spec.Interval = &metav1.Duration{Duration: time.Hour}
		require.NoError(t, c.Update(ctx, &b))
		recent := metav1.NewTime(time.Now().Add(-time.Minute))
		b.Status.LastMove = &js.Move{Kind: js.MoveLeader, Account: p.aPub, Stream: res[0], From: "C1-1", To: "C1-2", Time: &recent}
		require.NoError(t, c.Status().Update(ctx, &b))

		for range 5 {
			fresh := &BalancerReconciler{Client: c, Dialer: r.Dialer, PendingPoll: time.Millisecond}
			_, err := fresh.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			require.NoError(t, err)
		}
		for _, s := range requestsPool {
			require.Equal(t, "C1-0", streamLeader(t, ctx, jsA, s), "a restarted balancer moved inside the interval of its last move")
		}

		require.NoError(t, c.Get(ctx, key, &b))
		due := metav1.NewTime(time.Now().Add(-2 * time.Hour))
		b.Status.LastMove.Time = &due
		require.NoError(t, c.Status().Update(ctx, &b))
		got := reconciledAccount(t, ctx, &BalancerReconciler{Client: c, Dialer: r.Dialer, PendingPoll: time.Millisecond}, func(ct *assert.CollectT, b *js.NatsBalancer) {
			assert.True(ct, b.Status.LastMove.Time.After(due.Time), "no move once the interval had passed")
		}, "the balancer did not move once its last move was an interval old")
		require.Equal(t, p.aPub, got.Status.LastMove.Account)
		require.Contains(t, requestsPool, got.Status.LastMove.Stream)
		require.Equal(t, "C1-0", got.Status.LastMove.From)

		require.NoError(t, c.Get(ctx, key, &b))
		b.Spec.Interval = &metav1.Duration{Duration: 10 * time.Millisecond}
		require.NoError(t, c.Update(ctx, &b))
		reconciledAccount(t, ctx, r, func(ct *assert.CollectT, b *js.NatsBalancer) {
			assert.Equal(ct, even, b.Status.Pools)
		}, "the pools did not come even again")
	})

	t.Run("ReportsOverlap", func(t *testing.T) {
		var s js.NatsStream
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: ns, Name: req[0]}, &s))
		s.Labels["hot"] = "true"
		require.NoError(t, c.Update(ctx, &s))
		var b js.NatsBalancer
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "payments"}, &b))
		b.Spec.Pools = append(b.Spec.Pools, pool("hot", "hot", "true"))
		require.NoError(t, c.Update(ctx, &b))

		got := reconciledAccount(t, ctx, r, func(ct *assert.CollectT, b *js.NatsBalancer) {
			accountCondition(ct, b, ConditionOverlapping, metav1.ConditionTrue, ReasonOverlapping)
		}, "the overlap was not reported")
		require.Equal(t, "REQ_0 matches requests and hot; balanced in requests", meta.FindStatusCondition(got.Status.Conditions, ConditionOverlapping).Message)
		require.Equal(t, []js.PoolStatus{even[0], even[1], {Name: "hot"}, even[2]}, got.Status.Pools)
	})

	t.Run("YieldsToSystemBalancer", func(t *testing.T) {
		sys := &js.NatsSystemBalancer{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "demo"}}
		require.NoError(t, c.Create(ctx, sys))
		sys.Status.Pending = []js.Move{{Kind: js.MovePlacement, Account: p.aPub, Stream: req[1], From: "C1-1"}}
		require.NoError(t, c.Status().Update(ctx, sys))
		stepToFirst(t, ctx, jsA, req[1])
		stepToFirst(t, ctx, jsA, req[0])

		for range 20 {
			b := reconciledAccount(t, ctx, r, func(ct *assert.CollectT, b *js.NatsBalancer) {
				accountCondition(ct, b, ConditionHolding, metav1.ConditionTrue, ReasonYielding)
			}, "the balancer did not yield")
			require.Equal(t, "REQ_1 has a placement move pending from NatsSystemBalancer demo", meta.FindStatusCondition(b.Status.Conditions, ConditionHolding).Message)
			time.Sleep(20 * time.Millisecond)
		}
		require.Equal(t, "C1-0", streamLeader(t, ctx, jsA, req[0]), "a leader moved while the system balancer had a move pending")
		require.Equal(t, "C1-0", streamLeader(t, ctx, jsA, req[1]), "a leader moved while the system balancer had a move pending")

		sys.Status.Pending = nil
		require.NoError(t, c.Status().Update(ctx, sys))
		reconciledAccount(t, ctx, r, func(ct *assert.CollectT, b *js.NatsBalancer) {
			accountCondition(ct, b, ConditionHolding, metav1.ConditionFalse, ReasonSettled)
			if assert.NotEmpty(ct, b.Status.Pools) {
				assert.Zero(ct, b.Status.Pools[0].LeaderSkew, "%v", b.Status.Pools)
			}
		}, "the balancer did not resume")
	})
}

// stepToFirst moves stream's leader to C1-0, the first server of C1, through
// j's own stepdown API.
func stepToFirst(t *testing.T, ctx context.Context, j jetstream.JetStream, stream string) {
	t.Helper()
	const server = "C1-0"
	require.Eventually(t, func() bool {
		s, err := j.Stream(ctx, stream)
		if err != nil || s.CachedInfo().Cluster == nil {
			return false
		}
		switch s.CachedInfo().Cluster.Leader {
		case server:
			return true
		case "":
			return false
		}
		_, _ = j.Conn().Request("$JS.API.STREAM.LEADER.STEPDOWN."+stream, fmt.Appendf(nil, `{"placement":{"preferred":%q}}`, server), 5*time.Second)
		return false
	}, time.Minute, 300*time.Millisecond, "%s did not move to %s", stream, server)
}
