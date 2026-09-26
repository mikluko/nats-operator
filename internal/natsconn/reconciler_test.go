package natsconn

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

var demo = types.NamespacedName{Namespace: "payments", Name: "demo"}

func TestReconcilerReady(t *testing.T) {
	n := startNATS(t)
	tests := []struct {
		name       string
		objs       func() []client.Object
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			name:       "connected",
			objs:       func() []client.Object { return append(n.secrets("payments"), connection("payments", "demo", n.url)) },
			wantStatus: metav1.ConditionTrue,
			wantReason: ReasonConnected,
		},
		{
			name: "creds Secret missing",
			objs: func() []client.Object {
				return []client.Object{n.secrets("payments")[0], connection("payments", "demo", n.url)}
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonSecretNotFound,
		},
		{
			name: "creds key missing",
			objs: func() []client.Object {
				return []client.Object{
					n.secrets("payments")[0],
					secret("payments", "creds", map[string][]byte{"nats.creds": n.creds}),
					connection("payments", "demo", n.url),
				}
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonInvalidSecret,
		},
		{
			name: "creds unparseable",
			objs: func() []client.Object {
				return []client.Object{
					n.secrets("payments")[0],
					secret("payments", "creds", map[string][]byte{DefaultCredentialsKey: []byte("garbage")}),
					connection("payments", "demo", n.url),
				}
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonInvalidSecret,
		},
		{
			name: "server unreachable",
			objs: func() []client.Object {
				return append(n.secrets("payments"), connection("payments", "demo", "tls://127.0.0.1:1"))
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: ReasonConnectFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fakeClient(t, tt.objs()...)
			p := NewPool()
			t.Cleanup(p.Close)
			r := &Reconciler{Client: c, Pool: p, RetryAfter: time.Minute}

			res, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: demo})
			require.NoError(t, err)
			if tt.wantStatus == metav1.ConditionTrue {
				require.Zero(t, res.RequeueAfter)
			} else {
				require.Equal(t, time.Minute, res.RequeueAfter)
			}

			var nc natsv1beta1.NatsConnection
			require.NoError(t, c.Get(t.Context(), demo, &nc))
			cond := meta.FindStatusCondition(nc.Status.Conditions, ConditionReady)
			require.NotNil(t, cond)
			require.Equal(t, tt.wantStatus, cond.Status)
			require.Equal(t, tt.wantReason, cond.Reason)
			require.Equal(t, nc.Generation, cond.ObservedGeneration)
			require.Equal(t, nc.Generation, nc.Status.ObservedGeneration)
		})
	}
}

func TestReconcilerForgetsDeleted(t *testing.T) {
	n := startNATS(t)
	nc := connection("payments", "demo", n.url)
	c := fakeClient(t, append(n.secrets("payments"), nc)...)
	p := NewPool()
	t.Cleanup(p.Close)
	r := &Reconciler{Client: c, Pool: p}
	_, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: demo})
	require.NoError(t, err)
	conn, err := p.Get(ConnectionKey(demo), n.endpoint())
	require.NoError(t, err)

	require.NoError(t, c.Delete(t.Context(), nc))
	_, err = r.Reconcile(t.Context(), reconcile.Request{NamespacedName: demo})
	require.NoError(t, err)
	require.True(t, conn.IsClosed())
}

func TestReconcilerForgetsUnresolvable(t *testing.T) {
	n := startNATS(t)
	objs := append(n.secrets("payments"), connection("payments", "demo", n.url))
	c := fakeClient(t, objs...)
	p := NewPool()
	t.Cleanup(p.Close)
	r := &Reconciler{Client: c, Pool: p}
	_, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: demo})
	require.NoError(t, err)
	conn, err := p.Get(ConnectionKey(demo), n.endpoint())
	require.NoError(t, err)

	require.NoError(t, c.Delete(t.Context(), objs[1]))
	_, err = r.Reconcile(t.Context(), reconcile.Request{NamespacedName: demo})
	require.NoError(t, err)
	require.True(t, conn.IsClosed())
}

func TestReconcilerDisconnected(t *testing.T) {
	n := startNATS(t)
	c := fakeClient(t, append(n.secrets("payments"), connection("payments", "demo", n.url))...)
	p := NewPool()
	t.Cleanup(p.Close)
	r := &Reconciler{Client: c, Pool: p}
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(q.ShutDown)
	r.queue.Store(&q)
	p.OnChange(r.enqueue)

	_, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: demo})
	require.NoError(t, err)
	n.srv.Shutdown()

	req, shutdown := q.Get()
	require.False(t, shutdown)
	require.Equal(t, demo, req.NamespacedName)
	q.Done(req)

	res, err := r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, DefaultRetryAfter, res.RequeueAfter)
	var nc natsv1beta1.NatsConnection
	require.NoError(t, c.Get(t.Context(), demo, &nc))
	cond := meta.FindStatusCondition(nc.Status.Conditions, ConditionReady)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, ReasonDisconnected, cond.Reason)
}

func TestReconcilerEnqueueIgnoresOtherKinds(t *testing.T) {
	r := &Reconciler{}
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(q.ShutDown)
	r.enqueue(ConnectionKey(demo))
	require.Zero(t, q.Len())
	r.queue.Store(&q)
	r.enqueue(Key{Kind: "NatsCluster", NamespacedName: demo})
	require.Zero(t, q.Len())
	r.enqueue(ConnectionKey(demo))
	require.Equal(t, 1, q.Len())
}

func TestSecretNames(t *testing.T) {
	tests := []struct {
		name string
		spec natsv1beta1.NatsConnectionSpec
		want []string
	}{
		{name: "none", spec: natsv1beta1.NatsConnectionSpec{Servers: []string{"nats://a"}}},
		{name: "both", spec: connection("ns", "c", "nats://a").Spec, want: []string{"ca", "creds"}},
		{
			name: "one Secret for both",
			spec: natsv1beta1.NatsConnectionSpec{
				TLS:         &natsv1beta1.ConnectionTLS{CA: &natsv1beta1.CA{SecretKeyRef: natsv1beta1.CASecretKeySelector{Name: "s"}}},
				Credentials: &natsv1beta1.Credentials{SecretKeyRef: natsv1beta1.CredentialsSecretKeySelector{Name: "s"}},
			},
			want: []string{"s"},
		},
		{name: "TLS without CA", spec: natsv1beta1.NatsConnectionSpec{TLS: &natsv1beta1.ConnectionTLS{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, SecretNames(&tt.spec))
		})
	}
}
