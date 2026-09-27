package streamctl

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/refindex"
)

const (
	testNamespace = "apps"
	testResync    = time.Minute
)

// fixture drives the reconcilers directly against a fake API server and an
// in-process nats-server with one NatsConnection, "demo", reaching it.
type fixture struct {
	t         *testing.T
	nats      *testNATS
	js        jetstream.JetStream
	c         client.Client
	streams   *StreamReconciler
	consumers *ConsumerReconciler
	kvs       *KeyValueReconciler
	stores    *ObjectStoreReconciler
	now       time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	n := startNATS(t, 1, false)
	b := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&js.NatsStream{}, &js.NatsConsumer{}, &js.NatsKeyValue{}, &js.NatsObjectStore{}, &natsv1beta1.NatsConnection{}).
		WithObjects(&natsv1beta1.NatsConnection{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "demo"},
			Spec:       natsv1beta1.NatsConnectionSpec{Servers: n.urls},
		})
	idx := indexerFunc(func(obj client.Object, field string, extract client.IndexerFunc) { b.WithIndex(obj, field, extract) })
	require.NoError(t, refindex.IndexUID(t.Context(), idx, &js.NatsStream{}))
	require.NoError(t, refindex.IndexUID(t.Context(), idx, &js.NatsConsumer{}))
	require.NoError(t, refindex.IndexUID(t.Context(), idx, &js.NatsKeyValue{}))
	require.NoError(t, refindex.IndexUID(t.Context(), idx, &js.NatsObjectStore{}))
	c := b.Build()
	pool := natsconn.NewPool()
	t.Cleanup(pool.Close)
	f := &fixture{t: t, nats: n, js: n.connect(t), c: c, now: time.Date(2026, 9, 26, 9, 12, 6, 0, time.UTC)}
	dialer := &natsconn.Dialer{Reader: c, Pool: pool}
	syncer := lifecycle.Syncer{Resync: testResync, Now: func() time.Time { return f.now }}
	f.streams = &StreamReconciler{Client: c, Dialer: dialer, Syncer: syncer}
	f.consumers = &ConsumerReconciler{Client: c, Dialer: dialer, Syncer: syncer}
	f.kvs = &KeyValueReconciler{Client: c, Dialer: dialer, Syncer: syncer}
	f.stores = &ObjectStoreReconciler{Client: c, Dialer: dialer, Syncer: syncer}
	return f
}

var demo = natsv1beta1.ObjectReference{Name: "demo"}

// newStream is a NatsStream named name on demo with server-side name
// server, generation 1.
func newStream(name, server string, mutate func(*js.NatsStreamSpec)) *js.NatsStream {
	s := &js.NatsStream{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name, UID: uuid.NewUUID(), Generation: 1},
		Spec: js.NatsStreamSpec{
			ConnectionRef:  demo,
			Policies:       js.Policies{AdoptionPolicy: js.AdoptionNever, TerminalPolicy: js.TerminalHold},
			DeletionPolicy: js.DeletionRetain,
			StreamConfig:   js.StreamConfig{Name: server, Subjects: []string{name + ".>"}},
		},
	}
	if mutate != nil {
		mutate(&s.Spec)
	}
	return s
}

func (f *fixture) create(obj client.Object) {
	f.t.Helper()
	require.NoError(f.t, f.c.Create(f.t.Context(), obj))
}

// edit applies mutate to the NatsStream name and bumps its generation, as
// the API server does on a spec change.
func (f *fixture) editStream(name string, mutate func(*js.NatsStreamSpec)) {
	f.t.Helper()
	s := f.stream(name)
	mutate(&s.Spec)
	s.Generation++
	require.NoError(f.t, f.c.Update(f.t.Context(), s))
}

func (f *fixture) editConsumer(name string, mutate func(*js.NatsConsumerSpec)) {
	f.t.Helper()
	c := f.consumer(name)
	mutate(&c.Spec)
	c.Generation++
	require.NoError(f.t, f.c.Update(f.t.Context(), c))
}

func (f *fixture) reconcileStream(name string) reconcile.Result {
	f.t.Helper()
	res, err := f.streams.Reconcile(f.t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: name}})
	require.NoError(f.t, err)
	return res
}

func (f *fixture) reconcileConsumer(name string) reconcile.Result {
	f.t.Helper()
	res, err := f.consumers.Reconcile(f.t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: name}})
	require.NoError(f.t, err)
	return res
}

func (f *fixture) stream(name string) *js.NatsStream {
	f.t.Helper()
	var s js.NatsStream
	require.NoError(f.t, f.c.Get(f.t.Context(), types.NamespacedName{Namespace: testNamespace, Name: name}, &s))
	return &s
}

func (f *fixture) consumer(name string) *js.NatsConsumer {
	f.t.Helper()
	var c js.NatsConsumer
	require.NoError(f.t, f.c.Get(f.t.Context(), types.NamespacedName{Namespace: testNamespace, Name: name}, &c))
	return &c
}

func (f *fixture) gone(obj client.Object) bool {
	f.t.Helper()
	err := f.c.Get(f.t.Context(), client.ObjectKeyFromObject(obj), obj)
	return client.IgnoreNotFound(err) == nil && err != nil
}

// serverStream returns the stream's config, or nil when it does not exist.
func (f *fixture) serverStream(name string) *jetstream.StreamConfig {
	f.t.Helper()
	s, err := f.js.Stream(f.t.Context(), name)
	if err != nil {
		require.ErrorIs(f.t, err, jetstream.ErrStreamNotFound)
		return nil
	}
	info, err := s.Info(f.t.Context())
	require.NoError(f.t, err)
	return &info.Config
}

// serverConsumer returns the info of consumer name on ORDERS, or nil when
// it does not exist.
func (f *fixture) serverConsumer(name string) *jetstream.ConsumerInfo {
	f.t.Helper()
	const stream = "ORDERS"
	info, err := f.js.Stream(f.t.Context(), stream)
	require.NoError(f.t, err)
	names := info.ConsumerNames(f.t.Context())
	var found bool
	for n := range names.Name() {
		found = found || n == name
	}
	if !found {
		return nil
	}
	c, err := f.js.Consumer(f.t.Context(), stream, name)
	if err != nil {
		pc, perr := f.js.PushConsumer(f.t.Context(), stream, name)
		require.NoError(f.t, perr)
		return pc.CachedInfo()
	}
	return c.CachedInfo()
}

// condition requires conds to carry typ with status and reason, and
// returns it.
func condition(t *testing.T, conds []metav1.Condition, typ string, status metav1.ConditionStatus, reason string) *metav1.Condition {
	t.Helper()
	c := meta.FindStatusCondition(conds, typ)
	require.NotNil(t, c, "no %s condition in %+v", typ, conds)
	require.Equal(t, status, c.Status, "%s: %+v", typ, c)
	require.Equal(t, reason, c.Reason, "%s: %+v", typ, c)
	return c
}

// jetstreamConsumer is a durable pull consumer config named name, as a
// client other than the controller creates it.
func jetstreamConsumer(name string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{Durable: name, AckPolicy: jetstream.AckExplicitPolicy}
}
