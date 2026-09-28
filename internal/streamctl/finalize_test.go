package streamctl

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// TestStreamFinalize_StatusErrorNoRequeue pins that a finalize failing on
// its connection and its status write returns the error with no
// RequeueAfter: controller-runtime ignores a Result beside one.
func TestStreamFinalize_StatusErrorNoRequeue(t *testing.T) {
	down := errors.New("the API server is down")
	s := newStream("audit", "AUDIT", func(s *js.NatsStreamSpec) { s.DeletionPolicy = js.DeletionDelete })
	now := metav1.Now()
	s.DeletionTimestamp, s.Finalizers = &now, []string{lifecycle.Finalizer}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithStatusSubresource(&js.NatsStream{}).
		WithObjects(s, &natsv1beta1.NatsConnection{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "demo"},
			Spec:       natsv1beta1.NatsConnectionSpec{Servers: []string{"nats://127.0.0.1:1"}},
		}).
		WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
			return down
		}}).
		Build()
	pool := natsconn.NewPool()
	t.Cleanup(pool.Close)
	r := &StreamReconciler{Client: c, Dialer: &natsconn.Dialer{Reader: c, Pool: pool}, Syncer: lifecycle.Syncer{Resync: testResync}}
	res, err := r.Reconcile(t.Context(), requestFor("audit"))
	require.ErrorIs(t, err, down)
	require.Zero(t, res)
}
