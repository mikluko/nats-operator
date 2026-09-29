package grant_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
)

var natsUserKind = schema.GroupKind{Group: authGroup, Kind: "NatsUser"}

func natsUser(ns, name, accountNS string) *authv1beta1.NatsUser {
	u := &authv1beta1.NatsUser{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	u.Spec.AccountRef.Kind = authv1beta1.AccountKindAccount
	u.Spec.AccountRef.Name = "payments"
	u.Spec.AccountRef.Namespace = accountNS
	return u
}

func userTargets(o client.Object) []string {
	return []string{o.(*authv1beta1.NatsUser).Spec.AccountRef.Namespace}
}

func TestEnqueueReferrers(t *testing.T) {
	users := []client.Object{
		natsUser("payments", "api", "nats-system"),
		natsUser("payments", "jetstream", "nats-system"),
		natsUser("payments", "local", ""),
		natsUser("payments", "elsewhere", "other-system"),
		natsUser("orders", "api", "nats-system"),
		natsUser("billing", "api", "nats-system"),
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme(t)).
		WithObjects(users...).
		WithIndex(&authv1beta1.NatsUser{}, grant.TargetNamespaceField, userIndex(t)).
		Build()
	h := grant.EnqueueReferrers(c, natsUserKind, &authv1beta1.NatsUserList{})

	withFrom := func(namespaces ...string) *natsv1beta1.NatsReferenceGrant {
		g := paymentsGrant("")
		g.Spec.From = nil
		for _, ns := range namespaces {
			g.Spec.From = append(g.Spec.From, natsv1beta1.ReferenceGrantFrom{Group: authGroup, Kind: "NatsUser", Namespace: ns})
		}
		g.Spec.From = append(g.Spec.From, natsv1beta1.ReferenceGrantFrom{Group: authGroup, Kind: "NatsAccount", Namespace: "billing"})
		return g
	}
	req := func(ns, name string) reconcile.Request {
		return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
	}
	tests := []struct {
		name string
		send func(q workqueue.TypedRateLimitingInterface[reconcile.Request])
		want []reconcile.Request
	}{
		{"create", func(q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			h.Create(t.Context(), event.CreateEvent{Object: withFrom("payments")}, q)
		}, []reconcile.Request{req("payments", "api"), req("payments", "jetstream")}},
		{"delete", func(q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			h.Delete(t.Context(), event.DeleteEvent{Object: withFrom("orders")}, q)
		}, []reconcile.Request{req("orders", "api")}},
		{"update that drops a namespace", func(q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			h.Update(t.Context(), event.UpdateEvent{ObjectOld: withFrom("payments", "orders"), ObjectNew: withFrom("orders")}, q)
		}, []reconcile.Request{req("payments", "api"), req("payments", "jetstream"), req("orders", "api")}},
		{"generic", func(q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			h.Generic(t.Context(), event.GenericEvent{Object: withFrom("billing")}, q)
		}, []reconcile.Request{req("billing", "api")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()
			tt.send(q)
			var got []reconcile.Request
			for q.Len() > 0 {
				r, _ := q.Get()
				got = append(got, r)
				q.Done(r)
			}
			require.ElementsMatch(t, tt.want, got)
		})
	}
}

// capture is a FieldIndexer that keeps the one extractor registered on it.
type capture struct {
	field   string
	extract client.IndexerFunc
}

func (c *capture) IndexField(_ context.Context, _ client.Object, field string, extract client.IndexerFunc) error {
	c.field, c.extract = field, extract
	return nil
}

// userIndex is the extractor IndexReferrers registers for NatsUser.
func userIndex(t *testing.T) client.IndexerFunc {
	t.Helper()
	var c capture
	require.NoError(t, grant.IndexReferrers(t.Context(), &c, &authv1beta1.NatsUser{}, userTargets))
	require.Equal(t, grant.TargetNamespaceField, c.field)
	return c.extract
}

func TestIndexReferrers(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{"none", nil, nil},
		{"own and empty dropped", []string{"", "payments"}, nil},
		{"duplicates dropped", []string{"nats-system", "a", "nats-system"}, []string{"nats-system", "a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c capture
			require.NoError(t, grant.IndexReferrers(t.Context(), &c, &authv1beta1.NatsUser{}, func(client.Object) []string { return tt.in }))
			require.Equal(t, tt.want, c.extract(natsUser("payments", "u", "")))
		})
	}
}
