package grant_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
)

// TestEnvtest pins story 4 against a real API server and an informer cache:
// the grant admits payments and not orders, a grant event requeues the
// payments users through the cache's index, and deleting the grant revokes.
func TestEnvtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../config/crd"},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })

	s := scheme(t)
	require.NoError(t, corev1.AddToScheme(s))
	c, err := client.New(cfg, client.Options{Scheme: s})
	require.NoError(t, err)
	for _, ns := range []string{"nats-system", "payments", "orders"} {
		require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	}

	informers, err := cache.New(cfg, cache.Options{Scheme: s})
	require.NoError(t, err)
	require.NoError(t, grant.IndexReferrers(t.Context(), informers, &authv1beta1.NatsUser{}, userTargets))
	go func() { _ = informers.Start(t.Context()) }()
	require.True(t, informers.WaitForCacheSync(t.Context()))

	g := paymentsGrant("payments")
	require.NoError(t, c.Create(t.Context(), g))
	for _, u := range []*authv1beta1.NatsUser{natsUser("payments", "api", "nats-system"), natsUser("orders", "api", "nats-system")} {
		require.NoError(t, c.Create(t.Context(), u))
	}

	payments := grant.Referrer{Group: authGroup, Kind: "NatsUser", Namespace: "payments"}
	orders := grant.Referrer{Group: authGroup, Kind: "NatsUser", Namespace: "orders"}
	account := grant.Target{Group: authGroup, Kind: "NatsAccount", Namespace: "nats-system", Name: "payments"}
	admitted := func(rt require.TestingT, from grant.Referrer) bool {
		cond, err := grant.Admit(t.Context(), informers, from, account)
		require.NoError(rt, err)
		return cond == nil
	}

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assert.True(ct, admitted(ct, payments))
	}, 10*time.Second, 50*time.Millisecond)
	require.False(t, admitted(t, orders))

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
		defer q.ShutDown()
		grant.EnqueueReferrers(informers, natsUserKind, &authv1beta1.NatsUserList{}).
			Update(t.Context(), event.UpdateEvent{ObjectOld: g, ObjectNew: g}, q)
		if !assert.Equal(ct, 1, q.Len()) {
			return
		}
		r, _ := q.Get()
		assert.Equal(ct, types.NamespacedName{Namespace: "payments", Name: "api"}, r.NamespacedName)
	}, 10*time.Second, 50*time.Millisecond)

	require.NoError(t, c.Delete(t.Context(), g))
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assert.False(ct, admitted(ct, payments))
	}, 10*time.Second, 50*time.Millisecond)
}
