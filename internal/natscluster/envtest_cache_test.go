package natscluster

import (
	"maps"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/manager"
)

// TestEnvtestCreateFindsUncached pins that, reading through a manager whose
// cache holds only objects labelled LabelCluster, a create that finds an
// object the cache does not hold refuses it by name when the NatsCluster does
// not control it, and proceeds when it does.
func TestEnvtestCreateFindsUncached(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	ctx := t.Context()

	scheme, err := manager.NewScheme(natsv1beta1.AddToScheme, clusterv1beta1.AddToScheme)
	require.NoError(t, err)
	owned := manager.Owned{Label: LabelCluster, Kinds: []client.Object{
		&appsv1.StatefulSet{}, &corev1.ConfigMap{}, &corev1.Service{}, &corev1.PersistentVolumeClaim{},
		&policyv1.PodDisruptionBudget{}, &networkingv1.NetworkPolicy{},
	}}
	mgr, err := manager.New(cfg, &manager.Options{MetricsAddr: "0", ProbeAddr: "0"}, scheme, owned)
	require.NoError(t, err)
	go func() { _ = mgr.Start(ctx) }()
	require.True(t, mgr.GetCache().WaitForCacheSync(ctx))

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	cached := &Reconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Observer: &fakeObserver{}, ControllerNamespace: "nats-operator"}

	newCluster := func(t *testing.T, ns string) *clusterv1beta1.NatsCluster {
		t.Helper()
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		nc := storyCluster(t)
		nc.Namespace = ns
		require.NoError(t, c.Create(ctx, nc))
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			assert.NoError(ct, mgr.GetClient().Get(ctx, client.ObjectKeyFromObject(nc), &clusterv1beta1.NatsCluster{}))
		}, 10*time.Second, 50*time.Millisecond)
		return nc
	}

	t.Run("an unlabelled object it does not control is refused by name", func(t *testing.T) {
		nc := newCluster(t, "uncontrolled")
		ns := nc.Namespace
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: ns},
			Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "other"}, Ports: []corev1.ServicePort{{Name: "http", Port: 80}}},
		}
		deny := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: ns},
			Spec:       networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}},
		}
		require.NoError(t, c.Create(ctx, svc))
		require.NoError(t, c.Create(ctx, deny))

		_, err := cached.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
		var nce *notControlledError
		require.ErrorAs(t, err, &nce)
		require.Equal(t, []string{"Service demo", "NetworkPolicy demo"}, nce.Objects)

		got := &clusterv1beta1.NatsCluster{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(nc), got))
		ready := meta.FindStatusCondition(got.Status.Conditions, ConditionReady)
		require.NotNil(t, ready)
		require.Equal(t, ReasonReconcileFailed, ready.Reason)
		require.Equal(t, "not controlled by this NatsCluster: Service demo, NetworkPolicy demo", ready.Message)

		var haveSvc corev1.Service
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(svc), &haveSvc))
		require.Empty(t, haveSvc.OwnerReferences)
		require.Equal(t, svc.Spec.Selector, haveSvc.Spec.Selector)
		var haveNP networkingv1.NetworkPolicy
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(deny), &haveNP))
		require.Empty(t, haveNP.OwnerReferences)
		require.Equal(t, deny.Spec, haveNP.Spec)
	})

	t.Run("a StatefulSet it controls that the cache does not hold is not refused", func(t *testing.T) {
		nc := newCluster(t, "unseen")
		direct := &Reconciler{Client: c, Observer: &fakeObserver{}, ControllerNamespace: "nats-operator"}
		_, err := direct.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
		require.NoError(t, err)

		var sets appsv1.StatefulSetList
		require.NoError(t, c.List(ctx, &sets, client.InNamespace(nc.Namespace)))
		require.Len(t, sets.Items, int(nc.Spec.Replicas))
		for i := range sets.Items {
			sts := &sets.Items[i]
			labels := maps.Clone(sts.Labels)
			delete(labels, LabelCluster)
			sts.Labels = labels
			require.NoError(t, c.Update(ctx, sts))
		}
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			var seen appsv1.StatefulSetList
			if assert.NoError(ct, mgr.GetClient().List(ctx, &seen, client.InNamespace(nc.Namespace))) {
				assert.Empty(ct, seen.Items)
			}
		}, 10*time.Second, 50*time.Millisecond)

		_, err = cached.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
		require.NoError(t, err)
		got := &clusterv1beta1.NatsCluster{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(nc), got))
		ready := meta.FindStatusCondition(got.Status.Conditions, ConditionReady)
		require.NotNil(t, ready)
		require.NotEqual(t, ReasonReconcileFailed, ready.Reason, ready.Message)
	})
}
