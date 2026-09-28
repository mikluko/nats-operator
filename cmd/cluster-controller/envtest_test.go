package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/manager/managertest"
	"github.com/mikluko/nats-operator/internal/natscluster"
)

const (
	envtestWait = 30 * time.Second
	envtestTick = 100 * time.Millisecond
)

// TestEnvtestOwnedCache pins that setup, in a manager scoped to owned and to
// story 1's namespace, brings story 1's NatsCluster up to date through that
// cache, labels every owned object it renders, and never reconciles a
// NatsCluster in another namespace.
func TestEnvtestOwnedCache(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })

	scheme, err := manager.NewScheme(schemes...)
	require.NoError(t, err)
	b, err := os.ReadFile("../../docs/content/docs/stories/01-quickstart/01-natscluster.yaml")
	require.NoError(t, err)
	nc := &clusterv1beta1.NatsCluster{}
	require.NoError(t, yaml.UnmarshalStrict(b, nc))

	opts := &manager.Options{MetricsAddr: "0", ProbeAddr: "0", WatchNamespaces: []string{nc.Namespace}}
	mgr, err := manager.New(cfg, opts, scheme, owned)
	require.NoError(t, err)
	require.NoError(t, setup(t.Context(), mgr))
	go func() { _ = mgr.Start(t.Context()) }()

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	outside := nc.DeepCopy()
	outside.Namespace = "outside"
	for _, n := range []*clusterv1beta1.NatsCluster{outside, nc} {
		require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n.Namespace}}))
		require.NoError(t, c.Create(t.Context(), n))
	}
	key := client.ObjectKeyFromObject(nc)

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		got := &clusterv1beta1.NatsCluster{}
		if !assert.NoError(ct, c.Get(t.Context(), key, got)) {
			return
		}
		cond := meta.FindStatusCondition(got.Status.Conditions, natscluster.ConditionProgressing)
		if assert.NotNil(ct, cond) {
			assert.Equal(ct, natscluster.ReasonUpToDate, cond.Reason, cond.Message)
		}
	}, envtestWait, envtestTick)

	var sets appsv1.StatefulSetList
	require.NoError(t, c.List(t.Context(), &sets, client.InNamespace(nc.Namespace)))
	require.Len(t, sets.Items, int(nc.Spec.Replicas))
	for i := range sets.Items {
		sts := &sets.Items[i]
		for _, vct := range sts.Spec.VolumeClaimTemplates {
			require.Contains(t, vct.Labels, owned.Label, "claims of %s", sts.Name)
		}
		sts.Status.Replicas, sts.Status.ReadyReplicas = 1, 1
		require.NoError(t, c.Status().Update(t.Context(), sts))
	}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		got := &clusterv1beta1.NatsCluster{}
		if assert.NoError(ct, c.Get(t.Context(), key, got)) {
			assert.Equal(ct, nc.Spec.Replicas, got.Status.ReadyReplicas)
		}
	}, envtestWait, envtestTick)

	for _, kind := range owned.Kinds {
		gvk, err := apiutil.GVKForObject(kind, scheme)
		require.NoError(t, err)
		list, err := scheme.New(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
		require.NoError(t, err)
		require.NoError(t, c.List(t.Context(), list.(client.ObjectList), client.InNamespace(nc.Namespace)))
		require.NoError(t, meta.EachListItem(list, func(o runtime.Object) error {
			obj := o.(client.Object)
			assert.Contains(t, obj.GetLabels(), owned.Label, "%s %s", gvk.Kind, obj.GetName())
			return nil
		}))
	}

	got := &clusterv1beta1.NatsCluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(outside), got))
	require.Empty(t, got.Finalizers, "a NatsCluster outside the watched namespaces")
	require.Empty(t, got.Status.Conditions, "a NatsCluster outside the watched namespaces")
	require.NoError(t, c.List(t.Context(), &sets, client.InNamespace(outside.Namespace)))
	require.Empty(t, sets.Items, "StatefulSets outside the watched namespaces")
}

// TestEnvtestReadyUnderRoles pins config/rbac/cluster-controller as enough
// for the cluster controller to become ready in namespace-scoped mode.
func TestEnvtestReadyUnderRoles(t *testing.T) {
	scheme, err := manager.NewScheme(schemes...)
	require.NoError(t, err)
	managertest.ReadyUnderRoles(t, scheme, owned, setup, "../../config/rbac/cluster-controller", "../../config/crd")
}
