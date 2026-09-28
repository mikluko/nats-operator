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
	"github.com/mikluko/nats-operator/internal/natscluster"
)

const (
	envtestWait = 30 * time.Second
	envtestTick = 100 * time.Millisecond
)

// TestEnvtestOwnedCache runs setup in a manager scoped to owned against a
// real API server: story 1's NatsCluster comes up to date and counts its
// servers ready, which it reads through that cache, and every object of an
// owned kind it renders carries the owner label.
func TestEnvtestOwnedCache(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })

	scheme, err := newScheme()
	require.NoError(t, err)
	mgr, err := manager.New(cfg, &manager.Options{MetricsAddr: "0", ProbeAddr: "0"}, scheme, owned)
	require.NoError(t, err)
	require.NoError(t, setup(t.Context(), mgr))
	go func() { _ = mgr.Start(t.Context()) }()

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	b, err := os.ReadFile("../../docs/content/docs/stories/01-quickstart/01-natscluster.yaml")
	require.NoError(t, err)
	nc := &clusterv1beta1.NatsCluster{}
	require.NoError(t, yaml.UnmarshalStrict(b, nc))
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nc.Namespace}}))
	require.NoError(t, c.Create(t.Context(), nc))
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
}
