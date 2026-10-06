package natscluster

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// TestEnvtestSettledLeadersElsewhere pins, against a real API server and
// the monitoring endpoints serving testdata/leaders-elsewhere, that a
// NatsCluster whose servers all answer and are Ready, but whose stream and
// consumer groups name a leader in another NATS cluster, is Ready and not
// Settled, with every such group counted leaderless.
func TestEnvtestSettledLeadersElsewhere(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, natsv1beta1.AddToScheme(scheme))
	require.NoError(t, clusterv1beta1.AddToScheme(scheme))
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := t.Context()

	nc := storyCluster(t)
	var eps monitorAt
	for i, name := range serverNames(nc) {
		jsz, err := os.ReadFile(filepath.Join("testdata", "leaders-elsewhere", name+".json"))
		require.NoError(t, err)
		mux := http.NewServeMux()
		mux.HandleFunc("/varz", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprintf(w, `{"server_name":%q,"server_id":"ID%d","version":"2.15.0","jetstream":{"config":{}}}`, name, i)
		})
		mux.HandleFunc("/gatewayz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) })
		mux.HandleFunc("/jsz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(jsz) })
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		eps = append(eps, sysobs.Endpoint{Name: name, URL: srv.URL})
	}
	r := &Reconciler{Client: c, Observer: eps, ControllerNamespace: "nats-operator"}

	require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nc.Namespace}}))
	require.NoError(t, c.Create(ctx, nc))
	key := types.NamespacedName{Namespace: nc.Namespace, Name: nc.Name}
	reconcile := func() *clusterv1beta1.NatsCluster {
		t.Helper()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		got := &clusterv1beta1.NatsCluster{}
		require.NoError(t, c.Get(ctx, key, got))
		return got
	}

	reconcile()
	var sets appsv1.StatefulSetList
	require.NoError(t, c.List(ctx, &sets, client.InNamespace(nc.Namespace)))
	require.Len(t, sets.Items, 3)
	for i := range sets.Items {
		sts := &sets.Items[i]
		sts.Status.Replicas, sts.Status.ReadyReplicas = 1, 1
		require.NoError(t, c.Status().Update(ctx, sts))
	}
	got := reconcile()

	requireCondition(t, got, ConditionReady, metav1.ConditionTrue, ReasonAllServersReady)
	requireCondition(t, got, ConditionSettled, metav1.ConditionFalse, ReasonGroupsLeaderless)
	require.Equal(t, "4 Raft groups have no leader", meta.FindStatusCondition(got.Status.Conditions, ConditionSettled).Message)
}
