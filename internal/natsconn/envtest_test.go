package natsconn

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// TestEnvtest runs the Reconciler in a manager against a real API server:
// Ready follows a connect, a rotated Secret, and a server going away.
func TestEnvtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	n := startNATS(t)
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../config/crd"},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  testScheme(t),
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	require.NoError(t, err)
	p := NewPool()
	require.NoError(t, mgr.Add(p))
	r := &Reconciler{Client: mgr.GetClient(), Pool: p, RetryAfter: time.Hour}
	require.NoError(t, r.SetupWithManager(t.Context(), mgr))
	go func() { _ = mgr.Start(t.Context()) }()

	c, err := client.New(cfg, client.Options{Scheme: testScheme(t)})
	require.NoError(t, err)
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "payments"}}))
	for _, o := range append(n.secrets("payments"), connection("payments", "demo", n.url)) {
		require.NoError(t, c.Create(t.Context(), o))
	}
	requireReady(t, c, metav1.ConditionTrue, ReasonConnected)

	var creds corev1.Secret
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "payments", Name: "creds"}, &creds))
	creds.Data[DefaultCredentialsKey] = []byte("garbage")
	require.NoError(t, c.Update(t.Context(), &creds))
	requireReady(t, c, metav1.ConditionFalse, ReasonInvalidSecret)

	creds.Data[DefaultCredentialsKey] = n.creds
	require.NoError(t, c.Update(t.Context(), &creds))
	requireReady(t, c, metav1.ConditionTrue, ReasonConnected)

	n.srv.Shutdown()
	requireReady(t, c, metav1.ConditionFalse, ReasonDisconnected)
}

func requireReady(t *testing.T, c client.Client, status metav1.ConditionStatus, reason string) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		var nc natsv1beta1.NatsConnection
		if !assert.NoError(ct, c.Get(t.Context(), demo, &nc)) {
			return
		}
		cond := meta.FindStatusCondition(nc.Status.Conditions, ConditionReady)
		if assert.NotNil(ct, cond) {
			assert.Equal(ct, status, cond.Status)
			assert.Equal(ct, reason, cond.Reason)
		}
	}, 20*time.Second, 100*time.Millisecond)
}
