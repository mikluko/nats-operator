package balancectl

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/e2e"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// TestBalancerEnvtest runs the reconciler in a manager against a real API
// server over story 7's account balancer, yielding to a NatsSystemBalancer's
// pending placement move.
func TestBalancerEnvtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	const payments = "payments"
	p := newPlane(t)
	sc := startSupercluster(t, p, "C1", "C2")
	jsA := accountJS(t, sc["C1"][1], p.a)
	for _, name := range []string{"REQ_07", "DEF_0"} {
		cfg := jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}, Replicas: 3, Placement: &jetstream.Placement{Cluster: "C1"}}
		require.Eventually(t, func() bool { _, err := jsA.CreateStream(t.Context(), cfg); return err == nil }, 30*time.Second, 200*time.Millisecond, "create %s", name)
	}

	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: testScheme(t), Metrics: metricsserver.Options{BindAddress: "0"}})
	require.NoError(t, err)
	conns := natsconn.NewPool()
	require.NoError(t, mgr.Add(conns))
	r := &BalancerReconciler{Client: mgr.GetClient(), Dialer: &natsconn.Dialer{Reader: mgr.GetClient(), Pool: conns}, PendingPoll: 200 * time.Millisecond}
	require.NoError(t, r.SetupWithManager(t.Context(), mgr))
	go func() { _ = mgr.Start(t.Context()) }()

	c, err := client.New(cfg, client.Options{Scheme: testScheme(t)})
	require.NoError(t, err)
	for _, name := range []string{ns, payments} {
		require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}))
	}
	require.NoError(t, c.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: payments, Name: "demo"},
		Data:       map[string][]byte{natsconn.DefaultCredentialsKey: creds(t, jwtplane.User{}, p.a)},
	}))
	require.NoError(t, c.Create(t.Context(), &natsv1beta1.NatsConnection{
		ObjectMeta: metav1.ObjectMeta{Namespace: payments, Name: "demo"},
		Spec: natsv1beta1.NatsConnectionSpec{
			Servers:     []string{sc["C1"][0].ClientURL()},
			Credentials: &natsv1beta1.Credentials{SecretKeyRef: natsv1beta1.CredentialsSecretKeySelector{Name: "demo"}},
		},
	}))
	sys := &js.NatsSystemBalancer{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "demo"},
		Spec:       js.NatsSystemBalancerSpec{ConnectionRef: natsv1beta1.ObjectReference{Name: "demo"}},
	}
	require.NoError(t, c.Create(t.Context(), sys))
	sys.Status.Pending = []js.Move{{Kind: js.MovePlacement, Account: p.aPub, Stream: "REQ_07", From: "C1-1"}}
	require.NoError(t, c.Status().Update(t.Context(), sys))

	for _, obj := range readManifests(t, "07-balancing/01-account.yaml") {
		require.NoError(t, c.Create(t.Context(), obj), obj.GetName())
	}
	want := storyBalancerStatus(t, "07-balancing/01-status-natsbalancer.yaml")
	meta.SetStatusCondition(&want.Conditions, metav1.Condition{Type: ConditionHolding, Status: metav1.ConditionTrue,
		Reason: ReasonYielding, Message: "REQ_07 has a placement move pending from NatsSystemBalancer demo"})

	var b js.NatsBalancer
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		if !assert.NoError(ct, c.Get(t.Context(), types.NamespacedName{Namespace: payments, Name: "payments"}, &b)) {
			return
		}
		for _, w := range want.Conditions {
			accountCondition(ct, &b, w.Type, w.Status, w.Reason)
		}
		assert.Len(ct, b.Status.Pools, len(want.Pools))
	}, time.Minute, 200*time.Millisecond)

	require.Equal(t, meta.FindStatusCondition(want.Conditions, ConditionHolding).Message, meta.FindStatusCondition(b.Status.Conditions, ConditionHolding).Message)
	var names, wantNames []string
	for i := range want.Pools {
		wantNames, names = append(wantNames, want.Pools[i].Name), append(names, b.Status.Pools[i].Name)
	}
	require.Equal(t, wantNames, names)
	require.Equal(t, js.PoolStatus{Name: "requests", Streams: 1, LeaderSkew: 1}, b.Status.Pools[0])
	require.Equal(t, b.Generation, b.Status.ObservedGeneration)
}

func storyBalancerStatus(t *testing.T, file string) js.NatsBalancerStatus {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(storiesDir, file))
	require.NoError(t, err)
	raw, err = e2e.StripPlaceholders(raw)
	require.NoError(t, err)
	var doc struct {
		Status js.NatsBalancerStatus `json:"status"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &doc))
	require.NotEmpty(t, doc.Status.Conditions)
	return doc.Status
}
