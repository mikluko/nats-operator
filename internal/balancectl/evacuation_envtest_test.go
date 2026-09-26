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
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// TestEvacuationEnvtest runs the reconciler in a manager against a real API
// server over story 11's evacuation of prod-east into prod-east-2, NATS
// clusters of one in-process supercluster whose second carries the tag
// cluster:prod-east-2. prod-east holds a stream and a key-value bucket whose
// resources pin it, a stream with no resource whose config names it, and a
// stream that declares nothing. The story's NatsCluster is applied and
// stands for nothing: no cluster controller runs.
func TestEvacuationEnvtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	p := newPlane(t)
	sc := startTaggedSupercluster(t, p, map[string][]string{"prod-east-2": {"cluster:prod-east-2"}}, "prod-east", "prod-east-2")

	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: testScheme(t), Metrics: metricsserver.Options{BindAddress: "0"}})
	require.NoError(t, err)
	pool := natsconn.NewPool()
	require.NoError(t, mgr.Add(pool))
	r := &EvacuationReconciler{Client: mgr.GetClient(), Dialer: &natsconn.Dialer{Reader: mgr.GetClient(), Pool: pool}, PendingPoll: 200 * time.Millisecond}
	require.NoError(t, r.SetupWithManager(t.Context(), mgr))
	go func() { _ = mgr.Start(t.Context()) }()

	c, err := client.New(cfg, client.Options{Scheme: testScheme(t)})
	require.NoError(t, err)
	for _, name := range []string{ns, "orders", "payments"} {
		require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}))
	}
	for _, obj := range connection("prod-east-sys", sc["prod-east-2"][0].ClientURL(), p.sysCreds) {
		require.NoError(t, c.Create(t.Context(), obj))
	}
	pin := &js.Placement{Cluster: "prod-east"}
	orders := &js.NatsStream{
		ObjectMeta: metav1.ObjectMeta{Namespace: "orders", Name: "orders"},
		Spec:       js.NatsStreamSpec{ConnectionRef: natsv1beta1.ObjectReference{Name: "orders"}, StreamConfig: js.StreamConfig{Name: "ORDERS", Placement: pin}},
	}
	sessions := &js.NatsKeyValue{
		ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "sessions"},
		Spec:       js.NatsKeyValueSpec{ConnectionRef: natsv1beta1.ObjectReference{Name: "payments"}, KeyValueConfig: js.KeyValueConfig{Name: "sessions", Placement: pin}},
	}
	require.NoError(t, c.Create(t.Context(), orders))
	require.NoError(t, c.Create(t.Context(), sessions))

	ctx := t.Context()
	jsA := accountJS(t, sc["prod-east"][0], p.a)
	jsB := accountJS(t, sc["prod-east"][1], p.b)
	owned := func(uid types.UID) map[string]string { return map[string]string{lifecycle.OwnerKey: string(uid)} }
	for _, cfg := range []jetstream.StreamConfig{
		{Name: "ORDERS", Subjects: []string{"orders.>"}, Replicas: 3, Placement: &jetstream.Placement{Cluster: "prod-east"}, Metadata: owned(orders.UID)},
		{Name: "audit", Subjects: []string{"audit.>"}, Replicas: 3, Placement: &jetstream.Placement{Cluster: "prod-east"}},
		{Name: "events", Subjects: []string{"events.>"}, Replicas: 1},
	} {
		require.Eventually(t, func() bool { _, err := jsA.CreateStream(ctx, cfg); return err == nil }, 30*time.Second, 200*time.Millisecond, "create %s", cfg.Name)
	}
	_, err = jsB.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "sessions", Replicas: 3, Placement: &jetstream.Placement{Cluster: "prod-east"}, Metadata: owned(sessions.UID)})
	require.NoError(t, err)

	t.Run("Story11", func(t *testing.T) {
		for _, obj := range readManifests(t, "11-evacuation/01-evacuation.yaml") {
			require.NoError(t, c.Create(ctx, obj), obj.GetName())
		}
		want := evacuationStoryStatus(t, "11-evacuation/01-status-natsclusterevacuation.yaml")

		var e js.NatsClusterEvacuation
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			if !assert.NoError(ct, c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "retire-prod-east"}, &e)) {
				return
			}
			for _, w := range want.Conditions {
				evacCondition(ct, &e, w.Type, w.Status, w.Reason)
			}
		}, 2*time.Minute, 200*time.Millisecond)
		require.Equal(t, meta.FindStatusCondition(want.Conditions, ConditionReady).Message, meta.FindStatusCondition(e.Status.Conditions, ConditionReady).Message)
		require.Equal(t, want.Pinned, e.Status.Pinned)
		require.Equal(t, want.InFlight, e.Status.InFlight)
		require.Equal(t, int32(2), e.Status.Moved)
		require.Equal(t, []js.ServerStream{{Account: p.aPub, Name: "audit"}}, e.Status.StalePlacement)
		require.Len(t, want.StalePlacement, 1)
		require.Equal(t, want.StalePlacement[0].Name, e.Status.StalePlacement[0].Name)
		require.Equal(t, e.Generation, e.Status.ObservedGeneration)
		require.Contains(t, e.Finalizers, lifecycle.Finalizer)
	})
}

func evacuationStoryStatus(t *testing.T, file string) js.NatsClusterEvacuationStatus {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(storiesDir, file))
	require.NoError(t, err)
	raw, err = e2e.StripPlaceholders(raw)
	require.NoError(t, err)
	var doc struct {
		Status js.NatsClusterEvacuationStatus `json:"status"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &doc))
	require.NotEmpty(t, doc.Status.Conditions)
	return doc.Status
}
