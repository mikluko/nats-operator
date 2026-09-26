package balancectl

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/e2e"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

const storiesDir = "../../docs/content/stories"

// TestEnvtest runs the reconciler in a manager against a real API server
// over story 7's system balancer, pointed at C1 of a two-cluster
// supercluster whose leaders all start on C1-0 and where one of two accounts
// carries the jetstream-stepdown export. Only the NatsConnection's servers
// are rewritten to reach it.
func TestEnvtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	p := newPlane(t)
	sc := startSupercluster(t, p, "C1", "C2")
	skewedStreams(t, t.Context(), accountJS(t, sc["C1"][1], p.a), "A", 3)
	skewedStreams(t, t.Context(), accountJS(t, sc["C1"][2], p.b), "B", 1)

	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: testScheme(t), Metrics: metricsserver.Options{BindAddress: "0"}})
	require.NoError(t, err)
	pool := natsconn.NewPool()
	require.NoError(t, mgr.Add(pool))
	r := &SystemBalancerReconciler{Client: mgr.GetClient(), Dialer: &natsconn.Dialer{Reader: mgr.GetClient(), Pool: pool}, PendingPoll: 200 * time.Millisecond}
	require.NoError(t, r.SetupWithManager(t.Context(), mgr))
	go func() { _ = mgr.Start(t.Context()) }()

	c, err := client.New(cfg, client.Options{Scheme: testScheme(t)})
	require.NoError(t, err)
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	require.NoError(t, c.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "jetstream-controller-creds"},
		Data:       map[string][]byte{natsconn.DefaultCredentialsKey: p.sysCreds},
	}))

	t.Run("Story7", func(t *testing.T) {
		for _, obj := range readManifests(t, "07-balancing/01-system.yaml") {
			if obj.GetKind() == "NatsConnection" {
				require.NoError(t, unstructured.SetNestedStringSlice(obj.Object, []string{sc["C1"][1].ClientURL()}, "spec", "servers"))
			}
			require.NoError(t, c.Create(t.Context(), obj), obj.GetName())
		}
		want := storyStatus(t, "07-balancing/01-status-natssystembalancer.yaml")

		b := eventually(t, c, "demo", func(ct *assert.CollectT, b *js.NatsSystemBalancer) {
			for _, w := range want.Conditions {
				condition(ct, b, w.Type, w.Status, w.Reason)
			}
			assert.NotNil(ct, b.Status.LastMove)
			assert.Empty(ct, b.Status.Pending)
		})
		require.Equal(t, want.Capabilities.Placement, b.Status.Capabilities.Placement)
		require.Equal(t, want.Capabilities.Leader, b.Status.Capabilities.Leader)
		require.Equal(t, "1 of 2 accounts carry no jetstream-stepdown export; their leaders are not moved", b.Status.Capabilities.LeaderReason)
		require.Equal(t, afterCounts(want.Capabilities.LeaderReason), afterCounts(b.Status.Capabilities.LeaderReason), "the story's wording")
		require.Len(t, b.Status.Servers, 3)
		require.Equal(t, js.MoveLeader, b.Status.LastMove.Kind)
		require.Equal(t, "C1-0", b.Status.LastMove.From)
		require.Equal(t, b.Generation, b.Status.ObservedGeneration)
	})

	t.Run("ConnectionNotFound", func(t *testing.T) {
		b := balancer("lost", "missing", time.Now())
		b.UID, b.CreationTimestamp = "", metav1.Time{}
		require.NoError(t, c.Create(t.Context(), b))
		eventually(t, c, "lost", func(ct *assert.CollectT, b *js.NatsSystemBalancer) {
			condition(ct, b, ConditionReady, metav1.ConditionFalse, "ConnectionNotFound")
		})
	})
}

func eventually(t *testing.T, c client.Client, name string, check func(*assert.CollectT, *js.NatsSystemBalancer)) *js.NatsSystemBalancer {
	t.Helper()
	var b js.NatsSystemBalancer
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		if assert.NoError(ct, c.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: name}, &b)) {
			check(ct, &b)
		}
	}, time.Minute, 200*time.Millisecond)
	return &b
}

// afterCounts is a leader reason less the counts it opens with.
func afterCounts(reason string) string {
	_, rest, _ := strings.Cut(reason, " accounts ")
	return rest
}

func storyStatus(t *testing.T, file string) js.NatsSystemBalancerStatus {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(storiesDir, file))
	require.NoError(t, err)
	raw, err = e2e.StripPlaceholders(raw)
	require.NoError(t, err)
	var doc struct {
		Status js.NatsSystemBalancerStatus `json:"status"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &doc))
	require.NotNil(t, doc.Status.Capabilities)
	return doc.Status
}

func readManifests(t *testing.T, file string) []*unstructured.Unstructured {
	t.Helper()
	f, err := os.Open(filepath.Join(storiesDir, file))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	r := utilyaml.NewYAMLReader(bufio.NewReader(f))
	var out []*unstructured.Unstructured
	for {
		raw, err := r.Read()
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		var m map[string]any
		require.NoError(t, yaml.Unmarshal(raw, &m))
		if len(m) > 0 {
			out = append(out, &unstructured.Unstructured{Object: m})
		}
	}
}
