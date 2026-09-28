package streamctl

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

const storiesDir = "../../docs/content/docs/stories"

// envResync is the resync period the envtest manager runs with.
const envResync = 2 * time.Second

// TestEnvtest runs the jetstream-controller's reconcilers in a manager
// against a real API server over the manifests of stories 1, 3 and 8, each
// against in-process NATS clusters. Only the NatsConnections' servers are
// rewritten to reach them.
func TestEnvtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	quickstart := startNATS(t, 3, false)
	unmanaged := startNATS(t, 3, true)
	supercluster := startSupercluster(t, "east", "west")

	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	startManager(t, cfg)
	c, err := client.New(cfg, client.Options{Scheme: testScheme(t)})
	require.NoError(t, err)
	for _, ns := range []string{"nats-system", "payments"} {
		require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	}

	t.Run("Story1", func(t *testing.T) { testStory1(t, c, quickstart) })
	t.Run("Story3", func(t *testing.T) { testStory3(t, c, unmanaged) })
	t.Run("Story8", func(t *testing.T) { testStory8(t, c, supercluster) })
	t.Run("ConnectionGone", func(t *testing.T) { testConnectionGone(t, c, quickstart) })
}

// testConnectionGone deletes a NatsConnection before the resources that
// name it, each under deletionPolicy Delete, and sees every resource go
// while its server object stays.
func testConnectionGone(t *testing.T, c client.Client, n *testNATS) {
	const ns = "teardown"
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	conn := &natsv1beta1.NatsConnection{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "gone"},
		Spec:       natsv1beta1.NatsConnectionSpec{Servers: n.urls},
	}
	ref := natsv1beta1.ObjectReference{Name: conn.Name}
	policies := js.Policies{AdoptionPolicy: js.AdoptionNever, TerminalPolicy: js.TerminalHold}
	objs := []client.Object{
		&js.NatsStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "teardown"},
			Spec: js.NatsStreamSpec{ConnectionRef: ref, Policies: policies, DeletionPolicy: js.DeletionDelete,
				StreamConfig: js.StreamConfig{Name: "TEARDOWN", Subjects: []string{"teardown.>"}}},
		},
		&js.NatsConsumer{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "teardown"},
			Spec: js.NatsConsumerSpec{ConnectionRef: &ref, Stream: "TEARDOWN", Policies: policies, DeletionPolicy: js.DeletionDelete,
				ConsumerConfig: js.ConsumerConfig{Name: "teardown"}},
		},
		&js.NatsKeyValue{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "teardown"},
			Spec:       js.NatsKeyValueSpec{ConnectionRef: ref, Policies: policies, DeletionPolicy: js.DeletionDelete},
		},
		&js.NatsObjectStore{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "teardown"},
			Spec:       js.NatsObjectStoreSpec{ConnectionRef: ref, Policies: policies, DeletionPolicy: js.DeletionDelete},
		},
	}
	require.NoError(t, c.Create(t.Context(), conn))
	for _, o := range objs {
		require.NoError(t, c.Create(t.Context(), o), "%T", o)
	}
	for _, o := range objs {
		eventually(t, c, o, func(ct *assert.CollectT) {
			st, err := syncStatus(o)
			if assert.NoError(ct, err) {
				assert.True(ct, meta.IsStatusConditionTrue(st.Conditions, lifecycle.ConditionReady), "%T %+v", o, st.Conditions)
			}
		})
	}

	require.NoError(t, c.Delete(t.Context(), conn))
	require.Eventually(t, func() bool {
		return apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(conn), conn))
	}, 30*time.Second, 200*time.Millisecond)
	for _, o := range objs {
		require.NoError(t, c.Delete(t.Context(), o), "%T", o)
	}
	for _, o := range objs {
		require.Eventually(t, func() bool {
			return apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(o), o))
		}, 30*time.Second, 200*time.Millisecond, "%T holds its finalizer", o)
	}

	j := n.connect(t)
	streamInfo(t, j, "TEARDOWN")
	_, err := j.Consumer(t.Context(), "TEARDOWN", "teardown")
	require.NoError(t, err)
	streamInfo(t, j, "KV_teardown")
	streamInfo(t, j, "OBJ_teardown")
}

// syncStatus returns the SyncStatus of a JetStream object resource.
func syncStatus(o client.Object) (*js.SyncStatus, error) {
	switch o := o.(type) {
	case *js.NatsStream:
		return &o.Status.SyncStatus, nil
	case *js.NatsConsumer:
		return &o.Status.SyncStatus, nil
	case *js.NatsKeyValue:
		return &o.Status.SyncStatus, nil
	case *js.NatsObjectStore:
		return &o.Status.SyncStatus, nil
	}
	return nil, fmt.Errorf("%T is not a JetStream object resource", o)
}

func startManager(t *testing.T, cfg *rest.Config) {
	t.Helper()
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: testScheme(t), Metrics: metricsserver.Options{BindAddress: "0"}})
	require.NoError(t, err)
	pool := natsconn.NewPool()
	require.NoError(t, mgr.Add(pool))
	require.NoError(t, (&natsconn.Reconciler{Client: mgr.GetClient(), Pool: pool}).SetupWithManager(t.Context(), mgr))
	dialer := &natsconn.Dialer{Reader: mgr.GetClient(), Pool: pool}
	syncer := lifecycle.Syncer{Resync: envResync}
	require.NoError(t, (&StreamReconciler{Client: mgr.GetClient(), Dialer: dialer, Syncer: syncer}).SetupWithManager(t.Context(), mgr))
	require.NoError(t, (&ConsumerReconciler{Client: mgr.GetClient(), Dialer: dialer, Syncer: syncer}).SetupWithManager(t.Context(), mgr))
	require.NoError(t, (&KeyValueReconciler{Client: mgr.GetClient(), Dialer: dialer, Syncer: syncer}).SetupWithManager(t.Context(), mgr))
	require.NoError(t, (&ObjectStoreReconciler{Client: mgr.GetClient(), Dialer: dialer, Syncer: syncer}).SetupWithManager(t.Context(), mgr))
	go func() { _ = mgr.Start(t.Context()) }()
}

func testStory1(t *testing.T, c client.Client, n *testNATS) {
	applyStory(t, c, n, "01-quickstart/03-natsconnection.yaml", "01-quickstart/03-natsstream.yaml")

	s := eventuallyStream(t, c, "nats-system", "orders", func(ct *assert.CollectT, s *js.NatsStream) {
		assertConditions(ct, s.Status.Conditions, "01-quickstart/03-status-natsstream.yaml")
		if assert.NotNil(ct, s.Status.Server) {
			assert.NotEmpty(ct, s.Status.Server.Leader)
			assert.Len(ct, s.Status.Server.Replicas, 2)
		}
	})
	require.Equal(t, &js.Ownership{Origin: js.OwnershipCreated, UID: s.UID}, s.Status.Ownership)
	require.NotNil(t, s.Status.LastSyncedTime)
	require.NotNil(t, s.Status.Server.Created)

	info := streamInfo(t, n.connect(t), "ORDERS")
	require.Equal(t, 3, info.Config.Replicas)
	require.EqualValues(t, 5<<30, info.Config.MaxBytes)
	require.Equal(t, 72*time.Hour, info.Config.MaxAge)
}

func testStory3(t *testing.T, c client.Client, n *testNATS) {
	j := n.connect(t)
	for _, cfg := range []jetstream.StreamConfig{
		{Name: "PAYMENTS", Subjects: []string{"payments.>"}, Replicas: 3, MaxAge: 168 * time.Hour, MaxBytes: -1},
		{Name: "LEDGER", Subjects: []string{"ledger.>"}, Replicas: 3},
	} {
		_, err := j.CreateStream(t.Context(), cfg)
		require.NoError(t, err)
	}
	for _, s := range []*corev1.Secret{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "messaging-nats-ca"}, Data: map[string][]byte{"ca.crt": n.ca}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "payments-secrets"}, Data: map[string][]byte{"nats.creds": n.creds}},
	} {
		require.NoError(t, c.Create(t.Context(), s))
	}
	applyStory(t, c, n, "03-unmanaged/01-natsconnection.yaml", "03-unmanaged/01-natsstreams.yaml", "03-unmanaged/01-natsconsumer.yaml",
		"03-unmanaged/01-natskeyvalue-objectstore.yaml")

	t.Run("Adopt", func(t *testing.T) {
		s := eventuallyStream(t, c, "payments", "payments", func(ct *assert.CollectT, s *js.NatsStream) {
			assertConditions(ct, s.Status.Conditions, "03-unmanaged/01-status-natsstream-payments.yaml")
		})
		require.Equal(t, &js.Ownership{Origin: js.OwnershipAdopted, UID: s.UID}, s.Status.Ownership)
		requireSpecHolds(t, s, "03-unmanaged/01-live-natsstream-payments.yaml")
		require.Equal(t, string(s.UID), streamInfo(t, j, "PAYMENTS").Config.Metadata[lifecycle.OwnerKey])
	})

	t.Run("AdoptOrCreate", func(t *testing.T) {
		s := eventuallyStream(t, c, "payments", "refunds", func(ct *assert.CollectT, s *js.NatsStream) {
			assert.True(ct, meta.IsStatusConditionTrue(s.Status.Conditions, lifecycle.ConditionReady))
		})
		require.Equal(t, js.OwnershipCreated, s.Status.Ownership.Origin)
		require.Equal(t, js.RetentionLimits, *s.Spec.Retention, "omitted fields are late-initialized")
		require.Equal(t, 2160*time.Hour, streamInfo(t, j, "REFUNDS").Config.MaxAge)
	})

	t.Run("Consumers", func(t *testing.T) {
		for _, name := range []string{"ledger-audit", "payments-settlement"} {
			eventuallyConsumer(t, c, name, func(ct *assert.CollectT, cons *js.NatsConsumer) {
				assert.True(ct, meta.IsStatusConditionTrue(cons.Status.Conditions, lifecycle.ConditionReady), "%+v", cons.Status.Conditions)
			})
		}
		settlement, err := j.Consumer(t.Context(), "PAYMENTS", "settlement")
		require.NoError(t, err)
		require.Equal(t, []string{"payments.settled.>"}, settlement.CachedInfo().Config.FilterSubjects)
		_, err = j.Consumer(t.Context(), "LEDGER", "audit")
		require.NoError(t, err, "a consumer names a stream with no resource by its server-side name")
	})

	t.Run("TerminalRetry", func(t *testing.T) {
		s := eventuallyStream(t, c, "payments", "ledger", func(ct *assert.CollectT, s *js.NatsStream) {
			assertConditions(ct, s.Status.Conditions, "03-unmanaged/01-status-natsstream-ledger.yaml")
		})
		require.NotNil(t, s.Status.NextCheckTime, "Retry schedules a recheck")
		require.NoError(t, j.DeleteStream(t.Context(), "LEDGER"))

		s = eventuallyStream(t, c, "payments", "ledger", func(ct *assert.CollectT, s *js.NatsStream) {
			assert.True(ct, meta.IsStatusConditionTrue(s.Status.Conditions, lifecycle.ConditionReady), "%+v", s.Status.Conditions)
		})
		require.Nil(t, meta.FindStatusCondition(s.Status.Conditions, lifecycle.ConditionTerminal))
		require.Equal(t, string(s.UID), streamInfo(t, j, "LEDGER").Config.Metadata[lifecycle.OwnerKey])
		require.Eventually(t, func() bool {
			_, err := j.Consumer(t.Context(), "LEDGER", "audit")
			return err == nil
		}, 30*time.Second, 200*time.Millisecond, "the resync recreates the consumer the stream took with it")
	})

	t.Run("KeyValueAndObjectStore", func(t *testing.T) { testStory3Buckets(t, c, j) })

	t.Run("Deletion", func(t *testing.T) {
		require.NoError(t, c.Delete(t.Context(), &js.NatsConsumer{ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "payments-settlement"}}))
		require.NoError(t, c.Delete(t.Context(), &js.NatsStream{ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "refunds"}}))
		require.Eventually(t, func() bool {
			var cons js.NatsConsumer
			var s js.NatsStream
			return client.IgnoreNotFound(c.Get(t.Context(), types.NamespacedName{Namespace: "payments", Name: "payments-settlement"}, &cons)) == nil &&
				client.IgnoreNotFound(c.Get(t.Context(), types.NamespacedName{Namespace: "payments", Name: "refunds"}, &s)) == nil &&
				cons.Name == "" && s.Name == ""
		}, 30*time.Second, 200*time.Millisecond)
		_, err := j.Consumer(t.Context(), "PAYMENTS", "settlement")
		require.ErrorIs(t, err, jetstream.ErrConsumerNotFound, "consumers default to Delete")
		streamInfo(t, j, "REFUNDS")
	})
}

// testStory3Buckets checks story 3's bucket and object store: both created
// from their manifests, and both kept on the server when their resources
// are deleted under the default Retain.
func testStory3Buckets(t *testing.T, c client.Client, j jetstream.JetStream) {
	kv := &js.NatsKeyValue{ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "sessions"}}
	eventually(t, c, kv, func(ct *assert.CollectT) {
		assert.True(ct, meta.IsStatusConditionTrue(kv.Status.Conditions, lifecycle.ConditionReady), "%+v", kv.Status.Conditions)
		assert.True(ct, meta.IsStatusConditionTrue(kv.Status.Conditions, lifecycle.ConditionSynced), "%+v", kv.Status.Conditions)
	})
	require.Equal(t, &js.Ownership{Origin: js.OwnershipCreated, UID: kv.UID}, kv.Status.Ownership)
	require.Len(t, kv.Status.Server.Replicas, 2)
	bucket, err := j.KeyValue(t.Context(), "sessions")
	require.NoError(t, err)
	st, err := bucket.Status(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 5, st.History())
	require.Equal(t, 24*time.Hour, st.TTL())
	cfg := streamInfo(t, j, "KV_sessions").Config
	require.Equal(t, 3, cfg.Replicas)
	require.EqualValues(t, 64<<10, cfg.MaxMsgSize)
	require.EqualValues(t, 1<<30, cfg.MaxBytes)
	require.Equal(t, string(kv.UID), cfg.Metadata[lifecycle.OwnerKey])

	os := &js.NatsObjectStore{ObjectMeta: metav1.ObjectMeta{Namespace: "payments", Name: "receipts"}}
	eventually(t, c, os, func(ct *assert.CollectT) {
		assert.True(ct, meta.IsStatusConditionTrue(os.Status.Conditions, lifecycle.ConditionReady), "%+v", os.Status.Conditions)
	})
	require.Equal(t, js.DeletionRetain, os.Spec.DeletionPolicy, "Retain is the default")
	cfg = streamInfo(t, j, "OBJ_receipts").Config
	require.Equal(t, 2160*time.Hour, cfg.MaxAge)
	require.EqualValues(t, 50<<30, cfg.MaxBytes)
	require.Equal(t, jetstream.S2Compression, cfg.Compression)
	require.Equal(t, string(os.UID), cfg.Metadata[lifecycle.OwnerKey])

	require.NoError(t, c.Delete(t.Context(), kv))
	require.NoError(t, c.Delete(t.Context(), os))
	require.Eventually(t, func() bool {
		return apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(kv), kv)) &&
			apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(os), os))
	}, 30*time.Second, 200*time.Millisecond)
	streamInfo(t, j, "KV_sessions")
	streamInfo(t, j, "OBJ_receipts")
}

// eventually reads obj again until check passes on it.
func eventually(t *testing.T, c client.Client, obj client.Object, check func(*assert.CollectT)) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		if assert.NoError(ct, c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj)) {
			check(ct)
		}
	}, 60*time.Second, 200*time.Millisecond)
}

// applyStory creates every object in files, pointing each NatsConnection at
// n.
func applyStory(t *testing.T, c client.Client, n *testNATS, files ...string) {
	t.Helper()
	for _, f := range files {
		for _, obj := range readManifests(t, f) {
			if obj.GetKind() == "NatsConnection" {
				urls := make([]any, len(n.urls))
				for i, u := range n.urls {
					urls[i] = u
				}
				require.NoError(t, unstructured.SetNestedSlice(obj.Object, urls, "spec", "servers"))
			}
			require.NoError(t, c.Create(t.Context(), obj), "%s %s", f, obj.GetName())
		}
	}
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

// assertConditions asserts that conds carry every condition the story's
// status file shows, with its status, reason and, where shown, message.
func assertConditions(ct *assert.CollectT, conds []metav1.Condition, statusFile string) {
	raw, err := os.ReadFile(filepath.Join(storiesDir, statusFile))
	if !assert.NoError(ct, err) {
		return
	}
	var doc struct {
		Status struct {
			Conditions []metav1.Condition `json:"conditions"`
		} `json:"status"`
	}
	if !assert.NoError(ct, yaml.Unmarshal(raw, &doc)) {
		return
	}
	for _, want := range doc.Status.Conditions {
		got := meta.FindStatusCondition(conds, want.Type)
		if !assert.NotNil(ct, got, "no %s condition", want.Type) {
			continue
		}
		assert.Equal(ct, want.Status, got.Status, want.Type)
		assert.Equal(ct, want.Reason, got.Reason, want.Type)
		if want.Message != "" {
			assert.Equal(ct, want.Message, got.Message, want.Type)
		}
	}
}

// requireSpecHolds requires s's spec to carry every field the story's
// manifest shows.
func requireSpecHolds(t *testing.T, s *js.NatsStream, file string) {
	t.Helper()
	want := readManifests(t, file)[0]
	var gotSpec js.NatsStreamSpec
	require.NoError(t, runtimeConvert(want.Object["spec"], &gotSpec))
	got, err := toMap(s.Spec)
	require.NoError(t, err)
	wantMap, err := toMap(gotSpec)
	require.NoError(t, err)
	for k, v := range wantMap {
		require.Equal(t, v, got[k], "spec.%s", k)
	}
}

func runtimeConvert(in any, out any) error {
	raw, err := yaml.Marshal(in)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(raw, out)
}

func toMap(v any) (map[string]any, error) {
	var m map[string]any
	return m, runtimeConvert(v, &m)
}

func eventuallyStream(t *testing.T, c client.Client, ns, name string, check func(*assert.CollectT, *js.NatsStream)) *js.NatsStream {
	t.Helper()
	var s js.NatsStream
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		if assert.NoError(ct, c.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: name}, &s)) {
			check(ct, &s)
		}
	}, 60*time.Second, 200*time.Millisecond)
	return &s
}

func eventuallyConsumer(t *testing.T, c client.Client, name string, check func(*assert.CollectT, *js.NatsConsumer)) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		var cons js.NatsConsumer
		if assert.NoError(ct, c.Get(t.Context(), types.NamespacedName{Namespace: "payments", Name: name}, &cons)) {
			check(ct, &cons)
		}
	}, 60*time.Second, 200*time.Millisecond)
}

func streamInfo(t *testing.T, j jetstream.JetStream, name string) *jetstream.StreamInfo {
	t.Helper()
	s, err := j.Stream(t.Context(), name)
	require.NoError(t, err)
	return s.CachedInfo()
}
