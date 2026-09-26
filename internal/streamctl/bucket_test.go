package streamctl

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/lifecycle"
)

func newKeyValue(name string, mutate func(*js.NatsKeyValueSpec)) *js.NatsKeyValue {
	kv := &js.NatsKeyValue{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name, UID: uuid.NewUUID(), Generation: 1},
		Spec: js.NatsKeyValueSpec{
			ConnectionRef:  demo,
			Policies:       js.Policies{AdoptionPolicy: js.AdoptionNever, TerminalPolicy: js.TerminalHold},
			DeletionPolicy: js.DeletionRetain,
		},
	}
	if mutate != nil {
		mutate(&kv.Spec)
	}
	return kv
}

func newObjectStore(name string, mutate func(*js.NatsObjectStoreSpec)) *js.NatsObjectStore {
	os := &js.NatsObjectStore{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name, UID: uuid.NewUUID(), Generation: 1},
		Spec: js.NatsObjectStoreSpec{
			ConnectionRef:  demo,
			Policies:       js.Policies{AdoptionPolicy: js.AdoptionNever, TerminalPolicy: js.TerminalHold},
			DeletionPolicy: js.DeletionRetain,
		},
	}
	if mutate != nil {
		mutate(&os.Spec)
	}
	return os
}

func (f *fixture) reconcile(r reconcile.Reconciler, name string) reconcile.Result {
	f.t.Helper()
	res, err := r.Reconcile(f.t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: name}})
	require.NoError(f.t, err)
	return res
}

func (f *fixture) keyValue(name string) *js.NatsKeyValue {
	f.t.Helper()
	var kv js.NatsKeyValue
	require.NoError(f.t, f.c.Get(f.t.Context(), types.NamespacedName{Namespace: testNamespace, Name: name}, &kv))
	return &kv
}

func (f *fixture) objectStore(name string) *js.NatsObjectStore {
	f.t.Helper()
	var os js.NatsObjectStore
	require.NoError(f.t, f.c.Get(f.t.Context(), types.NamespacedName{Namespace: testNamespace, Name: name}, &os))
	return &os
}

// requireInSync requires conds to show a resource whose server object
// matches its spec, which a reconcile that found drift would not show.
func requireInSync(t *testing.T, conds []metav1.Condition) {
	t.Helper()
	condition(t, conds, lifecycle.ConditionReady, metav1.ConditionTrue, lifecycle.ReasonSynced)
	condition(t, conds, lifecycle.ConditionSynced, metav1.ConditionTrue, lifecycle.ReasonMatchesSpec)
	require.Nil(t, meta.FindStatusCondition(conds, lifecycle.ConditionTerminal))
}

func TestKeyValueCreate(t *testing.T) {
	f := newFixture(t)
	f.create(newKeyValue("sessions", func(s *js.NatsKeyValueSpec) {
		s.History = ptrTo(int32(5))
		s.TTL = &metav1.Duration{Duration: 24 * time.Hour}
		s.MaxValueSize = ptrTo(resource.MustParse("64Ki"))
		s.MaxBytes = ptrTo(resource.MustParse("1Gi"))
		s.Storage = ptrTo(js.StorageFile)
		s.Replicas = ptrTo(int32(1))
		s.Compression = ptrTo(true)
		s.Metadata = map[string]string{"team": "payments"}
	}))

	require.Equal(t, testResync, f.reconcile(f.kvs, "sessions").RequeueAfter)
	kv := f.keyValue("sessions")
	require.Contains(t, kv.Finalizers, lifecycle.Finalizer)
	requireInSync(t, kv.Status.Conditions)
	require.Equal(t, &js.Ownership{Origin: js.OwnershipCreated, UID: kv.UID}, kv.Status.Ownership)
	require.NotNil(t, kv.Status.Server)
	require.NotNil(t, kv.Status.Server.Created)

	bucket, err := f.js.KeyValue(t.Context(), "sessions")
	require.NoError(t, err, "the bucket is named by metadata.name")
	st, err := bucket.Status(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 5, st.History())
	require.Equal(t, 24*time.Hour, st.TTL())
	require.True(t, st.IsCompressed())
	cfg := f.serverStream("KV_sessions")
	require.EqualValues(t, 64<<10, cfg.MaxMsgSize)
	require.EqualValues(t, 1<<30, cfg.MaxBytes)
	require.Equal(t, string(kv.UID), cfg.Metadata[lifecycle.OwnerKey], "the marker is in the bucket's stream")
	require.Equal(t, "Created", cfg.Metadata[lifecycle.OriginKey])
	require.Equal(t, "payments", cfg.Metadata["team"])

	f.now = f.now.Add(testResync)
	f.reconcile(f.kvs, "sessions")
	requireInSync(t, f.keyValue("sessions").Status.Conditions)
}

// TestKeyValueReadBack pins that each config nats.go builds a bucket's
// stream from reads back from that stream as itself, so a resync finds no
// drift where there is none.
func TestKeyValueReadBack(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*js.NatsKeyValueSpec)
	}{
		{"defaults", nil},
		{"description and limits", func(s *js.NatsKeyValueSpec) {
			s.Description = "sessions"
			s.MaxBytes = ptrTo(resource.MustParse("1Mi"))
			s.TTL = &metav1.Duration{Duration: time.Minute}
			s.Storage = ptrTo(js.StorageMemory)
		}},
		{"limit marker TTL", func(s *js.NatsKeyValueSpec) {
			s.LimitMarkerTTL = &metav1.Duration{Duration: time.Minute}
		}},
		{"republish", func(s *js.NatsKeyValueSpec) {
			s.Republish = &js.Republish{Source: "$KV.copy.>", Destination: "repub.>"}
		}},
		{"mirror by bucket", func(s *js.NatsKeyValueSpec) {
			s.Mirror = &js.StreamSource{Name: "origin"}
		}},
		{"mirror by stream", func(s *js.NatsKeyValueSpec) {
			s.Mirror = &js.StreamSource{Name: "KV_origin"}
		}},
		{"sources", func(s *js.NatsKeyValueSpec) {
			s.Sources = []js.StreamSource{{Name: "origin"}, {Name: "KV_other"}}
		}},
		{"source with transforms", func(s *js.NatsKeyValueSpec) {
			s.Sources = []js.StreamSource{{Name: "ORDERS", SubjectTransforms: []js.SubjectTransform{{Source: "orders.>", Destination: "$KV.copy.>"}}}}
		}},
		{"external source of the same name", func(s *js.NatsKeyValueSpec) {
			s.Sources = []js.StreamSource{{Name: "copy", External: &js.ExternalStream{APIPrefix: "$JS.hub.API"}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.create(newKeyValue("copy", tc.mutate))
			f.reconcile(f.kvs, "copy")
			requireInSync(t, f.keyValue("copy").Status.Conditions)
			f.now = f.now.Add(testResync)
			f.reconcile(f.kvs, "copy")
			requireInSync(t, f.keyValue("copy").Status.Conditions)
		})
	}
}

func TestKeyValueDrift(t *testing.T) {
	f := newFixture(t)
	f.create(newKeyValue("sessions", func(s *js.NatsKeyValueSpec) { s.History = ptrTo(int32(5)) }))
	f.reconcile(f.kvs, "sessions")

	cfg := f.serverStream("KV_sessions")
	cfg.MaxMsgsPerSubject = 2
	_, err := f.js.UpdateStream(t.Context(), *cfg)
	require.NoError(t, err)

	f.reconcile(f.kvs, "sessions")
	synced := condition(t, f.keyValue("sessions").Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionFalse, lifecycle.ReasonDriftCorrected)
	require.Equal(t, "reapplied spec over drift in history", synced.Message)
	require.EqualValues(t, 5, f.serverStream("KV_sessions").MaxMsgsPerSubject)

	f.reconcile(f.kvs, "sessions")
	requireInSync(t, f.keyValue("sessions").Status.Conditions)
}

func TestKeyValueAdoption(t *testing.T) {
	for _, tc := range []struct {
		name    string
		policy  js.AdoptionPolicy
		history *int32
		want    int64
		message string
	}{
		{"adopt", js.AdoptionAdopt, nil, 3, "adopted existing key-value bucket sessions; spec written from the server"},
		{"adopt or create", js.AdoptionAdoptOrCreate, ptrTo(int32(7)), 7, "adopted existing key-value bucket sessions; spec applied over the server config"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			_, err := f.js.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{Bucket: "sessions", History: 3, TTL: time.Hour})
			require.NoError(t, err)
			f.create(newKeyValue("sessions", func(s *js.NatsKeyValueSpec) {
				s.AdoptionPolicy = tc.policy
				s.History = tc.history
			}))

			f.reconcile(f.kvs, "sessions")
			kv := f.keyValue("sessions")
			adopted := condition(t, kv.Status.Conditions, lifecycle.ConditionAdopted, metav1.ConditionTrue, lifecycle.ReasonFoundUnowned)
			require.Equal(t, tc.message, adopted.Message)
			requireInSync(t, kv.Status.Conditions)
			require.Equal(t, &js.Ownership{Origin: js.OwnershipAdopted, UID: kv.UID}, kv.Status.Ownership)
			require.EqualValues(t, tc.want, *kv.Spec.History)
			require.Equal(t, time.Hour, kv.Spec.TTL.Duration, "omitted fields are written from the server")
			require.Equal(t, js.StorageFile, *kv.Spec.Storage)
			require.Empty(t, kv.Spec.Name, "the bucket name stays as declared")

			cfg := f.serverStream("KV_sessions")
			require.Equal(t, tc.want, cfg.MaxMsgsPerSubject)
			require.Equal(t, string(kv.UID), cfg.Metadata[lifecycle.OwnerKey])
			require.Equal(t, "Adopted", cfg.Metadata[lifecycle.OriginKey])
		})
	}
}

func TestKeyValueTerminal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing func(*fixture)
		mutate   func(*js.NatsKeyValueSpec)
		reason   string
		message  string
	}{
		{
			name: "exists unowned",
			existing: func(f *fixture) {
				_, err := f.js.CreateKeyValue(f.t.Context(), jetstream.KeyValueConfig{Bucket: "sessions"})
				require.NoError(f.t, err)
			},
			reason:  lifecycle.ReasonExistsUnowned,
			message: "key-value bucket sessions exists and carries no ownership marker; set spec.adoptionPolicy to Adopt or AdoptOrCreate to take it over",
		},
		{
			name: "not a bucket",
			existing: func(f *fixture) {
				_, err := f.js.CreateStream(f.t.Context(), jetstream.StreamConfig{Name: "KV_sessions", Subjects: []string{"sessions.>"}})
				require.NoError(f.t, err)
			},
			mutate:  func(s *js.NatsKeyValueSpec) { s.AdoptionPolicy = js.AdoptionAdoptOrCreate },
			reason:  ReasonNotABucket,
			message: "stream KV_sessions exists and is not a key-value bucket",
		},
		{
			name: "rejected by the server",
			mutate: func(s *js.NatsKeyValueSpec) {
				s.Republish = &js.Republish{Source: ">", Destination: "$KV.sessions.>"}
			},
			reason:  lifecycle.ReasonRejected,
			message: "stream configuration for republish destination forms a cycle",
		},
		{
			name:    "invalid bucket name",
			mutate:  func(s *js.NatsKeyValueSpec) { s.Name = "sessions.v2" },
			reason:  lifecycle.ReasonRejected,
			message: `bucket name "sessions.v2" is not letters, digits, '-' and '_'`,
		},
		{
			name:    "preferred leader",
			mutate:  func(s *js.NatsKeyValueSpec) { s.Placement = &js.Placement{Preferred: "n1"} },
			reason:  lifecycle.ReasonRejected,
			message: "placement.preferred cannot be set on a bucket: nats.go's bucket managers do not carry it",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			if tc.existing != nil {
				tc.existing(f)
			}
			f.create(newKeyValue("sessions", tc.mutate))
			require.Zero(t, f.reconcile(f.kvs, "sessions").RequeueAfter, "Hold waits for an edit")
			kv := f.keyValue("sessions")
			term := condition(t, kv.Status.Conditions, lifecycle.ConditionTerminal, metav1.ConditionTrue, tc.reason)
			require.Equal(t, tc.message, term.Message)
			condition(t, kv.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionFalse, lifecycle.ReasonTerminal)
			require.Nil(t, kv.Status.Ownership)
		})
	}
}

func TestBucketDeletion(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy js.DeletionPolicy
		owned  bool
		kept   bool
	}{
		{"retain", js.DeletionRetain, true, true},
		{"delete", js.DeletionDelete, true, false},
		{"delete unowned", js.DeletionDelete, false, true},
	} {
		t.Run("KeyValue/"+tc.name, func(t *testing.T) {
			f := newFixture(t)
			kv := newKeyValue("sessions", func(s *js.NatsKeyValueSpec) { s.DeletionPolicy = tc.policy })
			f.create(kv)
			f.reconcile(f.kvs, "sessions")
			if !tc.owned {
				f.remark("KV_sessions")
			}
			require.NoError(t, f.c.Delete(t.Context(), f.keyValue("sessions")))
			f.reconcile(f.kvs, "sessions")
			require.True(t, f.gone(kv), "the finalizer is removed")
			require.Equal(t, tc.kept, f.serverStream("KV_sessions") != nil)
		})
		t.Run("ObjectStore/"+tc.name, func(t *testing.T) {
			f := newFixture(t)
			os := newObjectStore("receipts", func(s *js.NatsObjectStoreSpec) { s.DeletionPolicy = tc.policy })
			f.create(os)
			f.reconcile(f.stores, "receipts")
			if !tc.owned {
				f.remark("OBJ_receipts")
			}
			require.NoError(t, f.c.Delete(t.Context(), f.objectStore("receipts")))
			f.reconcile(f.stores, "receipts")
			require.True(t, f.gone(os), "the finalizer is removed")
			require.Equal(t, tc.kept, f.serverStream("OBJ_receipts") != nil)
		})
	}
}

// remark gives stream the marker of a resource other than the fixture's.
func (f *fixture) remark(stream string) {
	f.t.Helper()
	cfg := f.serverStream(stream)
	cfg.Metadata = map[string]string{lifecycle.OwnerKey: "someone-else"}
	_, err := f.js.UpdateStream(f.t.Context(), *cfg)
	require.NoError(f.t, err)
}

func TestObjectStoreCreate(t *testing.T) {
	f := newFixture(t)
	f.create(newObjectStore("receipts", func(s *js.NatsObjectStoreSpec) {
		s.Name = "RECEIPTS"
		s.Description = "receipts"
		s.TTL = &metav1.Duration{Duration: 2160 * time.Hour}
		s.MaxBytes = ptrTo(resource.MustParse("1Gi"))
		s.Storage = ptrTo(js.StorageFile)
		s.Replicas = ptrTo(int32(1))
		s.Compression = ptrTo(true)
	}))

	f.reconcile(f.stores, "receipts")
	os := f.objectStore("receipts")
	requireInSync(t, os.Status.Conditions)
	require.Equal(t, &js.Ownership{Origin: js.OwnershipCreated, UID: os.UID}, os.Status.Ownership)
	require.NotNil(t, os.Status.Server)

	store, err := f.js.ObjectStore(t.Context(), "RECEIPTS")
	require.NoError(t, err)
	st, err := store.Status(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2160*time.Hour, st.TTL())
	require.Equal(t, "receipts", st.Description())
	require.True(t, st.IsCompressed())
	cfg := f.serverStream("OBJ_RECEIPTS")
	require.EqualValues(t, 1<<30, cfg.MaxBytes)
	require.Equal(t, string(os.UID), cfg.Metadata[lifecycle.OwnerKey])

	f.now = f.now.Add(testResync)
	f.reconcile(f.stores, "receipts")
	requireInSync(t, f.objectStore("receipts").Status.Conditions)
}

func TestObjectStoreDrift(t *testing.T) {
	f := newFixture(t)
	f.create(newObjectStore("invoices", func(s *js.NatsObjectStoreSpec) { s.TTL = &metav1.Duration{Duration: time.Hour} }))
	f.reconcile(f.stores, "invoices")

	cfg := f.serverStream("OBJ_invoices")
	cfg.MaxAge = 3 * time.Hour
	_, err := f.js.UpdateStream(t.Context(), *cfg)
	require.NoError(t, err)

	f.reconcile(f.stores, "invoices")
	synced := condition(t, f.objectStore("invoices").Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionFalse, lifecycle.ReasonDriftCorrected)
	require.Equal(t, "reapplied spec over drift in max_age", synced.Message)
	require.Equal(t, time.Hour, f.serverStream("OBJ_invoices").MaxAge)
}

func TestObjectStoreAdopt(t *testing.T) {
	f := newFixture(t)
	_, err := f.js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "receipts", TTL: time.Hour, Metadata: map[string]string{"team": "payments"}})
	require.NoError(t, err)
	f.create(newObjectStore("receipts", func(s *js.NatsObjectStoreSpec) { s.AdoptionPolicy = js.AdoptionAdopt }))

	f.reconcile(f.stores, "receipts")
	os := f.objectStore("receipts")
	condition(t, os.Status.Conditions, lifecycle.ConditionAdopted, metav1.ConditionTrue, lifecycle.ReasonFoundUnowned)
	requireInSync(t, os.Status.Conditions)
	require.Equal(t, time.Hour, os.Spec.TTL.Duration)
	require.Equal(t, map[string]string{"team": "payments"}, os.Spec.Metadata)
	cfg := f.serverStream("OBJ_receipts")
	require.Equal(t, string(os.UID), cfg.Metadata[lifecycle.OwnerKey])
	require.Equal(t, "payments", cfg.Metadata["team"])
}
