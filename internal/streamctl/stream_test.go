package streamctl

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/utils/ptr"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/lifecycle"
)

// createByHand creates a stream on the server as someone other than the
// controller would, carrying metadata.
func (f *fixture) createByHand(name string, metadata map[string]string) {
	f.t.Helper()
	_, err := f.js.CreateStream(f.t.Context(), jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{name + ".>"},
		MaxAge:   168 * time.Hour,
		MaxBytes: -1,
		Metadata: metadata,
	})
	require.NoError(f.t, err)
}

func TestStreamCreate(t *testing.T) {
	f := newFixture(t)
	f.create(newStream("orders", "ORDERS", func(s *js.NatsStreamSpec) {
		s.MaxAge = &metav1.Duration{Duration: 72 * time.Hour}
		s.MaxBytes = ptr.To(resource.MustParse("5Gi"))
		s.Storage = ptr.To(js.StorageFile)
		s.Retention = ptr.To(js.RetentionLimits)
	}))

	res := f.reconcileStream("orders")
	require.Equal(t, testResync, res.RequeueAfter, "a synced stream is rechecked on the resync period")

	s := f.stream("orders")
	require.Contains(t, s.Finalizers, lifecycle.Finalizer)
	condition(t, s.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionTrue, lifecycle.ReasonSynced)
	synced := condition(t, s.Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionTrue, lifecycle.ReasonMatchesSpec)
	require.Equal(t, "server config matches spec as of last check", synced.Message)
	require.Nil(t, meta.FindStatusCondition(s.Status.Conditions, lifecycle.ConditionTerminal))
	require.Nil(t, meta.FindStatusCondition(s.Status.Conditions, lifecycle.ConditionAdopted))
	require.Equal(t, &js.Ownership{Origin: js.OwnershipCreated, UID: s.UID}, s.Status.Ownership)
	require.EqualValues(t, 1, s.Status.ObservedGeneration)
	require.Equal(t, f.now, s.Status.LastSyncedTime.UTC())
	require.NotNil(t, s.Status.Server)
	require.NotNil(t, s.Status.Server.Created)
	require.Equal(t, js.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}, MaxAge: s.Spec.MaxAge, MaxBytes: s.Spec.MaxBytes, Storage: s.Spec.Storage, Retention: s.Spec.Retention},
		s.Spec.StreamConfig, "Never does not late-initialize")

	cfg := f.serverStream("ORDERS")
	require.NotNil(t, cfg)
	require.Equal(t, []string{"orders.>"}, cfg.Subjects)
	require.Equal(t, 72*time.Hour, cfg.MaxAge)
	require.EqualValues(t, 5<<30, cfg.MaxBytes)
	require.Equal(t, string(s.UID), cfg.Metadata[lifecycle.OwnerKey])
	require.Equal(t, "Created", cfg.Metadata[lifecycle.OriginKey])
}

func TestStreamServerNameDefaultsToMetadataName(t *testing.T) {
	f := newFixture(t)
	f.create(newStream("events", "", nil))
	f.reconcileStream("events")
	require.NotNil(t, f.serverStream("events"))
}

func TestStreamNeverExistsUnowned(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata map[string]string
		message  string
	}{
		{
			name:    "no marker",
			message: "stream LEDGER exists and carries no ownership marker; set spec.adoptionPolicy to Adopt or AdoptOrCreate to take it over",
		},
		{
			name:     "orphan marker",
			metadata: map[string]string{lifecycle.OwnerKey: "gone"},
			message:  "stream LEDGER exists and carries the marker of UID gone, which no longer exists; set spec.adoptionPolicy to Adopt or AdoptOrCreate to take it over",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.createByHand("LEDGER", tc.metadata)
			f.create(newStream("ledger", "LEDGER", func(s *js.NatsStreamSpec) { s.MaxAge = &metav1.Duration{Duration: time.Hour} }))

			res := f.reconcileStream("ledger")
			require.Zero(t, res.RequeueAfter, "Hold waits for an edit")
			s := f.stream("ledger")
			term := condition(t, s.Status.Conditions, lifecycle.ConditionTerminal, metav1.ConditionTrue, lifecycle.ReasonExistsUnowned)
			require.Equal(t, tc.message, term.Message)
			condition(t, s.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionFalse, lifecycle.ReasonTerminal)
			require.Nil(t, s.Status.NextCheckTime)
			require.Nil(t, s.Status.Ownership)
			require.Equal(t, 168*time.Hour, f.serverStream("LEDGER").MaxAge, "the server is untouched")
		})
	}
}

func TestStreamTerminalHold(t *testing.T) {
	f := newFixture(t)
	f.createByHand("LEDGER", nil)
	f.create(newStream("ledger", "LEDGER", nil))
	f.reconcileStream("ledger")

	require.NoError(t, f.js.DeleteStream(t.Context(), "LEDGER"))
	f.reconcileStream("ledger")
	condition(t, f.stream("ledger").Status.Conditions, lifecycle.ConditionTerminal, metav1.ConditionTrue, lifecycle.ReasonExistsUnowned)
	require.Nil(t, f.serverStream("LEDGER"), "Hold rechecks nothing until the resource is edited")

	f.editStream("ledger", func(s *js.NatsStreamSpec) { s.Description = "edited" })
	f.reconcileStream("ledger")
	s := f.stream("ledger")
	require.Nil(t, meta.FindStatusCondition(s.Status.Conditions, lifecycle.ConditionTerminal))
	condition(t, s.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionTrue, lifecycle.ReasonSynced)
	require.Equal(t, "edited", f.serverStream("LEDGER").Description)
}

func TestStreamTerminalRetry(t *testing.T) {
	f := newFixture(t)
	f.createByHand("LEDGER", nil)
	f.create(newStream("ledger", "LEDGER", func(s *js.NatsStreamSpec) { s.TerminalPolicy = js.TerminalRetry }))

	res := f.reconcileStream("ledger")
	require.Equal(t, testResync, res.RequeueAfter)
	s := f.stream("ledger")
	condition(t, s.Status.Conditions, lifecycle.ConditionTerminal, metav1.ConditionTrue, lifecycle.ReasonExistsUnowned)
	require.NotNil(t, s.Status.NextCheckTime)
	require.Equal(t, f.now.Add(testResync), s.Status.NextCheckTime.UTC())

	require.NoError(t, f.js.DeleteStream(t.Context(), "LEDGER"))
	f.reconcileStream("ledger")
	s = f.stream("ledger")
	require.Nil(t, meta.FindStatusCondition(s.Status.Conditions, lifecycle.ConditionTerminal), "Retry clears once the cause is gone")
	require.Nil(t, s.Status.NextCheckTime)
	condition(t, s.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionTrue, lifecycle.ReasonSynced)
	require.Equal(t, string(s.UID), f.serverStream("LEDGER").Metadata[lifecycle.OwnerKey])
}

func TestStreamAdopt(t *testing.T) {
	f := newFixture(t)
	f.create(newStream("payments", "PAYMENTS", func(s *js.NatsStreamSpec) {
		s.AdoptionPolicy = js.AdoptionAdopt
		s.Subjects = nil
		s.MaxAge = &metav1.Duration{Duration: time.Minute}
	}))

	res := f.reconcileStream("payments")
	require.Equal(t, testResync, res.RequeueAfter, "Adopt waits for the stream")
	s := f.stream("payments")
	condition(t, s.Status.Conditions, lifecycle.ConditionAdopted, metav1.ConditionFalse, lifecycle.ReasonNotFound)
	condition(t, s.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionFalse, lifecycle.ReasonNotFound)
	require.Nil(t, f.serverStream("PAYMENTS"), "Adopt never creates")

	f.createByHand("PAYMENTS", map[string]string{"team": "payments"})
	f.reconcileStream("payments")
	s = f.stream("payments")
	adopted := condition(t, s.Status.Conditions, lifecycle.ConditionAdopted, metav1.ConditionTrue, lifecycle.ReasonFoundUnowned)
	require.Equal(t, "adopted existing stream PAYMENTS; spec written from the server", adopted.Message)
	condition(t, s.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionTrue, lifecycle.ReasonSynced)
	condition(t, s.Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionTrue, lifecycle.ReasonMatchesSpec)
	require.Equal(t, &js.Ownership{Origin: js.OwnershipAdopted, UID: s.UID}, s.Status.Ownership)

	require.Equal(t, "PAYMENTS", s.Spec.Name)
	require.Equal(t, []string{"PAYMENTS.>"}, s.Spec.Subjects)
	require.Equal(t, 168*time.Hour, s.Spec.MaxAge.Duration, "the server's config replaces what spec set")
	require.Equal(t, "-1", s.Spec.MaxBytes.String())
	require.Equal(t, js.StorageFile, *s.Spec.Storage)
	require.Equal(t, js.RetentionLimits, *s.Spec.Retention)
	require.Equal(t, js.DiscardOld, *s.Spec.Discard)
	require.Nil(t, s.Spec.Sealed, "false flags are not written")
	require.Equal(t, map[string]string{"team": "payments"}, s.Spec.Metadata, "the marker stays out of spec")

	cfg := f.serverStream("PAYMENTS")
	require.Equal(t, 168*time.Hour, cfg.MaxAge, "adoption changes nothing but the marker")
	require.Equal(t, "payments", cfg.Metadata["team"])
	require.Equal(t, string(s.UID), cfg.Metadata[lifecycle.OwnerKey])
	require.Equal(t, "Adopted", cfg.Metadata[lifecycle.OriginKey])
}

func TestStreamAdoptOrCreate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		exists bool
		origin js.OwnershipOrigin
	}{
		{"exists", true, js.OwnershipAdopted},
		{"missing", false, js.OwnershipCreated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			if tc.exists {
				f.createByHand("REFUNDS", nil)
			}
			f.create(newStream("refunds", "REFUNDS", func(s *js.NatsStreamSpec) {
				s.AdoptionPolicy = js.AdoptionAdoptOrCreate
				s.Subjects = []string{"refunds.>"}
				s.MaxAge = &metav1.Duration{Duration: 2160 * time.Hour}
			}))
			f.reconcileStream("refunds")

			s := f.stream("refunds")
			condition(t, s.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionTrue, lifecycle.ReasonSynced)
			require.Equal(t, tc.origin, s.Status.Ownership.Origin)
			if tc.exists {
				condition(t, s.Status.Conditions, lifecycle.ConditionAdopted, metav1.ConditionTrue, lifecycle.ReasonFoundUnowned)
			}
			cfg := f.serverStream("REFUNDS")
			require.Equal(t, 2160*time.Hour, cfg.MaxAge, "spec is applied")
			require.Equal(t, []string{"refunds.>"}, cfg.Subjects)

			require.Equal(t, 2160*time.Hour, s.Spec.MaxAge.Duration, "set fields stay")
			require.Equal(t, js.StorageFile, *s.Spec.Storage, "omitted fields are late-initialized")
			require.EqualValues(t, 1, *s.Spec.Replicas)
			require.Equal(t, 2*time.Minute, s.Spec.Duplicates.Duration)

			f.reconcileStream("refunds")
			condition(t, f.stream("refunds").Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionTrue, lifecycle.ReasonMatchesSpec)
		})
	}
}

func TestStreamOwnedByOther(t *testing.T) {
	for _, policy := range []js.AdoptionPolicy{js.AdoptionNever, js.AdoptionAdopt, js.AdoptionAdoptOrCreate} {
		t.Run(string(policy), func(t *testing.T) {
			f := newFixture(t)
			owner := newStream("first", "SHARED", nil)
			f.create(owner)
			f.reconcileStream("first")

			f.create(newStream("second", "SHARED", func(s *js.NatsStreamSpec) { s.AdoptionPolicy = policy }))
			f.reconcileStream("second")
			term := condition(t, f.stream("second").Status.Conditions, lifecycle.ConditionTerminal, metav1.ConditionTrue, lifecycle.ReasonOwnedByOther)
			require.Contains(t, term.Message, string(owner.UID))
			require.Equal(t, string(owner.UID), f.serverStream("SHARED").Metadata[lifecycle.OwnerKey])
		})
	}
}

func TestStreamAdoptsOrphanMarker(t *testing.T) {
	f := newFixture(t)
	f.createByHand("KEPT", map[string]string{lifecycle.OwnerKey: string(uuid.NewUUID()), lifecycle.OriginKey: "Created"})
	f.create(newStream("kept", "KEPT", func(s *js.NatsStreamSpec) { s.AdoptionPolicy = js.AdoptionAdoptOrCreate }))
	f.reconcileStream("kept")
	s := f.stream("kept")
	condition(t, s.Status.Conditions, lifecycle.ConditionAdopted, metav1.ConditionTrue, lifecycle.ReasonFoundUnowned)
	require.Equal(t, string(s.UID), f.serverStream("KEPT").Metadata[lifecycle.OwnerKey])
}

func TestStreamDrift(t *testing.T) {
	f := newFixture(t)
	f.create(newStream("orders", "ORDERS", func(s *js.NatsStreamSpec) {
		s.MaxAge = &metav1.Duration{Duration: 72 * time.Hour}
		s.Metadata = map[string]string{"team": "orders"}
	}))
	f.reconcileStream("orders")

	cfg := f.serverStream("ORDERS")
	cfg.MaxAge = time.Hour
	cfg.Description = "by hand"
	cfg.Metadata["team"] = "someone"
	_, err := f.js.UpdateStream(t.Context(), *cfg)
	require.NoError(t, err)

	f.reconcileStream("orders")
	s := f.stream("orders")
	drift := condition(t, s.Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionFalse, lifecycle.ReasonDriftCorrected)
	require.Equal(t, "reapplied spec over drift in max_age, metadata", drift.Message, "an omitted field is not drift")
	condition(t, s.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionTrue, lifecycle.ReasonSynced)
	cfg = f.serverStream("ORDERS")
	require.Equal(t, 72*time.Hour, cfg.MaxAge)
	require.Equal(t, "orders", cfg.Metadata["team"])
	require.Equal(t, "by hand", cfg.Description, "fields spec omits keep the server's value")

	f.reconcileStream("orders")
	condition(t, f.stream("orders").Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionTrue, lifecycle.ReasonMatchesSpec)
}

func TestStreamSpecEditIsNotDrift(t *testing.T) {
	f := newFixture(t)
	f.create(newStream("orders", "ORDERS", nil))
	f.reconcileStream("orders")
	f.editStream("orders", func(s *js.NatsStreamSpec) { s.MaxAge = &metav1.Duration{Duration: time.Hour} })
	f.reconcileStream("orders")
	s := f.stream("orders")
	condition(t, s.Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionTrue, lifecycle.ReasonMatchesSpec)
	require.EqualValues(t, 2, s.Status.ObservedGeneration)
	require.Equal(t, time.Hour, f.serverStream("ORDERS").MaxAge)
}

func TestStreamRejected(t *testing.T) {
	f := newFixture(t)
	f.create(newStream("first", "FIRST", func(s *js.NatsStreamSpec) { s.Subjects = []string{"shared.>"} }))
	f.reconcileStream("first")
	f.create(newStream("second", "SECOND", func(s *js.NatsStreamSpec) { s.Subjects = []string{"shared.>"} }))
	res := f.reconcileStream("second")
	require.Zero(t, res.RequeueAfter)
	term := condition(t, f.stream("second").Status.Conditions, lifecycle.ConditionTerminal, metav1.ConditionTrue, lifecycle.ReasonRejected)
	require.Contains(t, term.Message, "subjects overlap")
	require.Nil(t, f.serverStream("SECOND"))
}

func TestStreamDeletion(t *testing.T) {
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
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			s := newStream("orders", "ORDERS", func(s *js.NatsStreamSpec) { s.DeletionPolicy = tc.policy })
			f.create(s)
			f.reconcileStream("orders")
			if !tc.owned {
				cfg := f.serverStream("ORDERS")
				cfg.Metadata = map[string]string{lifecycle.OwnerKey: "someone-else"}
				_, err := f.js.UpdateStream(t.Context(), *cfg)
				require.NoError(t, err)
			}
			require.NoError(t, f.c.Delete(t.Context(), f.stream("orders")))
			f.reconcileStream("orders")
			require.True(t, f.gone(s), "the finalizer is removed")
			require.Equal(t, tc.kept, f.serverStream("ORDERS") != nil)
		})
	}
}

func TestStreamDeleteUnreachableConnection(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ref      natsv1beta1.ObjectReference
		reason   string
		released bool
	}{
		{"not found", natsv1beta1.ObjectReference{Name: "missing"}, lifecycle.ReasonConnectionNotFound, true},
		{"no grant", natsv1beta1.ObjectReference{Name: "demo", Namespace: "elsewhere"}, grant.ReasonReferenceNotPermitted, true},
		{"failing", natsv1beta1.ObjectReference{Name: "broken"}, lifecycle.ReasonConnectionFailed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.create(&natsv1beta1.NatsConnection{
				ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "broken"},
				Spec: natsv1beta1.NatsConnectionSpec{
					Servers:     f.nats.urls,
					Credentials: &natsv1beta1.Credentials{SecretKeyRef: natsv1beta1.CredentialsSecretKeySelector{Name: "absent", Key: "user.creds"}},
				},
			})
			f.create(newStream("orders", "ORDERS", func(s *js.NatsStreamSpec) {
				s.DeletionPolicy = js.DeletionDelete
				s.ConnectionRef = tc.ref
			}))
			f.reconcileStream("orders")
			require.NoError(t, f.c.Delete(t.Context(), f.stream("orders")))
			res := f.reconcileStream("orders")
			if tc.released {
				require.True(t, f.gone(&js.NatsStream{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "orders"}}), "the finalizer is removed")
				return
			}
			require.NotZero(t, res.RequeueAfter)
			s := f.stream("orders")
			require.Contains(t, s.Finalizers, lifecycle.Finalizer)
			condition(t, s.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionFalse, tc.reason)
		})
	}
}

func TestStreamConnectionUnresolved(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ref    natsv1beta1.ObjectReference
		reason string
	}{
		{"not found", natsv1beta1.ObjectReference{Name: "missing"}, lifecycle.ReasonConnectionNotFound},
		{"no grant", natsv1beta1.ObjectReference{Name: "demo", Namespace: "elsewhere"}, grant.ReasonReferenceNotPermitted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.create(newStream("orders", "ORDERS", func(s *js.NatsStreamSpec) { s.ConnectionRef = tc.ref }))
			res := f.reconcileStream("orders")
			require.NotZero(t, res.RequeueAfter)
			s := f.stream("orders")
			condition(t, s.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionFalse, tc.reason)
			if tc.reason == grant.ReasonReferenceNotPermitted {
				condition(t, s.Status.Conditions, grant.ConditionReferencesResolved, metav1.ConditionFalse, grant.ReasonNoGrant)
			}
			require.Nil(t, f.serverStream("ORDERS"))
		})
	}
}
