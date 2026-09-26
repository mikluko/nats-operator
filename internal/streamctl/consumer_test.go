package streamctl

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/lifecycle"
)

// newConsumer is a NatsConsumer named name, durable server, on the stream
// ORDERS through demo, generation 1.
func newConsumer(name, server string, mutate func(*js.NatsConsumerSpec)) *js.NatsConsumer {
	c := &js.NatsConsumer{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name, UID: uuid.NewUUID(), Generation: 1},
		Spec: js.NatsConsumerSpec{
			ConnectionRef:  &demo,
			Stream:         "ORDERS",
			Policies:       js.Policies{AdoptionPolicy: js.AdoptionNever, TerminalPolicy: js.TerminalHold},
			DeletionPolicy: js.DeletionDelete,
			ConsumerConfig: js.ConsumerConfig{
				Name:          server,
				DeliverPolicy: ptrTo(js.DeliverAll),
				AckPolicy:     ptrTo(js.AckExplicit),
			},
		},
	}
	if mutate != nil {
		mutate(&c.Spec)
	}
	return c
}

// withOrders creates and reconciles the NatsStream "orders", ORDERS.
func (f *fixture) withOrders() {
	f.t.Helper()
	f.create(newStream("orders", "ORDERS", func(s *js.NatsStreamSpec) { s.DeletionPolicy = js.DeletionDelete }))
	f.reconcileStream("orders")
}

func TestConsumerCreate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		deliver string
	}{
		{"pull", ""},
		{"push", "deliver.audit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.withOrders()
			f.create(newConsumer("audit", "AUDIT", func(c *js.NatsConsumerSpec) {
				c.FilterSubjects = []string{"orders.settled.>"}
				c.AckWait = &metav1.Duration{Duration: 30 * time.Second}
				c.DeliverSubject = tc.deliver
			}))
			res := f.reconcileConsumer("audit")
			require.Equal(t, testResync, res.RequeueAfter)

			c := f.consumer("audit")
			require.Contains(t, c.Finalizers, lifecycle.Finalizer)
			condition(t, c.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionTrue, lifecycle.ReasonSynced)
			condition(t, c.Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionTrue, lifecycle.ReasonMatchesSpec)
			require.Equal(t, &js.Ownership{Origin: js.OwnershipCreated, UID: c.UID}, c.Status.Ownership)
			require.NotNil(t, c.Status.Server.Created)

			info := f.serverConsumer("AUDIT")
			require.NotNil(t, info)
			require.Equal(t, "AUDIT", info.Config.Durable)
			require.Equal(t, []string{"orders.settled.>"}, info.Config.FilterSubjects)
			require.Equal(t, tc.deliver, info.Config.DeliverSubject)
			require.Equal(t, string(c.UID), info.Config.Metadata[lifecycle.OwnerKey])

			f.reconcileConsumer("audit")
			condition(t, f.consumer("audit").Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionTrue, lifecycle.ReasonMatchesSpec)
		})
	}
}

func TestConsumerStreamNotFound(t *testing.T) {
	f := newFixture(t)
	f.create(newConsumer("audit", "AUDIT", nil))
	res := f.reconcileConsumer("audit")
	require.Equal(t, testResync, res.RequeueAfter)
	condition(t, f.consumer("audit").Status.Conditions, lifecycle.ConditionReady, metav1.ConditionFalse, ReasonStreamNotFound)
}

func TestConsumerStreamRef(t *testing.T) {
	f := newFixture(t)
	f.create(newConsumer("settlement", "settlement", func(c *js.NatsConsumerSpec) {
		c.ConnectionRef, c.Stream = nil, ""
		c.StreamRef = &natsv1beta1.ObjectReference{Name: "orders"}
		c.AdoptionPolicy = js.AdoptionAdoptOrCreate
		c.MaxDeliver = ptrTo(int64(10))
	}))

	f.reconcileConsumer("settlement")
	condition(t, f.consumer("settlement").Status.Conditions, lifecycle.ConditionReady, metav1.ConditionFalse, ReasonStreamNotFound)

	f.create(newStream("orders", "ORDERS", func(s *js.NatsStreamSpec) { s.ConnectionRef = natsv1beta1.ObjectReference{Name: "missing"} }))
	f.reconcileStream("orders")
	f.reconcileConsumer("settlement")
	condition(t, f.consumer("settlement").Status.Conditions, lifecycle.ConditionReady, metav1.ConditionFalse, ReasonStreamNotReady)

	f.editStream("orders", func(s *js.NatsStreamSpec) { s.ConnectionRef = demo })
	f.reconcileStream("orders")
	f.reconcileConsumer("settlement")
	c := f.consumer("settlement")
	condition(t, c.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionTrue, lifecycle.ReasonSynced)
	require.NotNil(t, f.serverConsumer("settlement"), "created through the stream's connection")
	require.EqualValues(t, 10, *c.Spec.MaxDeliver, "set fields stay")
	require.Equal(t, 30*time.Second, c.Spec.AckWait.Duration, "omitted fields are late-initialized")
	require.Equal(t, js.ReplayInstant, *c.Spec.ReplayPolicy)
	require.Empty(t, c.Spec.DeliverSubject)

	f.reconcileConsumer("settlement")
	condition(t, f.consumer("settlement").Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionTrue, lifecycle.ReasonMatchesSpec)
}

func TestConsumerStreamRefNeedsGrant(t *testing.T) {
	f := newFixture(t)
	f.create(newConsumer("settlement", "settlement", func(c *js.NatsConsumerSpec) {
		c.ConnectionRef, c.Stream = nil, ""
		c.StreamRef = &natsv1beta1.ObjectReference{Name: "orders", Namespace: "elsewhere"}
	}))
	f.reconcileConsumer("settlement")
	c := f.consumer("settlement")
	condition(t, c.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionFalse, grant.ReasonReferenceNotPermitted)
	condition(t, c.Status.Conditions, grant.ConditionReferencesResolved, metav1.ConditionFalse, grant.ReasonNoGrant)
}

func TestConsumerImmutableChange(t *testing.T) {
	for _, tc := range []struct {
		name     string
		recreate bool
	}{
		{"refused", false},
		{"recreated", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.withOrders()
			f.create(newConsumer("audit", "AUDIT", func(c *js.NatsConsumerSpec) { c.RecreateOnImmutableChange = &tc.recreate }))
			f.reconcileConsumer("audit")
			before := f.serverConsumer("AUDIT").Created

			f.editConsumer("audit", func(c *js.NatsConsumerSpec) { c.DeliverPolicy = ptrTo(js.DeliverNew) })
			f.reconcileConsumer("audit")
			c := f.consumer("audit")
			info := f.serverConsumer("AUDIT")
			if !tc.recreate {
				term := condition(t, c.Status.Conditions, lifecycle.ConditionTerminal, metav1.ConditionTrue, ReasonImmutableField)
				require.Contains(t, term.Message, "deliver_policy")
				require.Equal(t, before, info.Created, "the consumer is untouched")
				return
			}
			require.Nil(t, meta.FindStatusCondition(c.Status.Conditions, lifecycle.ConditionTerminal))
			condition(t, c.Status.Conditions, lifecycle.ConditionReady, metav1.ConditionTrue, lifecycle.ReasonSynced)
			require.Equal(t, jetstream.DeliverNewPolicy, info.Config.DeliverPolicy)
			require.NotEqual(t, before, info.Created, "the consumer was recreated")
		})
	}
}

func TestConsumerPullToPushIsImmutable(t *testing.T) {
	f := newFixture(t)
	f.withOrders()
	f.create(newConsumer("audit", "AUDIT", nil))
	f.reconcileConsumer("audit")
	f.editConsumer("audit", func(c *js.NatsConsumerSpec) { c.DeliverSubject = "deliver.audit" })
	f.reconcileConsumer("audit")
	term := condition(t, f.consumer("audit").Status.Conditions, lifecycle.ConditionTerminal, metav1.ConditionTrue, ReasonImmutableField)
	require.Contains(t, term.Message, "deliver_subject")
}

func TestConsumerDrift(t *testing.T) {
	f := newFixture(t)
	f.withOrders()
	f.create(newConsumer("audit", "AUDIT", func(c *js.NatsConsumerSpec) { c.MaxDeliver = ptrTo(int64(5)) }))
	f.reconcileConsumer("audit")

	info := f.serverConsumer("AUDIT")
	cfg := info.Config
	cfg.MaxDeliver = 9
	_, err := f.js.UpdateConsumer(t.Context(), "ORDERS", cfg)
	require.NoError(t, err)

	f.reconcileConsumer("audit")
	drift := condition(t, f.consumer("audit").Status.Conditions, lifecycle.ConditionSynced, metav1.ConditionFalse, lifecycle.ReasonDriftCorrected)
	require.Equal(t, "reapplied spec over drift in max_deliver", drift.Message)
	require.Equal(t, 5, f.serverConsumer("AUDIT").Config.MaxDeliver)
}

func TestConsumerNeverExistsUnowned(t *testing.T) {
	f := newFixture(t)
	f.withOrders()
	_, err := f.js.CreateOrUpdateConsumer(t.Context(), "ORDERS", jetstreamConsumer("AUDIT"))
	require.NoError(t, err)
	f.create(newConsumer("audit", "AUDIT", nil))
	f.reconcileConsumer("audit")
	term := condition(t, f.consumer("audit").Status.Conditions, lifecycle.ConditionTerminal, metav1.ConditionTrue, lifecycle.ReasonExistsUnowned)
	require.Equal(t, "consumer AUDIT on stream ORDERS exists and carries no ownership marker; set spec.adoptionPolicy to Adopt or AdoptOrCreate to take it over", term.Message)
}

func TestConsumerDeletion(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy js.DeletionPolicy
		kept   bool
	}{
		{"delete", js.DeletionDelete, false},
		{"retain", js.DeletionRetain, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.withOrders()
			c := newConsumer("audit", "AUDIT", func(c *js.NatsConsumerSpec) { c.DeletionPolicy = tc.policy })
			f.create(c)
			f.reconcileConsumer("audit")
			require.NoError(t, f.c.Delete(t.Context(), f.consumer("audit")))
			f.reconcileConsumer("audit")
			require.True(t, f.gone(c))
			require.Equal(t, tc.kept, f.serverConsumer("AUDIT") != nil)
		})
	}
}

func TestConsumerDeletionAfterStream(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*js.NatsConsumerSpec)
	}{
		{"stream", nil},
		{"streamRef", func(c *js.NatsConsumerSpec) {
			c.ConnectionRef, c.Stream = nil, ""
			c.StreamRef = &natsv1beta1.ObjectReference{Name: "orders"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.withOrders()
			c := newConsumer("audit", "AUDIT", tc.mutate)
			f.create(c)
			f.reconcileConsumer("audit")
			require.NotNil(t, f.serverConsumer("AUDIT"))

			require.NoError(t, f.c.Delete(t.Context(), f.stream("orders")))
			f.reconcileStream("orders")
			require.Nil(t, f.serverStream("ORDERS"))

			require.NoError(t, f.c.Delete(t.Context(), f.consumer("audit")))
			f.reconcileConsumer("audit")
			require.True(t, f.gone(c), "a consumer whose stream is gone is let go")
		})
	}
}
