package grant_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
)

const authGroup = "auth.nats.mikluko.io"

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, natsv1beta1.AddToScheme(s))
	require.NoError(t, authv1beta1.AddToScheme(s))
	return s
}

// paymentsGrant is story 4's payments-users grant, toName its to entry's
// name.
func paymentsGrant(toName string) *natsv1beta1.NatsReferenceGrant {
	return &natsv1beta1.NatsReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-users", Namespace: "nats-system"},
		Spec: natsv1beta1.NatsReferenceGrantSpec{
			From: []natsv1beta1.ReferenceGrantFrom{{Group: authGroup, Kind: "NatsUser", Namespace: "payments"}},
			To:   []natsv1beta1.ReferenceGrantTo{{Group: authGroup, Kind: "NatsAccount", Name: toName}},
		},
	}
}

func TestAdmit(t *testing.T) {
	user := func(ns string) grant.Referrer {
		return grant.Referrer{Group: authGroup, Kind: "NatsUser", Namespace: ns}
	}
	account := func(ns, name string) grant.Target {
		return grant.Target{Group: authGroup, Kind: "NatsAccount", Namespace: ns, Name: name}
	}
	tests := []struct {
		name   string
		grants []client.Object
		from   grant.Referrer
		to     grant.Target
		denied bool
	}{
		{"same namespace, no grant", nil, user("payments"), account("payments", "payments"), false},
		{"namespace omitted, no grant", nil, user("payments"), account("", "payments"), false},
		{"named grant", []client.Object{paymentsGrant("payments")}, user("payments"), account("nats-system", "payments"), false},
		{"unnamed grant covers the kind", []client.Object{paymentsGrant("")}, user("payments"), account("nats-system", "other"), false},
		{"no grant", nil, user("orders"), account("nats-system", "payments"), true},
		{"grant for another namespace", []client.Object{paymentsGrant("payments")}, user("orders"), account("nats-system", "payments"), true},
		{"grant names another object", []client.Object{paymentsGrant("payments")}, user("payments"), account("nats-system", "other"), true},
		{"grant for another referrer kind", []client.Object{paymentsGrant("")}, grant.Referrer{Group: authGroup, Kind: "NatsAccount", Namespace: "payments"}, account("nats-system", "payments"), true},
		{"grant for another target kind", []client.Object{paymentsGrant("")}, user("payments"), grant.Target{Group: authGroup, Kind: "NatsSystemAccount", Namespace: "nats-system", Name: "sys"}, true},
		{"grant for another group", []client.Object{paymentsGrant("")}, grant.Referrer{Group: "jetstream.nats.mikluko.io", Kind: "NatsUser", Namespace: "payments"}, account("nats-system", "payments"), true},
		{"grant in another namespace", []client.Object{func() client.Object {
			g := paymentsGrant("")
			g.Namespace = "elsewhere"
			return g
		}()}, user("payments"), account("nats-system", "payments"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(tt.grants...).Build()
			cond, err := grant.Admit(t.Context(), c, tt.from, tt.to)
			require.NoError(t, err)
			if !tt.denied {
				require.Nil(t, cond)
				return
			}
			require.NotNil(t, cond)
			require.Equal(t, grant.ConditionReferencesResolved, cond.Type)
			require.Equal(t, metav1.ConditionFalse, cond.Status)
			require.Equal(t, grant.ReasonNoGrant, cond.Reason)
		})
	}
}

// TestAdmitMessage pins the message story 4's status-natsuser-denied.yaml
// shows.
func TestAdmitMessage(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(paymentsGrant("payments")).Build()
	cond, err := grant.Admit(t.Context(), c,
		grant.Referrer{Group: authGroup, Kind: "NatsUser", Namespace: "orders"},
		grant.Target{Group: authGroup, Kind: "NatsAccount", Namespace: "nats-system", Name: "payments"})
	require.NoError(t, err)
	require.Equal(t, "no NatsReferenceGrant in nats-system admits NatsUser from namespace orders to NatsAccount payments", cond.Message)
}

func TestAdmitListError(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()
	cond, err := grant.Admit(t.Context(), c,
		grant.Referrer{Group: authGroup, Kind: "NatsUser", Namespace: "orders"},
		grant.Target{Group: authGroup, Kind: "NatsAccount", Namespace: "nats-system", Name: "payments"})
	require.Error(t, err)
	require.Nil(t, cond)
}
