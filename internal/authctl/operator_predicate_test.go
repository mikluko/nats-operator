package authctl

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

func TestAccountSignedChange(t *testing.T) {
	now := metav1.Now()
	base := func() *authv1beta1.NatsAccount {
		return &authv1beta1.NatsAccount{
			ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns", Generation: 1, ResourceVersion: "1"},
			Status:     authv1beta1.NatsAccountStatus{PublicKey: "AKEY", JWT: "jwt", ObservedGeneration: 1},
		}
	}
	tests := []struct {
		name   string
		mutate func(*authv1beta1.NatsAccount)
		want   bool
	}{
		{name: "conditions only", mutate: func(a *authv1beta1.NatsAccount) {
			a.ResourceVersion = "2"
			a.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}}
			a.Status.JWTHash = "h"
		}},
		{name: "generation", mutate: func(a *authv1beta1.NatsAccount) { a.Generation = 2 }, want: true},
		{name: "deletion", mutate: func(a *authv1beta1.NatsAccount) { a.DeletionTimestamp = &now }, want: true},
		{name: "public key", mutate: func(a *authv1beta1.NatsAccount) { a.Status.PublicKey = "BKEY" }, want: true},
		{name: "jwt", mutate: func(a *authv1beta1.NatsAccount) { a.Status.JWT = "jwt2" }, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := base()
			tt.mutate(n)
			require.Equal(t, tt.want, accountSignedChange.Update(event.UpdateEvent{ObjectOld: base(), ObjectNew: n}))
		})
	}
	require.True(t, accountSignedChange.Create(event.CreateEvent{Object: base()}))
	require.True(t, accountSignedChange.Delete(event.DeleteEvent{Object: base()}))
}

func TestGrantOperators(t *testing.T) {
	s := testScheme(t)
	require.NoError(t, natsv1beta1.AddToScheme(s))
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(
		&authv1beta1.NatsOperator{ObjectMeta: metav1.ObjectMeta{Namespace: "nats-system", Name: "demo"}},
		&authv1beta1.NatsOperator{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "elsewhere"}},
	).Build()
	accounts := natsv1beta1.ReferenceGrantFrom{Group: authGroup, Kind: "NatsAccount", Namespace: "tenant"}
	users := natsv1beta1.ReferenceGrantFrom{Group: authGroup, Kind: "NatsUser", Namespace: "tenant"}
	operators := natsv1beta1.ReferenceGrantTo{Group: authGroup, Kind: "NatsOperator", Name: "demo"}
	natsAccounts := natsv1beta1.ReferenceGrantTo{Group: authGroup, Kind: "NatsAccount"}
	grantOf := func(from natsv1beta1.ReferenceGrantFrom, to natsv1beta1.ReferenceGrantTo) *natsv1beta1.NatsReferenceGrant {
		return &natsv1beta1.NatsReferenceGrant{
			ObjectMeta: metav1.ObjectMeta{Namespace: "nats-system", Name: "g"},
			Spec:       natsv1beta1.NatsReferenceGrantSpec{From: []natsv1beta1.ReferenceGrantFrom{from}, To: []natsv1beta1.ReferenceGrantTo{to}},
		}
	}
	tests := []struct {
		name  string
		grant *natsv1beta1.NatsReferenceGrant
		want  []reconcile.Request
	}{
		{name: "accounts to operators", grant: grantOf(accounts, operators), want: []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "nats-system", Name: "demo"}}}},
		{name: "users to operators", grant: grantOf(users, operators)},
		{name: "accounts to accounts", grant: grantOf(accounts, natsAccounts)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, grantOperators(t.Context(), c, tt.grant))
		})
	}
}
