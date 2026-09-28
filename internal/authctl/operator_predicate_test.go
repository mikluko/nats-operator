package authctl

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
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
