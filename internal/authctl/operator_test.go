package authctl

import (
	"testing"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// TestOperatorReconciler_NoPush pins that a NatsOperator signs its system
// account's JWT into status without pushing it, and keeps it while no
// server holds a newer one.
func TestOperatorReconciler_NoPush(t *testing.T) {
	seed := func() []byte {
		kp, err := nkeys.CreatePair(nkeys.PrefixByteAccount)
		require.NoError(t, err)
		s, err := kp.Seed()
		require.NoError(t, err)
		return s
	}
	s := testScheme(t)
	require.NoError(t, natsv1beta1.AddToScheme(s))
	op := &authv1beta1.NatsOperator{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "op", UID: "op"},
		Spec:       authv1beta1.NatsOperatorSpec{SystemAccountRef: natsv1beta1.ObjectReference{Name: "sys"}},
	}
	sys := &authv1beta1.NatsSystemAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "sys", UID: "sys"},
		Spec: authv1beta1.NatsSystemAccountSpec{
			OperatorRef: natsv1beta1.ObjectReference{Name: "op"},
			Keys: &authv1beta1.Keys{
				Identity: &authv1beta1.IdentityKey{SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: "sys-keys", Key: "identity"}},
				Signing:  []authv1beta1.SigningKey{{Name: "s", SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: "sys-keys", Key: "signing"}}},
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "sys-keys"},
		Data:       map[string][]byte{"identity": seed(), "signing": seed()},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(op, sys, secret).WithStatusSubresource(op).
		WithIndex(&authv1beta1.NatsAccount{}, operatorField, func(client.Object) []string { return nil }).
		WithIndex(&authv1beta1.NatsUser{}, userAccountField, func(client.Object) []string { return nil }).
		Build()
	d := &countingDistributor{current: authv1beta1.Distribution{Servers: 1}}
	r := &OperatorReconciler{Client: c, Distributor: d}
	var signed string
	for i := range 2 {
		_, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(op)})
		require.NoError(t, err)
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(op), op))
		require.NotNil(t, op.Status.SystemAccount, "reconcile %d", i)
		if signed == "" {
			signed = op.Status.SystemAccount.JWT
		}
		require.Equal(t, signed, op.Status.SystemAccount.JWT, "reconcile %d", i)
	}
	require.Zero(t, d.pushes)
}
