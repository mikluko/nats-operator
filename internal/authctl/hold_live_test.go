package authctl_test

import (
	"testing"
	"time"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/authctl"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// keysSecret holds keys' identity and first signing seed under the keys
// "identity" and "signing".
func keysSecret(t *testing.T, name string, keys jwtplane.Keys) *corev1.Secret {
	t.Helper()
	id, err := keys.Identity.Seed()
	require.NoError(t, err)
	sk, err := keys.Signing[0].Pair.Seed()
	require.NoError(t, err)
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testOperator.Namespace, Name: name},
		Data:       map[string][]byte{"identity": id, "signing": sk},
	}
}

// seedsIn is the Keys reading the Secret keysSecret makes under name.
func seedsIn(name string) *authv1beta1.Keys {
	return &authv1beta1.Keys{
		Identity: &authv1beta1.IdentityKey{SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: name, Key: "identity"}},
		Signing:  []authv1beta1.SigningKey{{Name: "s", SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: name, Key: "signing"}}},
	}
}

func TestAccountReconciler_HoldsPushWhileAServerIsSilent(t *testing.T) {
	p := newPlane(t)
	c := startFullCluster(t, p, 3)
	r := resolversOn(t, c, testOperator)

	taken, takenPub := newAccount(t)
	fresh, freshPub := newAccount(t)
	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	revoked, err := kp.PublicKey()
	require.NoError(t, err)
	made, err := jwtplane.SignAccount(jwtplane.Account{
		Name:        "taken",
		Keys:        taken,
		Revocations: []jwtplane.Revocation{{PublicKey: revoked, At: time.Now().Add(-time.Hour)}},
	}, p.op, time.Now().Add(-time.Minute))
	require.NoError(t, err)
	for _, s := range c.srvs {
		require.NoError(t, s.AccountResolver().Store(takenPub, made))
	}
	held, err := r.Lookup(t.Context(), testOperator, takenPub)
	require.NoError(t, err)
	require.Equal(t, made, held, "the roster is the three servers")

	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, natsv1beta1.AddToScheme, authv1beta1.AddToScheme} {
		require.NoError(t, add(s))
	}
	account := func(name string) *authv1beta1.NatsAccount {
		return &authv1beta1.NatsAccount{
			ObjectMeta: metav1.ObjectMeta{Namespace: testOperator.Namespace, Name: name, UID: types.UID(name)},
			Spec:       authv1beta1.NatsAccountSpec{OperatorRef: natsv1beta1.ObjectReference{Name: testOperator.Name}, Keys: seedsIn(name + "-keys")},
		}
	}
	objs := []client.Object{
		&authv1beta1.NatsOperator{
			ObjectMeta: metav1.ObjectMeta{Namespace: testOperator.Namespace, Name: testOperator.Name, UID: "op"},
			Spec:       authv1beta1.NatsOperatorSpec{SystemAccountRef: natsv1beta1.ObjectReference{Name: "sys"}, Keys: seedsIn("op-keys")},
		},
		account("taken"), account("fresh"),
		keysSecret(t, "op-keys", p.op), keysSecret(t, "taken-keys", taken), keysSecret(t, "fresh-keys", fresh),
	}
	b := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(objs...)
	require.NoError(t, authctl.IndexFake(t.Context(), b))
	kc := b.Build()
	rec := &authctl.AccountReconciler{Client: kc, Distributor: r}
	reconciled := func(name string) authv1beta1.NatsAccountStatus {
		t.Helper()
		k := client.ObjectKey{Namespace: testOperator.Namespace, Name: name}
		_, err := rec.Reconcile(t.Context(), reconcile.Request{NamespacedName: k})
		require.NoError(t, err)
		var acc authv1beta1.NatsAccount
		require.NoError(t, kc.Get(t.Context(), k, &acc))
		return acc.Status
	}

	c.stop(2)
	for name, pub := range map[string]string{"taken": takenPub, "fresh": freshPub} {
		st := reconciled(name)
		require.Equal(t, pub, st.PublicKey)
		require.NotEmpty(t, st.JWT, name)
		require.True(t, meta.IsStatusConditionTrue(st.Conditions, authctl.ConditionRevocationsUnrecovered), name)
	}
	for i := range 2 {
		require.Equal(t, made, c.held(i, takenPub), "server %d keeps the JWT made elsewhere while server 2 is silent", i)
		require.Empty(t, c.held(i, freshPub), "server %d", i)
	}

	for range authctl.RosterMisses {
		require.NoError(t, r.PollOperator(t.Context(), testOperator))
	}
	st := reconciled("taken")
	require.Nil(t, meta.FindStatusCondition(st.Conditions, authctl.ConditionRevocationsUnrecovered))
	require.True(t, revokesKey(st.JWT, revoked), "the JWT signed once server 2 left the roster keeps the revocation")
	freshJWT := reconciled("fresh").JWT
	for i := range 2 {
		require.Equal(t, st.JWT, c.held(i, takenPub), "server %d", i)
		require.Equal(t, freshJWT, c.held(i, freshPub), "server %d holds the new account once server 2 left the roster", i)
	}
}
