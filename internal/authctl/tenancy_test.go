package authctl

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

func TestKeyHolder(t *testing.T) {
	t0 := metav1.NewTime(time.Unix(1000, 0))
	t1 := metav1.NewTime(time.Unix(2000, 0))
	claim := func(name string, created metav1.Time, recorded string) keyClaim {
		return keyClaim{kind: "NatsAccount", key: types.NamespacedName{Namespace: "ns", Name: name}, uid: types.UID(name), created: created, recorded: recorded}
	}
	for _, tc := range []struct {
		name   string
		self   keyClaim
		others []keyClaim
		want   string
	}{
		{"no other", claim("a", t0, ""), nil, ""},
		{"another key", claim("a", t0, ""), []keyClaim{claim("b", t0, "K2")}, ""},
		{"itself", claim("a", t0, "K"), []keyClaim{claim("a", t0, "K")}, ""},
		{"held by an older", claim("a", t1, ""), []keyClaim{claim("b", t0, "K")}, "NatsAccount ns/b"},
		{"held by a newer, not recorded here", claim("a", t0, ""), []keyClaim{claim("b", t1, "K")}, "NatsAccount ns/b"},
		{"both record it, the other older", claim("a", t1, "K"), []keyClaim{claim("b", t0, "K")}, "NatsAccount ns/b"},
		{"both record it, this one older", claim("a", t0, "K"), []keyClaim{claim("b", t1, "K")}, ""},
		{"both record it, same second, name decides", claim("b", t0, "K"), []keyClaim{claim("a", t0, "K")}, "NatsAccount ns/a"},
		{"both record it, same second, this name first", claim("a", t0, "K"), []keyClaim{claim("b", t0, "K")}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, held := keyHolder(tc.self, "K", tc.others)
			require.Equal(t, tc.want != "", held)
			if held {
				require.Equal(t, tc.want, got.String())
			}
		})
	}
}

// TestIsSystemKey pins that a NatsSystemAccount holds the key its status
// records, and the one its spec or identity seed Secret resolves to before
// status records any.
func TestIsSystemKey(t *testing.T) {
	kp, err := nkeys.CreateAccount()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)
	other := "A" + strings.Repeat("B", 55)
	identity := &authv1beta1.Keys{Identity: &authv1beta1.IdentityKey{SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: "sys-id", Key: "seed"}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "sys-id"}, Data: map[string][]byte{"seed": seed}}
	for _, tc := range []struct {
		name    string
		spec    authv1beta1.NatsSystemAccountSpec
		status  string
		objects []client.Object
		want    bool
	}{
		{"status records it", authv1beta1.NatsSystemAccountSpec{}, pub, nil, true},
		{"spec names it", authv1beta1.NatsSystemAccountSpec{PublicKey: pub}, "", nil, true},
		{"identity Secret holds it", authv1beta1.NatsSystemAccountSpec{Keys: identity}, "", []client.Object{secret}, true},
		{"identity Secret absent", authv1beta1.NatsSystemAccountSpec{Keys: identity}, "", nil, false},
		{"generated identity not yet generated", authv1beta1.NatsSystemAccountSpec{}, "", nil, false},
		{"another key", authv1beta1.NatsSystemAccountSpec{PublicKey: other}, other, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tc.objects...).Build()
			sys := &authv1beta1.NatsSystemAccount{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "sys"},
				Spec:       tc.spec,
				Status:     authv1beta1.NatsSystemAccountStatus{PublicKey: tc.status},
			}
			got, err := isSystemKey(t.Context(), c, sys, pub)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestSystemAccountAccounts pins that a NatsSystemAccount maps to every
// NatsAccount under the NatsOperator it names, and to no other.
func TestSystemAccountAccounts(t *testing.T) {
	account := func(namespace, name string, op natsv1beta1.ObjectReference) *authv1beta1.NatsAccount {
		return &authv1beta1.NatsAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Spec: authv1beta1.NatsAccountSpec{OperatorRef: op}}
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(
			account("sys-ns", "local", natsv1beta1.ObjectReference{Name: "op"}),
			account("tenant", "remote", natsv1beta1.ObjectReference{Namespace: "sys-ns", Name: "op"}),
			account("sys-ns", "elsewhere", natsv1beta1.ObjectReference{Name: "other"}),
		).
		WithIndex(&authv1beta1.NatsAccount{}, operatorField, func(o client.Object) []string {
			return []string{keyValue(refKey(o.(*authv1beta1.NatsAccount).Spec.OperatorRef, o.GetNamespace()))}
		}).
		Build()
	sys := &authv1beta1.NatsSystemAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: "sys-ns", Name: "sys"},
		Spec:       authv1beta1.NatsSystemAccountSpec{OperatorRef: natsv1beta1.ObjectReference{Name: "op"}},
	}
	require.ElementsMatch(t, []reconcile.Request{
		{NamespacedName: types.NamespacedName{Namespace: "sys-ns", Name: "local"}},
		{NamespacedName: types.NamespacedName{Namespace: "tenant", Name: "remote"}},
	}, systemAccountAccounts(t.Context(), c, sys))
	require.Empty(t, systemAccountAccounts(t.Context(), c, &authv1beta1.NatsAccount{}))
}
