package authctl

import (
	"testing"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
)

func TestGeneratedSecretName_Injective(t *testing.T) {
	type owner struct{ name, role string }
	owners := []owner{
		{"x", roleOperator}, {"x", roleSystemAccount}, {"x", roleAccount},
		{"x-system", roleAccount}, {"x-system", roleOperator}, {"x-account", roleOperator},
		{"x-operator", roleAccount}, {"x-system-account", roleAccount}, {"x-identity", roleAccount},
	}
	seen := map[string]owner{}
	for _, o := range owners {
		for _, k := range []string{"identity", "signing-1"} {
			name := generatedSecretName(o.name, o.role, k)
			prev, dup := seen[name]
			require.False(t, dup, "%s: %+v and %+v", name, prev, o)
			seen[name] = o
		}
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, authv1beta1.AddToScheme(s))
	return s
}

// TestGeneratedSeed_Ownership pins that a generated seed Secret is read
// only where it is annotated as generated for its owner.
func TestGeneratedSeed_Ownership(t *testing.T) {
	s := testScheme(t)
	acc := &authv1beta1.NatsAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "a", UID: types.UID("a")}}
	seed, err := nkeys.CreateAccount()
	require.NoError(t, err)
	raw, err := seed.Seed()
	require.NoError(t, err)
	secret := func(name, generatedFor string, owner client.Object) *corev1.Secret {
		sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name}, Data: map[string][]byte{SeedKey: raw}}
		if generatedFor != "" {
			sec.Annotations = map[string]string{GeneratedForAnnotation: generatedFor}
		}
		if owner != nil {
			require.NoError(t, controllerutil.SetControllerReference(owner, sec, s))
		}
		return sec
	}
	for _, tc := range []struct {
		name   string
		secret *corev1.Secret
		want   error
	}{
		{"generated for it", secret("owned", "account/a", nil), nil},
		{"generated for another", secret("other", "systemaccount/b", nil), errSeedNotOwned},
		{"generated for another role", secret("role", "operator/a", nil), errSeedNotOwned},
		{"not generated, though controlled by it", secret("stray", "", acc), errSeedNotOwned},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(tc.secret).Build()
			kp, err := generatedSeed(t.Context(), c, acc, roleAccount, tc.secret.Name, nkeys.PrefixByteAccount, true)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.Nil(t, kp)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, kp)
		})
	}
}

// TestGeneratedSeed_Created pins that a generated seed Secret is annotated
// as generated for its owner and carries no owner reference, so it outlives
// the owner.
func TestGeneratedSeed_Created(t *testing.T) {
	s := testScheme(t)
	acc := &authv1beta1.NatsAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "a", UID: types.UID("a")}}
	c := fake.NewClientBuilder().WithScheme(s).Build()
	kp, err := generatedSeed(t.Context(), c, acc, roleAccount, "a-account-identity", nkeys.PrefixByteAccount, true)
	require.NoError(t, err)
	require.NotNil(t, kp)
	var sec corev1.Secret
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "a-account-identity"}, &sec))
	require.Empty(t, sec.OwnerReferences)
	require.Equal(t, "account/a", sec.Annotations[GeneratedForAnnotation])
	again, err := generatedSeed(t.Context(), c, acc, roleAccount, "a-account-identity", nkeys.PrefixByteAccount, false)
	require.NoError(t, err)
	want, err := kp.PublicKey()
	require.NoError(t, err)
	got, err := again.PublicKey()
	require.NoError(t, err)
	require.Equal(t, want, got)
}
