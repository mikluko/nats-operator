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
// only where its owner controls it.
func TestGeneratedSeed_Ownership(t *testing.T) {
	s := testScheme(t)
	acc := &authv1beta1.NatsAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "a", UID: types.UID("a")}}
	other := &authv1beta1.NatsSystemAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "b", UID: types.UID("b")}}
	seed, err := nkeys.CreateAccount()
	require.NoError(t, err)
	raw, err := seed.Seed()
	require.NoError(t, err)
	secret := func(name string, owner client.Object) *corev1.Secret {
		sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name}, Data: map[string][]byte{SeedKey: raw}}
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
		{"owned", secret("owned", acc), nil},
		{"another owner's", secret("other", other), errSeedNotOwned},
		{"no owner", secret("stray", nil), errSeedNotOwned},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(tc.secret).Build()
			kp, err := generatedSeed(t.Context(), c, acc, tc.secret.Name, nkeys.PrefixByteAccount, true)
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
