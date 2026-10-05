package authctl_test

import (
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/authctl"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// TestAccountReconciler_RefusesAAdoptionThatDropsClaims pins, on a server
// with a full resolver holding JWTs made elsewhere, that the first signing
// of an account is refused where it would drop a claim that JWT carries,
// goes through once spec.adoption.droppedClaims accepts the loss, goes
// through at once where the spec only changes a value, and is refused
// whatever spec.adoption says where that JWT limits a JetStream tier no
// spec names.
func TestAccountReconciler_RefusesAAdoptionThatDropsClaims(t *testing.T) {
	p := newPlane(t)
	c := startFullCluster(t, p, 1)
	r := resolversOn(t, c, testOperator)

	made := map[string]string{}
	accounts := map[string]jwtplane.Keys{}
	elsewhere := func(name string, edit func(c *jwt.AccountClaims)) {
		keys, pub := newAccount(t)
		accounts[name] = keys
		ac := jwt.NewAccountClaims(pub)
		ac.Name = name
		signing, err := keys.Signing[0].Pair.PublicKey()
		require.NoError(t, err)
		ac.SigningKeys.Add(signing)
		ac.Limits.Subs = 10
		edit(ac)
		token, err := ac.Encode(p.op.Signing[0].Pair)
		require.NoError(t, err)
		made[name] = token
		require.NoError(t, c.srvs[0].AccountResolver().Store(pub, token))
	}
	elsewhere("mapped", func(c *jwt.AccountClaims) {
		c.Mappings = jwt.Mapping{"a": []jwt.WeightedMapping{{Subject: "b", Weight: 100}}}
	})
	elsewhere("changed", func(*jwt.AccountClaims) {})
	elsewhere("tiered", func(c *jwt.AccountClaims) {
		c.Limits.JetStreamTieredLimits = jwt.JetStreamTieredLimits{"R7": {MemoryStorage: 1 << 20, DiskStorage: 1 << 20}}
	})

	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, natsv1beta1.AddToScheme, authv1beta1.AddToScheme} {
		require.NoError(t, add(s))
	}
	objs := []client.Object{
		&authv1beta1.NatsOperator{
			ObjectMeta: metav1.ObjectMeta{Namespace: testOperator.Namespace, Name: testOperator.Name, UID: "op"},
			Spec:       authv1beta1.NatsOperatorSpec{SystemAccountRef: natsv1beta1.ObjectReference{Name: "sys"}, Keys: seedsIn("op-keys")},
		},
		keysSecret(t, "op-keys", p.op),
	}
	for name, keys := range accounts {
		objs = append(objs, &authv1beta1.NatsAccount{
			ObjectMeta: metav1.ObjectMeta{Namespace: testOperator.Namespace, Name: name, UID: types.UID(name)},
			Spec: authv1beta1.NatsAccountSpec{
				OperatorRef: natsv1beta1.ObjectReference{Name: testOperator.Name},
				Keys:        seedsIn(name + "-keys"),
				Limits:      &authv1beta1.AccountLimits{Subscriptions: ptr.To[int64](20)},
			},
		}, keysSecret(t, name+"-keys", keys))
	}
	b := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(objs...)
	require.NoError(t, authctl.IndexFake(t.Context(), b))
	kc := b.Build()
	rec := &authctl.AccountReconciler{Client: kc, Distributor: r}
	reconciled := func(name string) authv1beta1.NatsAccount {
		t.Helper()
		k := client.ObjectKey{Namespace: testOperator.Namespace, Name: name}
		_, err := rec.Reconcile(t.Context(), reconcile.Request{NamespacedName: k})
		require.NoError(t, err)
		var acc authv1beta1.NatsAccount
		require.NoError(t, kc.Get(t.Context(), k, &acc))
		return acc
	}
	accept := func(name string) {
		t.Helper()
		var acc authv1beta1.NatsAccount
		require.NoError(t, kc.Get(t.Context(), client.ObjectKey{Namespace: testOperator.Namespace, Name: name}, &acc))
		acc.Spec.Adoption = &authv1beta1.Adoption{DroppedClaims: authv1beta1.AdoptionAcceptDroppedClaims}
		require.NoError(t, kc.Update(t.Context(), &acc))
	}
	pubOf := func(acc authv1beta1.NatsAccount) string { return acc.Status.PublicKey }

	acc := reconciled("mapped")
	ready := meta.FindStatusCondition(acc.Status.Conditions, authctl.ConditionReady)
	require.Equal(t, metav1.ConditionFalse, ready.Status)
	require.Equal(t, authctl.ReasonAdoptionDropsClaims, ready.Reason)
	require.Contains(t, ready.Message, "mappings")
	require.NotContains(t, ready.Message, "limits.subs", "a limit set to another value is a change")
	require.Empty(t, acc.Status.JWT)
	require.Equal(t, made["mapped"], c.held(0, pubOf(acc)), "the servers keep the JWT made elsewhere")

	acc = reconciled("mapped")
	require.Equal(t, authctl.ReasonAdoptionDropsClaims, meta.FindStatusCondition(acc.Status.Conditions, authctl.ConditionReady).Reason, "refused again while spec stands")
	accept("mapped")
	acc = reconciled("mapped")
	require.True(t, meta.IsStatusConditionTrue(acc.Status.Conditions, authctl.ConditionReady))
	require.Equal(t, acc.Status.JWT, c.held(0, pubOf(acc)), "accepted, signed and pushed")
	signed, err := jwt.DecodeAccountClaims(acc.Status.JWT)
	require.NoError(t, err)
	require.Empty(t, signed.Mappings)
	require.EqualValues(t, 20, signed.Limits.Subs)

	acc = reconciled("changed")
	require.True(t, meta.IsStatusConditionTrue(acc.Status.Conditions, authctl.ConditionReady), "a changed value is not a drop")
	require.Equal(t, acc.Status.JWT, c.held(0, pubOf(acc)))

	accept("tiered")
	acc = reconciled("tiered")
	ready = meta.FindStatusCondition(acc.Status.Conditions, authctl.ConditionReady)
	require.Equal(t, metav1.ConditionFalse, ready.Status)
	require.Equal(t, authctl.ReasonTierInexpressible, ready.Reason)
	require.Contains(t, ready.Message, "R7")
	require.Empty(t, acc.Status.JWT)
	require.Equal(t, made["tiered"], c.held(0, pubOf(acc)))
}
