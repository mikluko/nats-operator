package authctl

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

func seededPair(t *testing.T, prefix nkeys.PrefixByte) (kp nkeys.KeyPair, pub string, seed []byte) {
	t.Helper()
	kp, err := nkeys.CreatePair(prefix)
	require.NoError(t, err)
	pub, err = kp.PublicKey()
	require.NoError(t, err)
	seed, err = kp.Seed()
	require.NoError(t, err)
	return kp, pub, seed
}

// TestAdoption_UntrustedSigner adopts, with the identity seeds and a
// NATS operator signing key the servers' NATS operator JWT does not list, a
// NATS operator whose server holds an account made elsewhere: the server
// keeps the JWTs it serves, both accounts say why, and the NatsOperator
// names the key.
func TestAdoption_UntrustedSigner(t *testing.T) {
	opID, opPub, opIDSeed := seededPair(t, nkeys.PrefixByteOperator)
	listed, listedPub, _ := seededPair(t, nkeys.PrefixByteOperator)
	_, unlistedPub, unlistedSeed := seededPair(t, nkeys.PrefixByteOperator)
	sysID, sysPub, sysSeed := seededPair(t, nkeys.PrefixByteAccount)
	_, accPub, accSeed := seededPair(t, nkeys.PrefixByteAccount)

	oc := jwt.NewOperatorClaims(opPub)
	oc.SystemAccount = sysPub
	oc.SigningKeys.Add(listedPub)
	opJWT, err := oc.Encode(opID)
	require.NoError(t, err)
	sysJWT, err := jwt.NewAccountClaims(sysPub).Encode(listed)
	require.NoError(t, err)
	accJWT, err := jwt.NewAccountClaims(accPub).Encode(listed)
	require.NoError(t, err)

	res, err := server.NewDirAccResolver(t.TempDir(), 0, time.Hour, server.HardDelete)
	require.NoError(t, err)
	require.NoError(t, res.Store(sysPub, sysJWT))
	require.NoError(t, res.Store(accPub, accJWT))
	trusted, err := jwt.DecodeOperatorClaims(opJWT)
	require.NoError(t, err)
	s, err := server.NewServer(&server.Options{ServerName: "s0", Host: "127.0.0.1", Port: -1,
		TrustedOperators: []*jwt.OperatorClaims{trusted}, SystemAccount: sysPub, AccountResolver: res, NoLog: true, NoSigs: true})
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	require.True(t, s.ReadyForConnections(10*time.Second))

	_, userPub, userSeed := seededPair(t, nkeys.PrefixByteUser)
	userJWT, err := jwt.NewUserClaims(userPub).Encode(sysID)
	require.NoError(t, err)
	nc, err := nats.Connect(s.ClientURL(), nats.UserJWTAndSeed(userJWT, string(userSeed)), nats.NoReconnect())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	sch := testScheme(t)
	require.NoError(t, natsv1beta1.AddToScheme(sch))
	ref := func(key string) authv1beta1.SeedSecretKeySelector {
		return authv1beta1.SeedSecretKeySelector{Name: "seeds", Key: key}
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "seeds"}, Data: map[string][]byte{
		"op": opIDSeed, "op-signing": unlistedSeed, "sys": sysSeed, "acc": accSeed,
	}}
	op := &authv1beta1.NatsOperator{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "op", UID: "op"}, Spec: authv1beta1.NatsOperatorSpec{
		Keys: &authv1beta1.Keys{
			Identity: &authv1beta1.IdentityKey{SecretKeyRef: ref("op")},
			Signing:  []authv1beta1.SigningKey{{Name: "s", SecretKeyRef: ref("op-signing")}},
		},
		SystemAccountRef: natsv1beta1.ObjectReference{Name: "sys"},
	}}
	sys := &authv1beta1.NatsSystemAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "sys", UID: "sys"}, Spec: authv1beta1.NatsSystemAccountSpec{
		OperatorRef: natsv1beta1.ObjectReference{Name: "op"},
		Keys:        &authv1beta1.Keys{Identity: &authv1beta1.IdentityKey{SecretKeyRef: ref("sys")}},
	}}
	acc := &authv1beta1.NatsAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "acc", UID: "acc"}, Spec: authv1beta1.NatsAccountSpec{
		OperatorRef: natsv1beta1.ObjectReference{Name: "op"},
		Keys:        &authv1beta1.Keys{Identity: &authv1beta1.IdentityKey{SecretKeyRef: ref("acc")}},
	}}
	b := fake.NewClientBuilder().WithScheme(sch).WithObjects(secret, op, sys, acc).WithStatusSubresource(op, sys, acc)
	require.NoError(t, indexes(t.Context(), builderIndexer{b}))
	c := b.Build()

	d := &Resolvers{Conn: func(context.Context, types.NamespacedName) (*nats.Conn, error) { return nc, nil }, Wait: 500 * time.Millisecond}
	reconcilers := map[client.Object]reconcile.Reconciler{
		op:  &OperatorReconciler{Client: c, Distributor: d},
		sys: &SystemAccountReconciler{Client: c, Distributor: d},
		acc: &AccountReconciler{Client: c, Distributor: d},
	}
	for _, o := range []client.Object{op, op, sys, acc, op, sys} {
		_, err := reconcilers[o].Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(o)})
		require.NoError(t, err, "%T", o)
	}

	opKey := client.ObjectKeyFromObject(op)
	for pub, was := range map[string]string{sysPub: sysJWT, accPub: accJWT} {
		held, err := d.Lookup(t.Context(), opKey, pub)
		require.NoError(t, err)
		require.Equal(t, was, held, "the server was sent a JWT of %s that it does not trust", pub)
	}
	requireUntrusted := func(conds []metav1.Condition, dist *authv1beta1.Distribution) {
		t.Helper()
		cond := meta.FindStatusCondition(conds, ConditionDistributed)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionFalse, cond.Status, cond.Message)
		require.Equal(t, ReasonUntrustedSigner, cond.Reason, cond.Message)
		require.NotNil(t, dist)
		require.Equal(t, [2]int32{1, 0}, [2]int32{dist.Servers, dist.Current})
	}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(acc), acc))
	require.NotEmpty(t, acc.Status.JWT)
	requireUntrusted(acc.Status.Conditions, acc.Status.Distribution)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sys), sys))
	requireUntrusted(sys.Status.Conditions, sys.Status.Distribution)

	require.NoError(t, c.Get(t.Context(), opKey, op))
	cond := meta.FindStatusCondition(op.Status.Conditions, ConditionSigningKeyUntrusted)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
	require.Equal(t, ReasonUntrustedSigner, cond.Reason, cond.Message)
	require.Equal(t, "1 of 1 servers do not list signing key "+unlistedPub+" in their NATS operator JWT", cond.Message)
}
