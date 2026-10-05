package authctl

import (
	"context"
	"fmt"
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
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// nscSystem is a NATS operator and a system account as nsc makes them: the
// system account has a signing key, the two monitoring exports, a user sys
// and a revoked user old, both signed by the signing key.
type nscSystem struct {
	opJWT, sysJWT, sysPub, sysSigningPub string
	sys, old                             nscSystemUser
	// opSigning is the NATS operator's signing key, which signed sysJWT.
	opSigning nkeys.KeyPair
	// c holds the NatsOperator op and the NatsSystemAccount sys of namespace
	// ns, adopting the keys through the Secret nsc, and the accounts given
	// to newNscSystem.
	c client.Client
}

type nscSystemUser struct {
	pub, jwt string
	seed     []byte
}

func (u nscSystemUser) connect(url string) (*nats.Conn, error) {
	return nats.Connect(url, nats.UserJWTAndSeed(u.jwt, string(u.seed)), nats.NoReconnect())
}

var (
	takeoverOperator = types.NamespacedName{Namespace: "ns", Name: "op"}
	takeoverSystem   = types.NamespacedName{Namespace: "ns", Name: "sys"}
)

func newNscSystem(t *testing.T, accounts ...*authv1beta1.NatsAccount) *nscSystem {
	t.Helper()
	pair := func(prefix nkeys.PrefixByte) (nkeys.KeyPair, string, []byte) {
		kp, err := nkeys.CreatePair(prefix)
		require.NoError(t, err)
		pub, err := kp.PublicKey()
		require.NoError(t, err)
		seed, err := kp.Seed()
		require.NoError(t, err)
		return kp, pub, seed
	}
	opID, opPub, opSeed := pair(nkeys.PrefixByteOperator)
	opSK, opSKPub, opSKSeed := pair(nkeys.PrefixByteOperator)
	_, sysPub, sysSeed := pair(nkeys.PrefixByteAccount)
	sysSK, sysSKPub, sysSKSeed := pair(nkeys.PrefixByteAccount)
	n := &nscSystem{sysPub: sysPub, sysSigningPub: sysSKPub, opSigning: opSK}

	oc := jwt.NewOperatorClaims(opPub)
	oc.Name = "nsc"
	oc.SystemAccount = sysPub
	oc.SigningKeys.Add(opSKPub)
	var err error
	n.opJWT, err = oc.Encode(opID)
	require.NoError(t, err)

	user := func() nscSystemUser {
		_, pub, seed := pair(nkeys.PrefixByteUser)
		uc := jwt.NewUserClaims(pub)
		uc.IssuerAccount = sysPub
		token, err := uc.Encode(sysSK)
		require.NoError(t, err)
		return nscSystemUser{pub: pub, jwt: token, seed: seed}
	}
	n.sys, n.old = user(), user()

	sc := jwt.NewAccountClaims(sysPub)
	sc.Name = "SYS"
	sc.SigningKeys.Add(sysSKPub)
	sc.Exports = jwtplane.MonitoringExports()
	sc.Revoke(n.old.pub)
	n.sysJWT, err = sc.Encode(opSK)
	require.NoError(t, err)

	s := testScheme(t)
	require.NoError(t, natsv1beta1.AddToScheme(s))
	keys := func(name string) *authv1beta1.Keys {
		ref := func(key string) authv1beta1.SeedSecretKeySelector {
			return authv1beta1.SeedSecretKeySelector{Name: "nsc", Key: name + "-" + key}
		}
		return &authv1beta1.Keys{
			Identity: &authv1beta1.IdentityKey{SecretKeyRef: ref("identity")},
			Signing:  []authv1beta1.SigningKey{{Name: "signing-1", SecretKeyRef: ref("signing")}},
		}
	}
	op := &authv1beta1.NatsOperator{
		ObjectMeta: metav1.ObjectMeta{Namespace: takeoverOperator.Namespace, Name: takeoverOperator.Name, UID: "op"},
		Spec:       authv1beta1.NatsOperatorSpec{Keys: keys("op"), SystemAccountRef: natsv1beta1.ObjectReference{Name: takeoverSystem.Name}},
	}
	sys := &authv1beta1.NatsSystemAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: takeoverSystem.Namespace, Name: takeoverSystem.Name, UID: "sys"},
		Spec:       authv1beta1.NatsSystemAccountSpec{Keys: keys("sys"), OperatorRef: natsv1beta1.ObjectReference{Name: takeoverOperator.Name}},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "nsc"},
		Data:       map[string][]byte{"op-identity": opSeed, "op-signing": opSKSeed, "sys-identity": sysSeed, "sys-signing": sysSKSeed},
	}
	withStatus := []client.Object{op, sys}
	for _, a := range accounts {
		withStatus = append(withStatus, a)
	}
	b := fake.NewClientBuilder().WithScheme(s).WithObjects(append(withStatus, secret)...).WithStatusSubresource(withStatus...)
	require.NoError(t, indexes(t.Context(), builderIndexer{b}))
	n.c = b.Build()
	return n
}

// serve starts a server with a full resolver holding the system account
// and accountJWTs, and returns its client URL.
func (n *nscSystem) serve(t *testing.T, accountJWTs ...string) string {
	t.Helper()
	res, err := server.NewDirAccResolver(t.TempDir(), 0, time.Hour, server.HardDelete)
	require.NoError(t, err)
	require.NoError(t, res.Store(n.sysPub, n.sysJWT))
	for _, token := range accountJWTs {
		c, err := jwt.DecodeAccountClaims(token)
		require.NoError(t, err)
		require.NoError(t, res.Store(c.Subject, token))
	}
	oc, err := jwt.DecodeOperatorClaims(n.opJWT)
	require.NoError(t, err)
	s, err := server.NewServer(&server.Options{
		ServerName:       "s0",
		Host:             "127.0.0.1",
		Port:             -1,
		TrustedOperators: []*jwt.OperatorClaims{oc},
		SystemAccount:    n.sysPub,
		AccountResolver:  res,
		NoLog:            true,
		NoSigs:           true,
	})
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	require.True(t, s.ReadyForConnections(10*time.Second))
	return s.ClientURL()
}

func (n *nscSystem) reconcileOperator(t *testing.T, d Distributor) (reconcile.Result, *authv1beta1.NatsOperator) {
	t.Helper()
	res, err := (&OperatorReconciler{Client: n.c, Distributor: d}).Reconcile(t.Context(), reconcile.Request{NamespacedName: takeoverOperator})
	require.NoError(t, err)
	op := &authv1beta1.NatsOperator{}
	require.NoError(t, n.c.Get(t.Context(), takeoverOperator, op))
	return res, op
}

func (n *nscSystem) reconcileSystemAccount(t *testing.T, d Distributor) *authv1beta1.NatsSystemAccount {
	t.Helper()
	_, err := (&SystemAccountReconciler{Client: n.c, Distributor: d}).Reconcile(t.Context(), reconcile.Request{NamespacedName: takeoverSystem})
	require.NoError(t, err)
	sys := &authv1beta1.NatsSystemAccount{}
	require.NoError(t, n.c.Get(t.Context(), takeoverSystem, sys))
	return sys
}

// TestSystemAccountTakeover pins that the first JWT signed for a system
// account made with nsc keeps the revocations of the JWT the servers hold
// and the two monitoring exports, so a user revoked under nsc stays refused.
func TestSystemAccountTakeover(t *testing.T) {
	n := newNscSystem(t)
	url := n.serve(t)
	_, err := n.old.connect(url)
	require.ErrorIs(t, err, nats.ErrAuthorization, "revoked under nsc")
	nc, err := n.sys.connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	d := &Resolvers{Conn: func(context.Context, types.NamespacedName) (*nats.Conn, error) { return nc, nil }, Wait: 500 * time.Millisecond}

	res, op := n.reconcileOperator(t, d)
	require.Zero(t, res.RequeueAfter)
	require.Nil(t, meta.FindStatusCondition(op.Status.Conditions, ConditionRevocationsUnrecovered))
	sys := n.reconcileSystemAccount(t, d)

	held, err := d.Lookup(t.Context(), takeoverOperator, n.sysPub)
	require.NoError(t, err)
	require.Equal(t, op.Status.SystemAccount.JWT, held)
	require.NotEqual(t, n.sysJWT, held)
	c, err := jwt.DecodeAccountClaims(held)
	require.NoError(t, err)
	require.Contains(t, c.Revocations, n.old.pub)
	require.ElementsMatch(t, jwtplane.MonitoringExports(), c.Exports)
	require.Len(t, sys.Status.Revocations, 1)
	require.Equal(t, n.old.pub, sys.Status.Revocations[0].PublicKey)
	require.Equal(t, []string{n.sysSigningPub}, sys.Status.Revocations[0].Issuers)

	_, err = n.old.connect(url)
	require.ErrorIs(t, err, nats.ErrAuthorization, "revoked after the takeover")
	again, err := n.sys.connect(url)
	require.NoError(t, err)
	again.Close()
}

// lookupDistributor answers Lookup with held, or with err where it is set.
type lookupDistributor struct {
	countingDistributor
	held string
	err  error
}

func (d *lookupDistributor) Lookup(context.Context, types.NamespacedName, string) (string, error) {
	return d.held, d.err
}

// TestSystemAccountTakeover_ServersSilent pins that a system account first
// signed while its servers cannot be asked is not pushed, and is asked for
// and signed again with their revocations once they answer.
func TestSystemAccountTakeover_ServersSilent(t *testing.T) {
	n := newNscSystem(t)
	d := &lookupDistributor{err: fmt.Errorf("%w: down", ErrUnreachable)}

	res, op := n.reconcileOperator(t, d)
	require.Equal(t, distributionRecheck, res.RequeueAfter)
	require.True(t, unrecovered(op.Status.Conditions))
	n.reconcileSystemAccount(t, d)
	require.Zero(t, d.pushes)

	d.held, d.err = n.sysJWT, nil
	res, op = n.reconcileOperator(t, d)
	require.Zero(t, res.RequeueAfter)
	require.Nil(t, meta.FindStatusCondition(op.Status.Conditions, ConditionRevocationsUnrecovered))
	c, err := jwt.DecodeAccountClaims(op.Status.SystemAccount.JWT)
	require.NoError(t, err)
	require.Contains(t, c.Revocations, n.old.pub)
	n.reconcileSystemAccount(t, d)
	require.Equal(t, 1, d.pushes)
}
