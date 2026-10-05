package authctl

import (
	"context"
	"fmt"
	"slices"
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
// and a revoked user old, both signed by the signing key, and a revoked user
// oldIdentity signed by the identity key.
type nscSystem struct {
	opJWT, sysJWT, sysPub, sysSigningPub string
	sys, old, oldIdentity                nscSystemUser
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
	adoptionOperator = types.NamespacedName{Namespace: "ns", Name: "op"}
	adoptionSystem   = types.NamespacedName{Namespace: "ns", Name: "sys"}
)

func newNscSystem(t *testing.T, accounts ...*authv1beta1.NatsAccount) *nscSystem {
	t.Helper()
	return newNscSystemWith(t, func(*jwt.AccountClaims) {}, accounts...)
}

// newNscSystemWith is newNscSystem with the system account's claims edited
// before they are signed.
func newNscSystemWith(t *testing.T, edit func(c *jwt.AccountClaims), accounts ...*authv1beta1.NatsAccount) *nscSystem {
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
	sysID, sysPub, sysSeed := pair(nkeys.PrefixByteAccount)
	sysSK, sysSKPub, sysSKSeed := pair(nkeys.PrefixByteAccount)
	n := &nscSystem{sysPub: sysPub, sysSigningPub: sysSKPub, opSigning: opSK}

	oc := jwt.NewOperatorClaims(opPub)
	oc.Name = "nsc"
	oc.SystemAccount = sysPub
	oc.SigningKeys.Add(opSKPub)
	var err error
	n.opJWT, err = oc.Encode(opID)
	require.NoError(t, err)

	user := func(issuer nkeys.KeyPair) nscSystemUser {
		_, pub, seed := pair(nkeys.PrefixByteUser)
		uc := jwt.NewUserClaims(pub)
		if issuer != sysID {
			uc.IssuerAccount = sysPub
		}
		token, err := uc.Encode(issuer)
		require.NoError(t, err)
		return nscSystemUser{pub: pub, jwt: token, seed: seed}
	}
	n.sys, n.old, n.oldIdentity = user(sysSK), user(sysSK), user(sysID)

	sc := jwt.NewAccountClaims(sysPub)
	sc.Name = "SYS"
	sc.SigningKeys.Add(sysSKPub)
	sc.Exports = jwtplane.MonitoringExports()
	sc.Revoke(n.old.pub)
	sc.Revoke(n.oldIdentity.pub)
	edit(sc)
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
		ObjectMeta: metav1.ObjectMeta{Namespace: adoptionOperator.Namespace, Name: adoptionOperator.Name, UID: "op"},
		Spec:       authv1beta1.NatsOperatorSpec{Keys: keys("op"), SystemAccountRef: natsv1beta1.ObjectReference{Name: adoptionSystem.Name}},
	}
	sys := &authv1beta1.NatsSystemAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: adoptionSystem.Namespace, Name: adoptionSystem.Name, UID: "sys"},
		Spec:       authv1beta1.NatsSystemAccountSpec{Keys: keys("sys"), OperatorRef: natsv1beta1.ObjectReference{Name: adoptionOperator.Name}},
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
	return n.serveSystem(t, n.sysJWT, accountJWTs...)
}

// serveSystem is serve with sysJWT in place of the system account JWT nsc
// made.
func (n *nscSystem) serveSystem(t *testing.T, sysJWT string, accountJWTs ...string) string {
	t.Helper()
	res, err := server.NewDirAccResolver(t.TempDir(), 0, time.Hour, server.HardDelete)
	require.NoError(t, err)
	require.NoError(t, res.Store(n.sysPub, sysJWT))
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
	res, err := (&OperatorReconciler{Client: n.c, Distributor: d}).Reconcile(t.Context(), reconcile.Request{NamespacedName: adoptionOperator})
	require.NoError(t, err)
	op := &authv1beta1.NatsOperator{}
	require.NoError(t, n.c.Get(t.Context(), adoptionOperator, op))
	return res, op
}

func (n *nscSystem) reconcileSystemAccount(t *testing.T, d Distributor) *authv1beta1.NatsSystemAccount {
	t.Helper()
	_, err := (&SystemAccountReconciler{Client: n.c, Distributor: d}).Reconcile(t.Context(), reconcile.Request{NamespacedName: adoptionSystem})
	require.NoError(t, err)
	sys := &authv1beta1.NatsSystemAccount{}
	require.NoError(t, n.c.Get(t.Context(), adoptionSystem, sys))
	return sys
}

// TestSystemAccountAdoption pins that the first JWT signed for a system
// account made with nsc keeps the revocations of the JWT the servers hold
// and the two monitoring exports, so a user revoked under nsc stays refused.
func TestSystemAccountAdoption(t *testing.T) {
	n := newNscSystem(t)
	url := n.serve(t)
	_, err := n.old.connect(url)
	require.ErrorIs(t, err, nats.ErrAuthorization, "revoked under nsc")
	_, err = n.oldIdentity.connect(url)
	require.ErrorIs(t, err, nats.ErrAuthorization, "revoked under nsc")
	nc, err := n.sys.connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	d := &Resolvers{Conn: func(context.Context, types.NamespacedName) (*nats.Conn, error) { return nc, nil }, Wait: 500 * time.Millisecond}

	res, op := n.reconcileOperator(t, d)
	require.Zero(t, res.RequeueAfter)
	require.Nil(t, meta.FindStatusCondition(op.Status.Conditions, ConditionRevocationsUnrecovered))
	sys := n.reconcileSystemAccount(t, d)

	held, err := d.Lookup(t.Context(), adoptionOperator, n.sysPub)
	require.NoError(t, err)
	require.Equal(t, op.Status.SystemAccount.JWT, held)
	require.NotEqual(t, n.sysJWT, held)
	c, err := jwt.DecodeAccountClaims(held)
	require.NoError(t, err)
	require.Contains(t, c.Revocations, n.old.pub)
	require.Contains(t, c.Revocations, n.oldIdentity.pub)
	require.ElementsMatch(t, jwtplane.MonitoringExports(), c.Exports)
	require.Len(t, sys.Status.Revocations, 2)
	for _, r := range sys.Status.Revocations {
		require.ElementsMatch(t, []string{n.sysPub, n.sysSigningPub}, r.Issuers, "recovered from the servers")
	}

	_, err = n.old.connect(url)
	require.ErrorIs(t, err, nats.ErrAuthorization, "revoked after the adoption")
	_, err = n.oldIdentity.connect(url)
	require.ErrorIs(t, err, nats.ErrAuthorization, "revoked after the adoption")
	again, err := n.sys.connect(url)
	require.NoError(t, err)
	again.Close()
}

// TestSystemAccountAdoption_SigningKeysRotated pins that a revocation
// recovered from the servers for a system account lists its identity key as
// an issuer, so a user the identity key signed stays refused once every
// signing key is rotated out.
func TestSystemAccountAdoption_SigningKeysRotated(t *testing.T) {
	n := newNscSystem(t)
	d := &lookupDistributor{held: n.sysJWT}
	n.reconcileOperator(t, d)
	n.reconcileSystemAccount(t, d)

	kp, err := nkeys.CreateAccount()
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)
	var secret corev1.Secret
	require.NoError(t, n.c.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "nsc"}, &secret))
	secret.Data["sys-signing-2"] = seed
	require.NoError(t, n.c.Update(t.Context(), &secret))
	sys := &authv1beta1.NatsSystemAccount{}
	require.NoError(t, n.c.Get(t.Context(), adoptionSystem, sys))
	sys.Spec.Keys.Signing = []authv1beta1.SigningKey{{Name: "signing-2", SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: "nsc", Key: "sys-signing-2"}}}
	require.NoError(t, n.c.Update(t.Context(), sys))

	for range 2 {
		n.reconcileOperator(t, d)
		sys = n.reconcileSystemAccount(t, d)
	}
	_, op := n.reconcileOperator(t, d)
	c, err := jwt.DecodeAccountClaims(op.Status.SystemAccount.JWT)
	require.NoError(t, err)
	require.NotContains(t, c.SigningKeys.Keys(), n.sysSigningPub, "rotated out")
	require.Contains(t, c.Revocations, n.oldIdentity.pub)
	i := slices.IndexFunc(sys.Status.Revocations, func(r authv1beta1.Revocation) bool { return r.PublicKey == n.oldIdentity.pub })
	require.GreaterOrEqual(t, i, 0)
	require.Contains(t, sys.Status.Revocations[i].Issuers, n.sysPub)

	_, err = n.oldIdentity.connect(n.serveSystem(t, op.Status.SystemAccount.JWT))
	require.ErrorIs(t, err, nats.ErrAuthorization)
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

// TestSystemAccountAdoption_DropsClaims pins that a system account whose
// JWT on the servers carries a claim no NatsSystemAccount expresses is not
// signed, its NatsOperator and the NatsSystemAccount both reading Ready
// False with the claim named, until spec.adoption.droppedClaims accepts the
// loss; and that a changed name is not a drop.
func TestSystemAccountAdoption_DropsClaims(t *testing.T) {
	n := newNscSystemWith(t, func(c *jwt.AccountClaims) { c.Description = "made with nsc" })
	d := &lookupDistributor{held: n.sysJWT}

	res, op := n.reconcileOperator(t, d)
	require.Zero(t, res.RequeueAfter)
	ready := meta.FindStatusCondition(op.Status.Conditions, ConditionReady)
	require.Equal(t, metav1.ConditionFalse, ready.Status)
	require.Equal(t, ReasonAdoptionDropsClaims, ready.Reason)
	require.Contains(t, ready.Message, "NatsSystemAccount ns/sys: ")
	require.Contains(t, ready.Message, "description")
	require.NotContains(t, ready.Message, "name")
	require.Nil(t, op.Status.SystemAccount)
	sys := n.reconcileSystemAccount(t, d)
	require.Zero(t, d.pushes)
	require.Equal(t, ready, meta.FindStatusCondition(sys.Status.Conditions, ConditionReady), "mirrored on the NatsSystemAccount")

	_, op = n.reconcileOperator(t, d)
	require.Equal(t, ReasonAdoptionDropsClaims, meta.FindStatusCondition(op.Status.Conditions, ConditionReady).Reason, "refused again while spec stands")

	require.NoError(t, n.c.Get(t.Context(), adoptionSystem, sys))
	sys.Spec.Adoption = &authv1beta1.Adoption{DroppedClaims: authv1beta1.AdoptionAcceptDroppedClaims}
	require.NoError(t, n.c.Update(t.Context(), sys))
	_, op = n.reconcileOperator(t, d)
	require.True(t, meta.IsStatusConditionTrue(op.Status.Conditions, ConditionReady))
	c, err := jwt.DecodeAccountClaims(op.Status.SystemAccount.JWT)
	require.NoError(t, err)
	require.Empty(t, c.Description)
	require.Contains(t, c.Revocations, n.old.pub)
	sys = n.reconcileSystemAccount(t, d)
	require.Equal(t, 1, d.pushes)
	require.True(t, meta.IsStatusConditionTrue(sys.Status.Conditions, ConditionReady))
}

// TestSystemAccountAdoption_ServersSilent pins that a system account first
// signed while its servers cannot be asked is not pushed, and is asked for
// and signed again with their revocations once they answer.
func TestSystemAccountAdoption_ServersSilent(t *testing.T) {
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
