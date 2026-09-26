package auth_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/auth"
	"github.com/mikluko/nats-operator/internal/grant"
)

// errNoSystemConnection is what the env's Sessions answer before a subtest
// hands them a system connection.
var errNoSystemConnection = errors.New("no system connection yet")

// systemConn is the env's ConnSessions.Conn: the connection a subtest
// stored, for any operator.
func (e *env) systemConn(context.Context, types.NamespacedName) (*nats.Conn, error) {
	if nc := e.sys.Load(); nc != nil {
		return nc, nil
	}
	return nil, errNoSystemConnection
}

// creds returns the JWT and seed of the creds file in Secret k under
// user.creds.
func (e *env) creds(ct assert.TestingT, k types.NamespacedName) (token, seed string, s *corev1.Secret) {
	s = &corev1.Secret{}
	if !assert.NoError(ct, e.c.Get(e.ctx, k, s)) {
		return "", "", nil
	}
	raw := s.Data[auth.DefaultCredentialsKey]
	token, err := jwt.ParseDecoratedJWT(raw)
	if !assert.NoError(ct, err) {
		return "", "", nil
	}
	kp, err := jwt.ParseDecoratedNKey(raw)
	if !assert.NoError(ct, err) {
		return "", "", nil
	}
	b, err := kp.Seed()
	if !assert.NoError(ct, err) {
		return "", "", nil
	}
	return token, string(b), s
}

// testStory2Users applies story 2's users: creds land in Secrets owned by
// their users, signed by the account's signing key; the bring-your-own-key
// user gets its JWT in status and no Secret; the controller presets sign
// into the system account.
func (e *env) testStory2Users(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(storiesDir, "02-auth-plane/01-natsusers.yaml"))
	require.NoError(t, err)
	e.apply(t, string(raw))

	var orders authv1beta1.NatsAccount
	var sys authv1beta1.NatsSystemAccount
	users := map[string]*authv1beta1.NatsUser{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("nats-system", "orders"), &orders)
		e.get(ct, key("nats-system", "sys"), &sys)
		for _, name := range []string{"orders-service", "orders-batch", "orders-jetstream", "cluster-controller", "jetstream-controller"} {
			u := &authv1beta1.NatsUser{}
			e.get(ct, key("nats-system", name), u)
			ready(ct, u.Status.Conditions, u.Generation, auth.ReasonSigned)
			users[name] = u
		}
	})

	for name, u := range users {
		require.Contains(t, u.Finalizers, auth.UserFinalizer, name)
	}

	batch := users["orders-batch"]
	require.Equal(t, "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4", batch.Status.PublicKey)
	bc, err := jwt.DecodeUserClaims(batch.Status.JWT)
	require.NoError(t, err)
	require.Equal(t, batch.Status.PublicKey, bc.Subject)
	require.Equal(t, orders.Status.PublicKey, bc.IssuerAccount)
	require.Equal(t, jwt.StringList{"orders.batch.>"}, bc.Pub.Allow)
	require.Zero(t, bc.Expires)
	var secrets corev1.SecretList
	require.NoError(t, e.c.List(t.Context(), &secrets))
	for _, s := range secrets.Items {
		require.NotContains(t, s.Name, "orders-batch", "a user bringing its own key gets no Secret")
	}

	for name, secret := range map[string]string{
		"orders-service":     "orders-service-creds",
		"orders-jetstream":   "orders-jetstream-creds",
		"cluster-controller": "cluster-controller-creds",
	} {
		token, _, s := e.creds(t, key("nats-system", secret))
		require.NotNil(t, s)
		require.True(t, metav1.IsControlledBy(s, users[name]), name)
		c, err := jwt.DecodeUserClaims(token)
		require.NoError(t, err)
		require.Equal(t, users[name].Status.PublicKey, c.Subject, name)
		require.Empty(t, users[name].Status.JWT, "a creds user's JWT stays in its Secret")
		wantAccount := orders.Status.PublicKey
		if name == "cluster-controller" {
			wantAccount = sys.Status.PublicKey
		}
		require.Equal(t, wantAccount, c.IssuerAccount, name)
		require.NotEqual(t, wantAccount, c.Issuer, "signed by a signing key, not the identity")
	}
	cc, err := jwt.DecodeUserClaims(func() string { tok, _, _ := e.creds(t, key("nats-system", "cluster-controller-creds")); return tok }())
	require.NoError(t, err)
	require.Contains(t, cc.Pub.Allow, "$SYS.REQ.SERVER.*.RELOAD", "the cluster-controller preset")

	before, _, _ := e.creds(t, key("nats-system", "orders-service-creds"))
	e.update(t, key("nats-system", "orders-service"), &authv1beta1.NatsUser{}, func(o client.Object) {
		o.SetAnnotations(map[string]string{"touched": "yes"})
	})
	require.Never(t, func() bool {
		after, _, _ := e.creds(t, key("nats-system", "orders-service-creds"))
		return after != before
	}, 2*time.Second, 100*time.Millisecond, "an unchanged user is not re-signed")

	e.update(t, key("nats-system", "orders-service"), &authv1beta1.NatsUser{}, func(o client.Object) {
		o.(*authv1beta1.NatsUser).Spec.Permissions.Publish.Allow = []string{"orders.>"}
	})
	e.eventually(t, func(ct *assert.CollectT) {
		token, _, _ := e.creds(ct, key("nats-system", "orders-service-creds"))
		c, err := jwt.DecodeUserClaims(token)
		if assert.NoError(ct, err) {
			assert.Equal(ct, jwt.StringList{"orders.>"}, c.Pub.Allow)
			assert.Equal(ct, users["orders-service"].Status.PublicKey, c.Subject, "the key is kept across re-signing")
		}
	})
}

// testStory4 applies story 4: users in the payments namespace attach to the
// payments account in nats-system through a grant, a user no grant covers is
// refused, and deleting the grant revokes the users it had admitted until it
// is restored.
func (e *env) testStory4(t *testing.T) {
	for _, f := range []string{"04-team-self-service/01-platform.yaml", "04-team-self-service/01-team.yaml"} {
		raw, err := os.ReadFile(filepath.Join(storiesDir, f))
		require.NoError(t, err)
		e.apply(t, string(raw))
	}
	e.apply(t, `
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata:
  name: payments-api
  namespace: orders
spec:
  accountRef:
    kind: NatsAccount
    namespace: nats-system
    name: payments
  credentials:
    secretKeyRef:
      name: payments-api-creds
`)

	var payments authv1beta1.NatsAccount
	api := &authv1beta1.NatsUser{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("nats-system", "payments"), &payments)
		e.get(ct, key("payments", "payments-api"), api)
		ready(ct, api.Status.Conditions, api.Generation, auth.ReasonSigned)
		token, _, _ := e.creds(ct, key("payments", "payments-api-creds"))
		c, err := jwt.DecodeUserClaims(token)
		if assert.NoError(ct, err) {
			assert.Equal(ct, payments.Status.PublicKey, c.IssuerAccount)
		}
	})

	raw, err := os.ReadFile(filepath.Join(storiesDir, "04-team-self-service/01-status-natsuser-payments-reader.yaml"))
	require.NoError(t, err)
	var want struct {
		Status authv1beta1.NatsUserStatus `json:"status"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &want))
	e.eventually(t, func(ct *assert.CollectT) {
		denied := &authv1beta1.NatsUser{}
		e.get(ct, key("orders", "payments-api"), denied)
		assert.Equal(ct, want.Status.ObservedGeneration, denied.Status.ObservedGeneration)
		for _, w := range want.Status.Conditions {
			got := meta.FindStatusCondition(denied.Status.Conditions, w.Type)
			if assert.NotNil(ct, got, w.Type) {
				assert.Equal(ct, w.Status, got.Status, w.Type)
				assert.Equal(ct, w.Reason, got.Reason, w.Type)
				if w.Message != "" {
					assert.Equal(ct, w.Message, got.Message, w.Type)
				}
			}
		}
		assert.Empty(ct, denied.Status.PublicKey)
	})
	require.True(t, apierrors.IsNotFound(e.c.Get(t.Context(), key("orders", "payments-api-creds"), &corev1.Secret{})),
		"a refused user gets no Secret")

	grantKey := key("nats-system", "payments-users")
	var g natsv1beta1.NatsReferenceGrant
	require.NoError(t, e.c.Get(t.Context(), grantKey, &g))
	require.NoError(t, e.c.Delete(t.Context(), &g))
	pub := api.Status.PublicKey
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("payments", "payments-api"), api)
		cond := meta.FindStatusCondition(api.Status.Conditions, grant.ConditionReferencesResolved)
		if assert.NotNil(ct, cond) {
			assert.Equal(ct, grant.ReasonNoGrant, cond.Reason)
		}
		e.get(ct, key("nats-system", "payments"), &payments)
		token, _, _ := e.creds(ct, key("payments", "payments-api-creds"))
		ac, err := jwt.DecodeAccountClaims(payments.Status.JWT)
		uc, uerr := jwt.DecodeUserClaims(token)
		if assert.NoError(ct, err) && assert.NoError(ct, uerr) {
			assert.True(ct, ac.IsClaimRevoked(uc), "the account JWT revokes a user whose grant is gone")
		}
	})

	g.ResourceVersion = ""
	require.NoError(t, e.c.Create(t.Context(), &g))
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("payments", "payments-api"), api)
		ready(ct, api.Status.Conditions, api.Generation, auth.ReasonSigned)
		e.get(ct, key("nats-system", "payments"), &payments)
		token, _, _ := e.creds(ct, key("payments", "payments-api-creds"))
		ac, err := jwt.DecodeAccountClaims(payments.Status.JWT)
		uc, uerr := jwt.DecodeUserClaims(token)
		if assert.NoError(ct, err) && assert.NoError(ct, uerr) {
			assert.Equal(ct, pub, uc.Subject, "the key is kept")
			assert.False(ct, ac.IsClaimRevoked(uc), "re-admitted, the user is re-signed past its revocation")
		}
	})
}

// testDeletion connects orders-service to a nats-server serving the auth
// plane, with the account JWTs the reconcilers push applied to it, and
// deletes the user: the account JWT revokes it, the finalizer holds the
// Secret until distribution is current and a kick pass through the
// auth-controller preset finds nothing, and the client is dropped.
func (e *env) testDeletion(t *testing.T) {
	var trust natsv1beta1.NatsOperatorTrust
	var orders authv1beta1.NatsAccount
	require.NoError(t, e.c.Get(t.Context(), demo, &trust))
	require.NoError(t, e.c.Get(t.Context(), key("nats-system", "orders"), &orders))
	oc, err := jwt.DecodeOperatorClaims(trust.Status.OperatorJWT)
	require.NoError(t, err)
	res := &server.MemAccResolver{}
	require.NoError(t, res.Store(oc.SystemAccount, trust.Status.SystemAccountJWT))
	require.NoError(t, res.Store(orders.Status.PublicKey, orders.Status.JWT))
	srv, err := server.NewServer(&server.Options{
		Host:             "127.0.0.1",
		Port:             -1,
		TrustedOperators: []*jwt.OperatorClaims{oc},
		SystemAccount:    oc.SystemAccount,
		AccountResolver:  res,
		NoLog:            true,
		NoSigs:           true,
	})
	require.NoError(t, err)
	go srv.Start()
	t.Cleanup(srv.Shutdown)
	require.True(t, srv.ReadyForConnections(10*time.Second))
	e.d.onPush(func(token string) {
		ac, err := jwt.DecodeAccountClaims(token)
		if err != nil {
			return
		}
		if acc, err := srv.LookupAccount(ac.Subject); err == nil {
			_ = res.Store(ac.Subject, token)
			srv.UpdateAccountClaims(acc, ac)
		}
	})
	t.Cleanup(func() { e.d.onPush(nil) })

	e.apply(t, `
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata:
  name: auth-controller
  namespace: nats-system
spec:
  accountRef:
    kind: NatsSystemAccount
    name: sys
  preset: auth-controller
  credentials:
    secretKeyRef:
      name: auth-controller-creds
`)
	var sysToken, sysSeed string
	e.eventually(t, func(ct *assert.CollectT) {
		sysToken, sysSeed, _ = e.creds(ct, key("nats-system", "auth-controller-creds"))
	})
	sysNC, err := nats.Connect(srv.ClientURL(), nats.UserJWTAndSeed(sysToken, sysSeed))
	require.NoError(t, err)
	t.Cleanup(sysNC.Close)
	e.sys.Store(sysNC)
	t.Cleanup(func() { e.sys.Store(nil) })

	token, seed, _ := e.creds(t, key("nats-system", "orders-service-creds"))
	closed := make(chan struct{})
	victim, err := nats.Connect(srv.ClientURL(), nats.UserJWTAndSeed(token, seed), nats.NoReconnect(),
		nats.ClosedHandler(func(*nats.Conn) { close(closed) }),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	require.NoError(t, err)
	t.Cleanup(victim.Close)

	userKey := key("nats-system", "orders-service")
	u := &authv1beta1.NatsUser{}
	require.NoError(t, e.c.Get(t.Context(), userKey, u))
	pub := u.Status.PublicKey
	require.NoError(t, e.c.Delete(t.Context(), u))

	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("nats-system", "orders"), &orders)
		ac, err := jwt.DecodeAccountClaims(orders.Status.JWT)
		if assert.NoError(ct, err) {
			assert.Contains(ct, ac.Revocations, pub)
		}
		e.get(ct, userKey, u)
		cond := meta.FindStatusCondition(u.Status.Conditions, auth.ConditionReady)
		if assert.NotNil(ct, cond) {
			assert.Equal(ct, auth.ReasonDistributing, cond.Reason)
		}
	})
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the revoked client was not dropped")
	}
	require.Never(t, func() bool {
		return apierrors.IsNotFound(e.c.Get(t.Context(), key("nats-system", "orders-service-creds"), &corev1.Secret{}))
	}, time.Second, 100*time.Millisecond, "the Secret is held until distribution is current")

	require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := e.c.Get(t.Context(), key("nats-system", "orders"), &orders); err != nil {
			return err
		}
		orders.Status.Distribution = &authv1beta1.Distribution{Servers: 1, Current: 1}
		return e.c.Status().Update(t.Context(), &orders)
	}))
	e.eventually(t, func(ct *assert.CollectT) {
		assert.True(ct, apierrors.IsNotFound(e.c.Get(e.ctx, userKey, &authv1beta1.NatsUser{})), "the user is gone")
		assert.True(ct, apierrors.IsNotFound(e.c.Get(e.ctx, key("nats-system", "orders-service-creds"), &corev1.Secret{})), "the Secret is gone")
	})

	require.Never(t, func() bool {
		var now authv1beta1.NatsAccount
		if err := e.c.Get(t.Context(), key("nats-system", "orders"), &now); err != nil {
			return true
		}
		ac, err := jwt.DecodeAccountClaims(now.Status.JWT)
		return err != nil || ac.Revocations[pub] == 0
	}, 2*time.Second, 100*time.Millisecond, "the revocation outlives the user")

	t.Run("SystemAccount", e.testSystemUserDeletion)
}

// testSystemUserDeletion deletes a user of a NatsSystemAccount whose
// status.distribution counts servers holding a JWT other than the revoking
// one the NatsOperator signed: the user is held, kick pass or not, until
// status.jwtHash names that JWT and every server holds it. The system
// account is kept from catching up by withdrawing the grant its
// cross-namespace operatorRef needs.
func (e *env) testSystemUserDeletion(t *testing.T) {
	for _, ns := range []string{"sysdel-op", "sysdel-sys"} {
		require.NoError(t, e.c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	}
	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	sysToOp := `
apiVersion: nats.mikluko.io/v1beta1
kind: NatsReferenceGrant
metadata: {name: sys-to-op, namespace: sysdel-op}
spec:
  from: [{group: auth.nats.mikluko.io, kind: NatsSystemAccount, namespace: sysdel-sys}]
  to: [{group: auth.nats.mikluko.io, kind: NatsOperator}]
`
	e.apply(t, sysToOp+`---
apiVersion: nats.mikluko.io/v1beta1
kind: NatsReferenceGrant
metadata: {name: op-to-sys, namespace: sysdel-sys}
spec:
  from: [{group: auth.nats.mikluko.io, kind: NatsOperator, namespace: sysdel-op}]
  to: [{group: auth.nats.mikluko.io, kind: NatsSystemAccount}]
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata: {name: ops, namespace: sysdel-op}
spec:
  systemAccountRef: {namespace: sysdel-sys, name: sys}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata: {name: sys, namespace: sysdel-sys}
spec:
  operatorRef: {namespace: sysdel-op, name: ops}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata: {name: held, namespace: sysdel-sys}
spec:
  accountRef: {kind: NatsSystemAccount, name: sys}
  publicKey: `+pub+`
`)
	opKey, sysKey, userKey := key("sysdel-op", "ops"), key("sysdel-sys", "sys"), key("sysdel-sys", "held")
	var op authv1beta1.NatsOperator
	var sys authv1beta1.NatsSystemAccount
	u := &authv1beta1.NatsUser{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, sysKey, &sys)
		ready(ct, sys.Status.Conditions, sys.Generation, auth.ReasonSigned)
		e.get(ct, userKey, u)
		ready(ct, u.Status.Conditions, u.Generation, auth.ReasonSigned)
	})

	var g natsv1beta1.NatsReferenceGrant
	require.NoError(t, e.c.Get(t.Context(), key("sysdel-op", "sys-to-op"), &g))
	require.NoError(t, e.c.Delete(t.Context(), &g))
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, sysKey, &sys)
		cond := meta.FindStatusCondition(sys.Status.Conditions, grant.ConditionReferencesResolved)
		if assert.NotNil(ct, cond) {
			assert.Equal(ct, grant.ReasonNoGrant, cond.Reason)
		}
	})
	setDistribution := func() {
		require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := e.c.Get(t.Context(), sysKey, &sys); err != nil {
				return err
			}
			sys.Status.Distribution = &authv1beta1.Distribution{Servers: 1, Current: 1}
			return e.c.Status().Update(t.Context(), &sys)
		}))
	}
	setDistribution()

	require.NoError(t, e.c.Delete(t.Context(), u))
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, opKey, &op)
		if assert.NotNil(ct, op.Status.SystemAccount) {
			ac, err := jwt.DecodeAccountClaims(op.Status.SystemAccount.JWT)
			if assert.NoError(ct, err) {
				assert.Contains(ct, ac.Revocations, pub)
			}
		}
		e.get(ct, userKey, u)
		cond := meta.FindStatusCondition(u.Status.Conditions, auth.ConditionReady)
		if assert.NotNil(ct, cond) {
			assert.Equal(ct, auth.ReasonDistributing, cond.Reason)
		}
	})
	require.Never(t, func() bool {
		return apierrors.IsNotFound(e.c.Get(t.Context(), userKey, &authv1beta1.NatsUser{}))
	}, 2*time.Second, 100*time.Millisecond, "a distribution counted for another JWT holds the user")

	g.ResourceVersion = ""
	require.NoError(t, e.c.Create(t.Context(), &g))
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, opKey, &op)
		e.get(ct, sysKey, &sys)
		if assert.NotNil(ct, op.Status.SystemAccount) {
			assert.Equal(ct, auth.JWTHash(op.Status.SystemAccount.JWT), sys.Status.JWTHash)
		}
	})
	setDistribution()
	e.eventually(t, func(ct *assert.CollectT) {
		assert.True(ct, apierrors.IsNotFound(e.c.Get(e.ctx, userKey, &authv1beta1.NatsUser{})), "the user is gone")
	})
}

// testRevocationRecord pins that an account's revocations are recorded in
// its status, that the JWT is rebuilt from the record when lost and the
// record from the JWT, that the system account's survive the loss of the
// NatsOperator's status.systemAccount once the user is gone, and that rotating out every signing
// key that may have issued a revoked JWT drops the revocation.
func (e *env) testRevocationRecord(t *testing.T) {
	ordersKey := key("nats-system", "orders")
	var orders authv1beta1.NatsAccount
	var recorded []authv1beta1.Revocation
	revokes := func(ct *assert.CollectT, accountJWT string, revs []authv1beta1.Revocation) {
		ac, err := jwt.DecodeAccountClaims(accountJWT)
		if !assert.NoError(ct, err) {
			return
		}
		for _, r := range revs {
			assert.Equal(ct, r.At.Unix(), ac.Revocations[r.PublicKey], r.PublicKey)
		}
	}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, ordersKey, &orders)
		recorded = orders.Status.Revocations
		if !assert.NotEmpty(ct, recorded, "UserDeletion left a revocation") {
			return
		}
		ac, err := jwt.DecodeAccountClaims(orders.Status.JWT)
		if assert.NoError(ct, err) {
			for _, r := range recorded {
				assert.ElementsMatch(ct, ac.SigningKeys.Keys(), r.Issuers)
			}
		}
		revokes(ct, orders.Status.JWT, recorded)
	})

	lose := func(mutate func(*authv1beta1.NatsAccountStatus)) {
		require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := e.c.Get(t.Context(), ordersKey, &orders); err != nil {
				return err
			}
			mutate(&orders.Status)
			return e.c.Status().Update(t.Context(), &orders)
		}))
	}
	lose(func(st *authv1beta1.NatsAccountStatus) { st.JWT = "" })
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, ordersKey, &orders)
		revokes(ct, orders.Status.JWT, recorded)
	})
	lose(func(st *authv1beta1.NatsAccountStatus) { st.Revocations = nil })
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, ordersKey, &orders)
		assert.Equal(ct, recorded, orders.Status.Revocations)
	})

	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	e.apply(t, `
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata: {name: revoked-system-user, namespace: nats-system}
spec:
  accountRef: {kind: NatsSystemAccount, name: sys}
  publicKey: `+pub+`
`)
	sysUserKey := key("nats-system", "revoked-system-user")
	sysUser := &authv1beta1.NatsUser{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, sysUserKey, sysUser)
		ready(ct, sysUser.Status.Conditions, sysUser.Generation, auth.ReasonSigned)
	})
	require.NoError(t, e.c.Delete(t.Context(), sysUser))
	var op authv1beta1.NatsOperator
	var sys authv1beta1.NatsSystemAccount
	sysRevoked := func(ct *assert.CollectT) {
		e.get(ct, demo, &op)
		e.get(ct, key("nats-system", "sys"), &sys)
		if assert.Len(ct, sys.Status.Revocations, 1) {
			assert.Equal(ct, pub, sys.Status.Revocations[0].PublicKey)
		}
		if assert.NotNil(ct, op.Status.SystemAccount) {
			revokes(ct, op.Status.SystemAccount.JWT, sys.Status.Revocations)
		}
	}
	e.eventually(t, sysRevoked)
	e.update(t, sysUserKey, &authv1beta1.NatsUser{}, func(o client.Object) { o.SetFinalizers(nil) })
	e.eventually(t, func(ct *assert.CollectT) {
		assert.True(ct, apierrors.IsNotFound(e.c.Get(e.ctx, sysUserKey, &authv1beta1.NatsUser{})))
	})
	require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := e.c.Get(t.Context(), demo, &op); err != nil {
			return err
		}
		op.Status.SystemAccount = nil
		return e.c.Status().Update(t.Context(), &op)
	}))
	e.eventually(t, sysRevoked)

	rotated := e.seedSecret(t, "nats-system", "orders-rotated", nkeys.PrefixByteAccount)
	e.update(t, ordersKey, &authv1beta1.NatsAccount{}, func(o client.Object) {
		o.(*authv1beta1.NatsAccount).Spec.Keys = &authv1beta1.Keys{
			Identity: &authv1beta1.IdentityKey{SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: "orders-account-identity", Key: auth.SeedKey}},
			Signing:  []authv1beta1.SigningKey{{Name: "rotated", SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: "orders-rotated", Key: auth.SeedKey}}},
		}
	})
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, ordersKey, &orders)
		ac, err := jwt.DecodeAccountClaims(orders.Status.JWT)
		if !assert.NoError(ct, err) {
			return
		}
		assert.Equal(ct, []string{rotated}, ac.SigningKeys.Keys())
		for _, r := range recorded {
			assert.NotContains(ct, ac.Revocations, r.PublicKey)
			for _, now := range orders.Status.Revocations {
				assert.NotEqual(ct, r.PublicKey, now.PublicKey)
			}
		}
	})
}
