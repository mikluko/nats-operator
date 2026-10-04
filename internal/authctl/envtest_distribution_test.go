package authctl_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/authctl"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

func TestEnvtestDistribution(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	te := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../config/crd"},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := te.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, te.Stop()) })

	ctrl.SetLogger(logr.Discard())
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, natsv1beta1.AddToScheme, authv1beta1.AddToScheme} {
		require.NoError(t, add(s))
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 s,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: ptr.To(true)},
		Client:                 manager.ClientOptions(),
	})
	require.NoError(t, err)
	pool := natsconn.NewPool(natsconn.WithPreset(jwtplane.PresetAuthController))
	conn := &authctl.SystemConnection{Reader: mgr.GetClient(), Pool: pool, Name: key("nats-system", "system")}
	resolvers := &authctl.Resolvers{Conn: conn.Conn, Wait: time.Second, Interval: 300 * time.Millisecond}
	require.NoError(t, mgr.Add(pool))
	require.NoError(t, mgr.Add(resolvers))
	require.NoError(t, authctl.Setup(t.Context(), mgr, resolvers, authctl.ConnSessions{Resolvers: resolvers}, nil))
	done := make(chan error, 1)
	go func() { done <- mgr.Start(t.Context()) }()
	t.Cleanup(func() { require.NoError(t, <-done) })

	c, err := client.New(cfg, client.Options{Scheme: s})
	require.NoError(t, err)
	e := &env{ctx: t.Context(), c: c}
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nats-system"}}))
	for _, f := range []string{"02-auth-plane/01-natsoperator.yaml", "02-auth-plane/01-natsaccounts.yaml"} {
		raw, err := os.ReadFile(filepath.Join(storiesDir, f))
		require.NoError(t, err)
		e.apply(t, string(raw))
	}

	op := &authv1beta1.NatsOperator{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, demo, op)
		assert.NotNil(ct, op.Status.SystemAccount)
	})
	fresh := &authv1beta1.NatsAccount{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("nats-system", "orders"), fresh)
		ready(ct, fresh.Status.Conditions, fresh.Generation, authctl.ReasonSigned)
		assert.NotEmpty(ct, fresh.Status.JWT)
		assert.True(ct, meta.IsStatusConditionTrue(fresh.Status.Conditions, authctl.ConditionRevocationsUnrecovered))
	})
	require.Nil(t, lastPush(fresh.Status.Distribution), "never distributed: signed without a server to ask")
	oc, err := jwt.DecodeOperatorClaims(op.Status.JWT)
	require.NoError(t, err)
	cl := startFullCluster(t, plane{opJWT: op.Status.JWT, sysJWT: op.Status.SystemAccount.JWT, sysPub: oc.SystemAccount}, 3)

	e.apply(t, `
apiVersion: auth.nats-operator.io/v1beta1
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
---
apiVersion: nats-operator.io/v1beta1
kind: NatsConnection
metadata:
  name: system
  namespace: nats-system
spec:
  servers: ["`+cl.srvs[0].ClientURL()+`"]
  credentials:
    secretKeyRef:
      name: auth-controller-creds
---
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsAccount
metadata:
  name: short
  namespace: nats-system
spec:
  operatorRef:
    name: demo
  jwtTTL: 4s
---
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsAccount
metadata:
  name: forever
  namespace: nats-system
spec:
  operatorRef:
    name: demo
  jwtTTL: 0s
`)

	t.Run("DistributedToEveryServer", func(t *testing.T) {
		orders := &authv1beta1.NatsAccount{}
		sys := &authv1beta1.NatsSystemAccount{}
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, key("nats-system", "orders"), orders)
			e.get(ct, key("nats-system", "sys"), sys)
			distributed(ct, orders.Status.Conditions, orders.Status.Distribution)
			distributed(ct, sys.Status.Conditions, sys.Status.Distribution)
			ready(ct, orders.Status.Conditions, orders.Generation, authctl.ReasonDistributed)
			assert.Nil(ct, meta.FindStatusCondition(orders.Status.Conditions, authctl.ConditionRevocationsUnrecovered), "the servers answered once up")
		})
		for i := range cl.srvs {
			require.Equal(t, orders.Status.JWT, cl.held(i, orders.Status.PublicKey), "server %d", i)
		}
		require.NotNil(t, orders.Status.Distribution.LastPushTime)
	})

	t.Run("SystemAccountPushLost", func(t *testing.T) {
		op := &authv1beta1.NatsOperator{}
		require.NoError(t, c.Get(t.Context(), demo, op))
		lost := resignedLater(t, e, op.Status.SystemAccount.JWT)
		require.NoError(t, resolvers.Push(t.Context(), demo, lost))
		e.update(t, demo, &authv1beta1.NatsOperator{}, func(o client.Object) {
			o.SetAnnotations(map[string]string{"test/nudge": "push-lost"})
		})
		sys := &authv1beta1.NatsSystemAccount{}
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, demo, op)
			e.get(ct, key("nats-system", "sys"), sys)
			assert.GreaterOrEqual(ct, issuedAt(ct, op.Status.SystemAccount.JWT), issuedAt(ct, lost))
			distributed(ct, sys.Status.Conditions, sys.Status.Distribution)
		})
		for i := range cl.srvs {
			require.Equal(t, op.Status.SystemAccount.JWT, cl.held(i, op.Status.SystemAccount.PublicKey), "server %d", i)
		}
	})

	t.Run("RenewedAtHalfTTL", func(t *testing.T) {
		short := &authv1beta1.NatsAccount{}
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, key("nats-system", "short"), short)
			distributed(ct, short.Status.Conditions, short.Status.Distribution)
		})
		first, err := jwt.DecodeAccountClaims(short.Status.JWT)
		require.NoError(t, err)
		require.Equal(t, int64(4), first.Expires-first.IssuedAt)
		var next *jwt.AccountClaims
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, key("nats-system", "short"), short)
			c, err := jwt.DecodeAccountClaims(short.Status.JWT)
			if assert.NoError(ct, err) && assert.NotEqual(ct, first.IssuedAt, c.IssuedAt) {
				next = c
			}
		})
		require.InDelta(t, 2, next.IssuedAt-first.IssuedAt, 1, "re-signed halfway through the first JWT's life")
		e.eventually(t, func(ct *assert.CollectT) {
			for i := range cl.srvs {
				c, err := jwt.DecodeAccountClaims(cl.held(i, next.Subject))
				if assert.NoError(ct, err) {
					assert.GreaterOrEqual(ct, c.IssuedAt, next.IssuedAt, "server %d holds the renewed JWT", i)
				}
			}
		})
	})

	t.Run("NoExpiryNeverResigned", func(t *testing.T) {
		forever := &authv1beta1.NatsAccount{}
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, key("nats-system", "forever"), forever)
			distributed(ct, forever.Status.Conditions, forever.Status.Distribution)
		})
		token := forever.Status.JWT
		c, err := jwt.DecodeAccountClaims(token)
		require.NoError(t, err)
		require.Zero(t, c.Expires)
		require.Never(t, func() bool {
			var now authv1beta1.NatsAccount
			return c.Subject == "" || e.c.Get(t.Context(), key("nats-system", "forever"), &now) != nil || now.Status.JWT != token
		}, 5*time.Second, 200*time.Millisecond, "a JWT that never expires is never re-signed")
		for i := range cl.srvs {
			require.Equal(t, token, cl.held(i, c.Subject), "server %d", i)
		}
	})

	t.Run("StatusLost", func(t *testing.T) {
		accountUser := revokedUser(t, e, "leaver", "kind: NatsAccount, name: orders")
		orders := &authv1beta1.NatsAccount{}
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, key("nats-system", "orders"), orders)
			assert.True(ct, revokesKey(orders.Status.JWT, accountUser))
		})
		require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := c.Get(t.Context(), key("nats-system", "orders"), orders); err != nil {
				return err
			}
			orders.Status = authv1beta1.NatsAccountStatus{}
			return c.Status().Update(t.Context(), orders)
		}))
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, key("nats-system", "orders"), orders)
			assert.True(ct, revokesKey(orders.Status.JWT, accountUser), "the JWT signed after the status was lost revokes the user")
			assert.True(ct, slices.ContainsFunc(orders.Status.Revocations, func(r authv1beta1.Revocation) bool { return r.PublicKey == accountUser }))
		})

		systemUser := revokedUser(t, e, "system-leaver", "kind: NatsSystemAccount, name: sys")
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, demo, op)
			if assert.NotNil(ct, op.Status.SystemAccount) {
				assert.True(ct, revokesKey(op.Status.SystemAccount.JWT, systemUser))
			}
		})
		loseSystemAccountStatus(t, e)
		sys := &authv1beta1.NatsSystemAccount{}
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, demo, op)
			e.get(ct, key("nats-system", "sys"), sys)
			if assert.NotNil(ct, op.Status.SystemAccount) {
				assert.True(ct, revokesKey(op.Status.SystemAccount.JWT, systemUser), "the system account JWT signed after both were lost revokes the user")
			}
			assert.True(ct, slices.ContainsFunc(sys.Status.Revocations, func(r authv1beta1.Revocation) bool { return r.PublicKey == systemUser }))
			ready(ct, op.Status.Conditions, op.Generation, authctl.ReasonSigned)
		})
	})

	t.Run("Deletion", func(t *testing.T) {
		pubs := map[string]string{}
		for _, name := range []string{"orders", "forever"} {
			var acc authv1beta1.NatsAccount
			require.NoError(t, c.Get(t.Context(), key("nats-system", name), &acc))
			require.Contains(t, acc.Finalizers, authctl.AccountFinalizer)
			pubs[name] = acc.Status.PublicKey
			require.NoError(t, c.Delete(t.Context(), &acc))
		}
		e.eventually(t, func(ct *assert.CollectT) {
			for name, pub := range pubs {
				assert.True(ct, apierrors.IsNotFound(c.Get(e.ctx, key("nats-system", name), &authv1beta1.NatsAccount{})), name)
				for i := range cl.srvs {
					assert.Empty(ct, cl.held(i, pub), "server %d still holds %s", i, name)
				}
			}
		})
		require.NoError(t, c.Get(t.Context(), demo, op))
		deleted := map[string]authv1beta1.DeletedAccount{}
		for _, d := range op.Status.DeletedAccounts {
			deleted[d.PublicKey] = d
		}
		require.Contains(t, deleted, pubs["orders"])
		require.NotNil(t, deleted[pubs["orders"]].Expires, "kept until its 48h JWT expires")
		require.Contains(t, deleted, pubs["forever"])
		require.Nil(t, deleted[pubs["forever"]].Expires, "kept for good")
	})
}

// loseSystemAccountStatus clears demo's status.systemAccount and the whole
// status of its NatsSystemAccount sys together: while demo names another
// system account, neither is rebuilt from the other.
func loseSystemAccountStatus(t *testing.T, e *env) {
	t.Helper()
	c := e.c
	op := &authv1beta1.NatsOperator{}
	e.update(t, demo, &authv1beta1.NatsOperator{}, func(o client.Object) {
		o.(*authv1beta1.NatsOperator).Spec.SystemAccountRef.Name = "absent"
	})
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, demo, op)
		cond := meta.FindStatusCondition(op.Status.Conditions, authctl.ConditionReady)
		if assert.NotNil(ct, cond) {
			assert.Equal(ct, authctl.ReasonNotFound, cond.Reason)
		}
	})
	sys := &authv1beta1.NatsSystemAccount{}
	require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := c.Get(t.Context(), key("nats-system", "sys"), sys); err != nil {
			return err
		}
		sys.Status = authv1beta1.NatsSystemAccountStatus{}
		return c.Status().Update(t.Context(), sys)
	}))
	require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := c.Get(t.Context(), demo, op); err != nil {
			return err
		}
		op.Status.SystemAccount = nil
		return c.Status().Update(t.Context(), op)
	}))
	e.update(t, demo, &authv1beta1.NatsOperator{}, func(o client.Object) {
		o.(*authv1beta1.NatsOperator).Spec.SystemAccountRef.Name = "sys"
	})
}

// revokedUser creates a NatsUser named name with a public key of its own
// in the account accountRef names, waits for it to be signed, deletes it,
// and returns its key once it is gone.
func revokedUser(t *testing.T, e *env, name, accountRef string) string {
	t.Helper()
	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	e.apply(t, `
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsUser
metadata: {name: `+name+`, namespace: nats-system}
spec:
  accountRef: {`+accountRef+`}
  publicKey: `+pub+`
`)
	u := &authv1beta1.NatsUser{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("nats-system", name), u)
		ready(ct, u.Status.Conditions, u.Generation, authctl.ReasonSigned)
	})
	require.NoError(t, e.c.Delete(t.Context(), u))
	e.eventually(t, func(ct *assert.CollectT) {
		assert.True(ct, apierrors.IsNotFound(e.c.Get(e.ctx, key("nats-system", name), &authv1beta1.NatsUser{})))
	})
	return pub
}

// resignedLater returns the claims of accountJWT signed again, by the
// NATS operator key in a Secret in nats-system that issued it, at least a second
// after it was issued: the JWT a push leaves on the servers when the status
// write recording it is lost.
func resignedLater(t *testing.T, e *env, accountJWT string) string {
	t.Helper()
	c, err := jwt.DecodeAccountClaims(accountJWT)
	require.NoError(t, err)
	var secrets corev1.SecretList
	require.NoError(t, e.c.List(t.Context(), &secrets, client.InNamespace("nats-system")))
	for _, s := range secrets.Items {
		kp, err := nkeys.FromSeed(s.Data[authctl.SeedKey])
		if err != nil {
			continue
		}
		if pub, err := kp.PublicKey(); err != nil || pub != c.Issuer {
			continue
		}
		time.Sleep(time.Until(time.Unix(c.IssuedAt+1, 0)))
		token, err := c.Encode(kp)
		require.NoError(t, err)
		return token
	}
	require.FailNow(t, "no Secret in nats-system holds the seed of "+c.Issuer)
	return ""
}

func issuedAt(t require.TestingT, accountJWT string) int64 {
	c, err := jwt.DecodeAccountClaims(accountJWT)
	require.NoError(t, err)
	return c.IssuedAt
}

func revokesKey(accountJWT, pub string) bool {
	c, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		return false
	}
	_, ok := c.Revocations[pub]
	return ok
}

// distributed asserts story 2's distribution status: every server holds
// the current JWT.
func distributed(ct *assert.CollectT, conds []metav1.Condition, d *authv1beta1.Distribution) {
	cond := meta.FindStatusCondition(conds, authctl.ConditionDistributed)
	if assert.NotNil(ct, cond) {
		assert.Equal(ct, metav1.ConditionTrue, cond.Status, cond.Message)
		assert.Equal(ct, authctl.ReasonAllServersCurrent, cond.Reason)
		assert.Equal(ct, "3 of 3 servers hold this JWT", cond.Message)
	}
	assert.Equal(ct, &authv1beta1.Distribution{Servers: 3, Current: 3, LastPushTime: lastPush(d)}, d)
}

func lastPush(d *authv1beta1.Distribution) *metav1.Time {
	if d == nil {
		return nil
	}
	return d.LastPushTime
}
