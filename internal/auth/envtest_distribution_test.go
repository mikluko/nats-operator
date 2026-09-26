package auth_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/jwt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/auth"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// TestEnvtestDistribution runs the reconcilers with Resolvers over the
// SystemConnection cmd/auth-controller wires, against three routed
// nats-servers with full resolvers booted from the operator the auth
// controller signed: story 2's status, re-signing at half the TTL, jwtTTL:
// 0, and deletion.
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
	})
	require.NoError(t, err)
	pool := natsconn.NewPool()
	conn := &auth.SystemConnection{Reader: mgr.GetClient(), Pool: pool, Name: key("nats-system", "system")}
	resolvers := &auth.Resolvers{Conn: conn.Conn, Wait: time.Second, Interval: 300 * time.Millisecond}
	require.NoError(t, mgr.Add(pool))
	require.NoError(t, mgr.Add(resolvers))
	require.NoError(t, auth.Setup(t.Context(), mgr, resolvers, auth.ConnSessions{Conn: conn.Conn, Wait: 300 * time.Millisecond}))
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

	// No server exists yet, so every push is unreachable: the system
	// account JWT the servers boot from reaches status all the same.
	op := &authv1beta1.NatsOperator{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, demo, op)
		assert.NotNil(ct, op.Status.SystemAccount)
	})
	oc, err := jwt.DecodeOperatorClaims(op.Status.JWT)
	require.NoError(t, err)
	cl := startFullCluster(t, plane{opJWT: op.Status.JWT, sysJWT: op.Status.SystemAccount.JWT, sysPub: oc.SystemAccount}, 3)

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
---
apiVersion: nats.mikluko.io/v1beta1
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
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata:
  name: short
  namespace: nats-system
spec:
  operatorRef:
    name: demo
  jwtTTL: 4s
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata:
  name: forever
  namespace: nats-system
spec:
  operatorRef:
    name: demo
  jwtTTL: 0s
`)

	t.Run("Story2", func(t *testing.T) {
		orders := &authv1beta1.NatsAccount{}
		sys := &authv1beta1.NatsSystemAccount{}
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, key("nats-system", "orders"), orders)
			e.get(ct, key("nats-system", "sys"), sys)
			distributed(ct, orders.Status.Conditions, orders.Status.Distribution)
			distributed(ct, sys.Status.Conditions, sys.Status.Distribution)
			ready(ct, orders.Status.Conditions, orders.Generation, auth.ReasonDistributed)
		})
		for i := range cl.srvs {
			require.Equal(t, orders.Status.JWT, cl.held(i, orders.Status.PublicKey), "server %d", i)
		}
		require.NotNil(t, orders.Status.Distribution.LastPushTime)
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

	t.Run("Deletion", func(t *testing.T) {
		pubs := map[string]string{}
		for _, name := range []string{"orders", "forever"} {
			var acc authv1beta1.NatsAccount
			require.NoError(t, c.Get(t.Context(), key("nats-system", name), &acc))
			require.Contains(t, acc.Finalizers, auth.AccountFinalizer)
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

// distributed asserts story 2's distribution status: every server holds
// the current JWT.
func distributed(ct *assert.CollectT, conds []metav1.Condition, d *authv1beta1.Distribution) {
	cond := meta.FindStatusCondition(conds, auth.ConditionDistributed)
	if assert.NotNil(ct, cond) {
		assert.Equal(ct, metav1.ConditionTrue, cond.Status, cond.Message)
		assert.Equal(ct, auth.ReasonAllServersCurrent, cond.Reason)
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
