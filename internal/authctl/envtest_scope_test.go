package authctl_test

import (
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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/authctl"
)

// testScopedSigningKey pins that an account taken over with a scoped signing
// key keeps the scope in its JWT, that a user the key signed before the
// takeover and a NatsUser of the scope's role are both held to it by a
// nats-server, and that the schema refuses a scope on a NATS operator's key
// and a role beside permissions.
func (e *env) testScopedSigningKey(t *testing.T) {
	const ns = "nats-system"
	pubs := map[string]string{}
	for _, name := range []string{"scoped-id", "scoped-plain", "scoped-reader"} {
		pubs[name] = e.seedSecret(t, ns, name, nkeys.PrefixByteAccount)
	}
	var sec corev1.Secret
	require.NoError(t, e.c.Get(t.Context(), key(ns, "scoped-reader"), &sec))
	readerKey, err := nkeys.FromSeed(sec.Data[authctl.SeedKey])
	require.NoError(t, err)
	before, err := nkeys.CreateUser()
	require.NoError(t, err)
	beforePub, err := before.PublicKey()
	require.NoError(t, err)
	beforeSeed, err := before.Seed()
	require.NoError(t, err)
	uc := jwt.NewUserClaims(beforePub)
	uc.IssuerAccount = pubs["scoped-id"]
	uc.SetScoped(true)
	beforeJWT, err := uc.Encode(readerKey)
	require.NoError(t, err)

	e.apply(t, `
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsAccount
metadata: {name: scoped, namespace: nats-system}
spec:
  operatorRef: {name: demo}
  keys:
    identity: {secretKeyRef: {name: scoped-id, key: seed}}
    signing:
      - name: plain
        secretKeyRef: {name: scoped-plain, key: seed}
      - name: reader
        secretKeyRef: {name: scoped-reader, key: seed}
        scope:
          role: reader
          permissions:
            publish: {allow: ["allowed.>"]}
            subscribe: {allow: ["allowed.>"]}
          connectionTypes: [STANDARD]
          limits: {subscriptions: 2, payload: 1Ki}
---
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsUser
metadata: {name: scoped-reader, namespace: nats-system}
spec:
  accountRef: {kind: NatsAccount, name: scoped}
  role: reader
---
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsUser
metadata: {name: scoped-plain, namespace: nats-system}
spec:
  accountRef: {kind: NatsAccount, name: scoped}
---
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsUser
metadata: {name: scoped-norole, namespace: nats-system}
spec:
  accountRef: {kind: NatsAccount, name: scoped}
  role: writer
`)

	var acc authv1beta1.NatsAccount
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key(ns, "scoped"), &acc)
		ready(ct, acc.Status.Conditions, acc.Generation, authctl.ReasonSigned)
		assert.Equal(ct, pubs["scoped-id"], acc.Status.PublicKey)
	})
	ac, err := jwt.DecodeAccountClaims(acc.Status.JWT)
	require.NoError(t, err)
	plain, listed := ac.SigningKeys.GetScope(pubs["scoped-plain"])
	require.True(t, listed)
	require.Nil(t, plain)
	scope, _ := ac.SigningKeys.GetScope(pubs["scoped-reader"])
	want := jwt.NewUserScope()
	want.Key, want.Role = pubs["scoped-reader"], "reader"
	want.Template.Pub.Allow.Add("allowed.>")
	want.Template.Sub.Allow.Add("allowed.>")
	want.Template.AllowedConnectionTypes.Add(jwt.ConnectionTypeStandard)
	want.Template.Subs, want.Template.Payload = 2, 1024
	require.Equal(t, want, scope)

	var readerJWT, readerSeed string
	e.eventually(t, func(ct *assert.CollectT) {
		readerJWT, readerSeed, _ = e.creds(ct, key(ns, "scoped-reader-creds"))
		token, _, _ := e.creds(ct, key(ns, "scoped-plain-creds"))
		var norole authv1beta1.NatsUser
		e.get(ct, key(ns, "scoped-norole"), &norole)
		notReady(ct, norole.Status.Conditions, authctl.ReasonInvalidJWT)
		if readerJWT == "" || token == "" {
			return
		}
		c, err := jwt.DecodeUserClaims(token)
		if assert.NoError(ct, err) {
			assert.Equal(ct, pubs["scoped-plain"], c.Issuer)
		}
	})
	rc, err := jwt.DecodeUserClaims(readerJWT)
	require.NoError(t, err)
	require.Equal(t, pubs["scoped-reader"], rc.Issuer)
	require.True(t, rc.HasEmptyPermissions())

	for name, manifest := range map[string]string{
		"role beside permissions": `
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsUser
metadata: {name: scoped-both, namespace: nats-system}
spec:
  accountRef: {kind: NatsAccount, name: scoped}
  role: reader
  permissions: {publish: {allow: ["x"]}}
`,
		"scope on a NATS operator's key": `
apiVersion: auth.nats-operator.io/v1beta1
kind: NatsOperator
metadata: {name: scoped, namespace: nats-system}
spec:
  keys:
    signing:
      - name: k
        secretKeyRef: {name: k, key: seed}
        scope: {role: reader}
  systemAccountRef: {name: sys}
`,
	} {
		var m map[string]any
		require.NoError(t, yaml.Unmarshal([]byte(manifest), &m))
		err := e.c.Create(t.Context(), &unstructured.Unstructured{Object: m})
		require.True(t, apierrors.IsInvalid(err), "%s: %v", name, err)
	}

	var trust natsv1beta1.NatsOperatorTrust
	require.NoError(t, e.c.Get(t.Context(), demo, &trust))
	oc, err := jwt.DecodeOperatorClaims(trust.Status.OperatorJWT)
	require.NoError(t, err)
	res := &server.MemAccResolver{}
	require.NoError(t, res.Store(oc.SystemAccount, trust.Status.SystemAccountJWT))
	require.NoError(t, res.Store(acc.Status.PublicKey, acc.Status.JWT))
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

	for name, u := range map[string][2]string{
		"signed before the takeover": {beforeJWT, string(beforeSeed)},
		"NatsUser of the role":       {readerJWT, readerSeed},
	} {
		t.Run(name, func(t *testing.T) {
			requireHeldToScope(t, srv.ClientURL(), u[0], u[1])
		})
	}
}

// requireHeldToScope connects the user and requires the server to hold it
// to testScopedSigningKey's reader scope: publish and subscribe under
// allowed.> only, and two subscriptions.
func requireHeldToScope(t *testing.T, url, token, seed string) {
	t.Helper()
	nc, err := nats.Connect(url, nats.UserJWTAndSeed(token, seed), nats.NoReconnect(),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	sub, err := nc.SubscribeSync("allowed.a")
	require.NoError(t, err)
	require.NoError(t, nc.Publish("allowed.a", []byte("in")))
	_, err = sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	require.NoError(t, nc.LastError())

	require.NoError(t, nc.Publish("forbidden.a", nil))
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assert.ErrorContains(ct, nc.LastError(), `Permissions Violation for Publish to "forbidden.a"`)
	}, 5*time.Second, 20*time.Millisecond)

	_, err = nc.SubscribeSync("allowed.b")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())
	require.False(t, nc.IsClosed())
	_, err = nc.SubscribeSync("allowed.c")
	require.NoError(t, err)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assert.ErrorContains(ct, nc.LastError(), "maximum subscriptions exceeded")
	}, 5*time.Second, 20*time.Millisecond)
}
