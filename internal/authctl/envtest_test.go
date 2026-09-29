package authctl_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/authctl"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/manager"
)

const storiesDir = "../../docs/content/docs/stories"

// recorder is a Distributor that keeps every push and hands it to the hook
// onPush set, if any.
type recorder struct {
	mu     sync.Mutex
	pushes map[types.NamespacedName][]string
	hook   func(accountJWT string)
}

var _ authctl.Distributor = (*recorder)(nil)

func (r *recorder) Push(_ context.Context, operator types.NamespacedName, accountJWT string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pushes == nil {
		r.pushes = map[types.NamespacedName][]string{}
	}
	r.pushes[operator] = append(r.pushes[operator], accountJWT)
	if r.hook != nil {
		r.hook(accountJWT)
	}
	return nil
}

// Current answers ErrUnreachable: a recorder reaches no server.
func (r *recorder) Current(context.Context, types.NamespacedName, string) (authv1beta1.Distribution, error) {
	return authv1beta1.Distribution{}, authctl.ErrUnreachable
}

// Lookup answers the newest JWT for account pushed for operator, standing
// for the servers that would hold it.
func (r *recorder) Lookup(_ context.Context, operator types.NamespacedName, account string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var newest string
	var issued int64
	for _, token := range r.pushes[operator] {
		c, err := jwt.DecodeAccountClaims(token)
		if err == nil && c.Subject == account && c.IssuedAt >= issued {
			newest, issued = token, c.IssuedAt
		}
	}
	return newest, nil
}

func (r *recorder) Delete(context.Context, types.NamespacedName, string) error {
	return nil
}

// onPush sets the hook every later push is handed to; nil clears it.
func (r *recorder) onPush(hook func(accountJWT string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hook = hook
}

// pushed reports whether token was pushed for operator.
func (r *recorder) pushed(operator types.NamespacedName, token string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.pushes[operator], token)
}

// eventLog is an events.EventRecorder keeping each event as its type,
// reason and note.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

var _ events.EventRecorder = (*eventLog)(nil)

func (l *eventLog) Eventf(_, _ runtime.Object, eventtype, reason, _, note string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, eventtype+" "+reason+" "+fmt.Sprintf(note, args...))
}

// has reports whether an event reads e.
func (l *eventLog) has(e string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Contains(l.events, e)
}

// env is a running API server with the auth reconcilers against it.
type env struct {
	ctx context.Context
	c   client.Client
	d   *recorder
	log *eventLog
	// sys is the system connection the env's Sessions kick through.
	sys atomic.Pointer[nats.Conn]
}

func TestEnvtest(t *testing.T) {
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
		Client:                 manager.ClientOptions(),
	})
	require.NoError(t, err)
	e := &env{ctx: t.Context(), d: &recorder{}, log: &eventLog{}}
	sessions := authctl.ConnSessions{Resolvers: &authctl.Resolvers{Conn: e.systemConn, Wait: 500 * time.Millisecond}}
	require.NoError(t, authctl.Setup(t.Context(), mgr, e.d, sessions, e.log))
	done := make(chan error, 1)
	go func() { done <- mgr.Start(t.Context()) }()
	t.Cleanup(func() { require.NoError(t, <-done) })

	c, err := client.New(cfg, client.Options{Scheme: s})
	require.NoError(t, err)
	e.c = c
	for _, ns := range []string{"nats-system", "team-a", "rot", "offline", "flip", "payments", "orders", "foreign", "lost", "tenancy", "thief", "orphan", "gone", "squat", "keep", "adopt", "claim"} {
		require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	}
	for _, f := range []string{
		"02-auth-plane/01-natsoperator.yaml",
		"02-auth-plane/01-natsaccounts.yaml",
		"02-auth-plane/01-natsoperatortrust.yaml",
		"05-account-wiring/01-exporter.yaml",
		"05-account-wiring/01-importer.yaml",
	} {
		raw, err := os.ReadFile(filepath.Join(storiesDir, f))
		require.NoError(t, err)
		e.apply(t, string(raw))
	}

	t.Run("AuthPlaneSigned", e.testAuthPlaneSigned)
	t.Run("ExportsImported", e.testExportsImported)
	t.Run("ServedByNatsServer", e.testServed)
	t.Run("CrossNamespaceImport", e.testCrossNamespaceImport)
	t.Run("ImportFromOtherOperator", e.testImportFromOtherOperator)
	t.Run("NoExpiryAndAccountTrust", e.testNoExpiryAndAccountTrust)
	t.Run("Rotation", e.testRotation)
	t.Run("OfflineIdentities", e.testOfflineIdentities)
	t.Run("SystemAccountFlipAndStepdown", e.testFlipAndStepdown)
	t.Run("UserCreds", e.testUserCreds)
	t.Run("UsersUnderGrant", e.testUsersUnderGrant)
	t.Run("UserDeletion", e.testDeletion)
	t.Run("UserOfOrphanedAccount", e.testOrphanedAccountUser)
	t.Run("AccountDeletedWithOperator", e.testAccountDeletedWithOperator)
	t.Run("RevocationRecord", e.testRevocationRecord)
	t.Run("LostSeed", e.testLostSeed)
	t.Run("SeedsOutliveOwner", e.testSeedsOutliveOwner)
	t.Run("AccountKeyHeld", e.testAccountKeyHeld)
	t.Run("AccountKeySquatted", e.testAccountKeySquatted)
	t.Run("SystemKeyUnrecorded", e.testSystemKeyUnrecorded)
	t.Run("UserKeyHeld", e.testUserKeyHeld)
	t.Run("ReplacedUserKey", e.testReplacedUserKey)
	t.Run("ReplacedKeyRefused", e.testReplacedKeyRefused)
}

var demo = types.NamespacedName{Namespace: "nats-system", Name: "demo"}

func (e *env) testAuthPlaneSigned(t *testing.T) {
	op := &authv1beta1.NatsOperator{}
	sys := &authv1beta1.NatsSystemAccount{}
	orders := &authv1beta1.NatsAccount{}
	trust := &natsv1beta1.NatsOperatorTrust{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, demo, op)
		e.get(ct, key("nats-system", "sys"), sys)
		e.get(ct, key("nats-system", "orders"), orders)
		e.get(ct, demo, trust)
		ready(ct, op.Status.Conditions, op.Generation, authctl.ReasonSigned)
		ready(ct, sys.Status.Conditions, sys.Generation, authctl.ReasonSigned)
		ready(ct, orders.Status.Conditions, orders.Generation, authctl.ReasonSigned)
		ready(ct, trust.Status.Conditions, trust.Generation, authctl.ReasonMirrored)
		if !assert.NotNil(ct, op.Status.SystemAccount) {
			return
		}
		assert.Equal(ct, authctl.JWTHash(op.Status.SystemAccount.JWT), sys.Status.JWTHash)
		assert.Equal(ct, op.Status.JWT, trust.Status.OperatorJWT)
		assert.Equal(ct, op.Status.SystemAccount.JWT, trust.Status.SystemAccountJWT)
	})

	require.EqualValues(t, 1, orders.Generation, "adding the finalizer leaves spec, jwtTTL: 48h among it, as applied")
	require.Equal(t, &authv1beta1.SeedSecrets{Identity: "demo-operator-identity", Signing: []string{"demo-operator-signing-1"}}, op.Status.SeedSecrets)
	for _, name := range []string{"demo-operator-identity", "demo-operator-signing-1", "sys-systemaccount-identity", "orders-account-signing-1"} {
		var sec corev1.Secret
		require.NoError(t, e.c.Get(t.Context(), key("nats-system", name), &sec))
		require.Contains(t, sec.Data, authctl.SeedKey)
		require.Empty(t, sec.OwnerReferences, "%s outlives its object", name)
		require.Contains(t, sec.Annotations, authctl.GeneratedForAnnotation, name)
	}

	oc, err := jwt.DecodeOperatorClaims(op.Status.JWT)
	require.NoError(t, err)
	require.Equal(t, op.Status.PublicKey, oc.Subject)
	require.Equal(t, oc.Subject, oc.Issuer)
	require.Equal(t, sys.Status.PublicKey, oc.SystemAccount)
	require.Equal(t, op.Status.SigningKeys, []string(oc.SigningKeys))
	require.Len(t, op.Status.SigningKeys, 1)
	require.Equal(t, "sys", op.Status.SystemAccount.Name)
	require.Equal(t, sys.Status.PublicKey, op.Status.SystemAccount.PublicKey)

	sc, err := jwt.DecodeAccountClaims(op.Status.SystemAccount.JWT)
	require.NoError(t, err)
	require.Equal(t, sys.Status.PublicKey, sc.Subject)
	require.Zero(t, sc.Expires, "the system account JWT never expires")
	require.Equal(t, op.Status.SigningKeys[0], sc.Issuer, "signed by the signing key, not the identity")

	ac, err := jwt.DecodeAccountClaims(orders.Status.JWT)
	require.NoError(t, err)
	require.Equal(t, orders.Status.PublicKey, ac.Subject)
	require.Equal(t, op.Status.SigningKeys[0], ac.Issuer)
	require.InDelta(t, (48 * time.Hour).Seconds(), float64(ac.Expires-ac.IssuedAt), 2)
	require.Equal(t, authctl.JWTHash(orders.Status.JWT), orders.Status.JWTHash)
	lim := ac.Limits
	require.Equal(t, []int64{500, 10000, 1 << 20, 1 << 30, 50 << 30, 20, 200},
		[]int64{lim.Conn, lim.Subs, lim.Payload, lim.MemoryStorage, lim.DiskStorage, lim.Streams, lim.Consumer})

	require.True(t, e.d.pushed(demo, orders.Status.JWT), "account JWT handed to the distributor")
	require.True(t, e.d.pushed(demo, op.Status.SystemAccount.JWT), "system account JWT handed to the distributor")
	require.True(t, e.log.has("Normal JWTPushed account JWT of "+orders.Status.PublicKey+" pushed"), "no JWTPushed for the account")
	require.True(t, e.log.has("Normal JWTPushed system account JWT of "+sys.Status.PublicKey+" pushed"), "no JWTPushed for the system account")

	signed := orders.Status.JWT
	require.Never(t, func() bool {
		var now authv1beta1.NatsAccount
		return e.c.Get(t.Context(), key("nats-system", "orders"), &now) != nil || now.Status.JWT != signed
	}, 2*time.Second, 100*time.Millisecond, "an unchanged account is not re-signed")
}

func (e *env) testExportsImported(t *testing.T) {
	monitoring := &authv1beta1.NatsAccount{}
	core := &authv1beta1.NatsAccount{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("nats-system", "monitoring"), monitoring)
		e.get(ct, key("nats-system", "core"), core)
		ready(ct, monitoring.Status.Conditions, monitoring.Generation, authctl.ReasonSigned)
		ready(ct, core.Status.Conditions, core.Generation, authctl.ReasonSigned)
		cond := meta.FindStatusCondition(core.Status.Conditions, grant.ConditionReferencesResolved)
		if assert.NotNil(ct, cond) {
			assert.Equal(ct, metav1.ConditionTrue, cond.Status)
			assert.Equal(ct, authctl.ReasonAllImportsResolved, cond.Reason)
		}
	})
	require.Equal(t, []authv1beta1.ImportStatus{
		{Export: "monitoring/check-results", Subject: "monitoring.results.>", LocalSubject: "upstream.results.>", Type: authv1beta1.ExportTypeStream},
		{Export: "monitoring/execute", Subject: "monitoring.execute", Type: authv1beta1.ExportTypeService, Activation: authv1beta1.ActivationSigned},
	}, core.Status.Imports)

	mc, err := jwt.DecodeAccountClaims(monitoring.Status.JWT)
	require.NoError(t, err)
	require.Len(t, mc.Exports, 2)
	exports := map[string]*jwt.Export{}
	for _, x := range mc.Exports {
		exports[x.Name] = x
	}
	require.Equal(t, jwt.Stream, exports["check-results"].Type)
	require.False(t, exports["check-results"].TokenReq)
	require.Equal(t, jwt.Service, exports["execute"].Type)
	require.True(t, exports["execute"].TokenReq)
	require.Equal(t, jwt.ResponseType(jwt.ResponseTypeSingleton), exports["execute"].ResponseType)

	cc, err := jwt.DecodeAccountClaims(core.Status.JWT)
	require.NoError(t, err)
	require.Len(t, cc.Imports, 2)
	var execute *jwt.Import
	for _, i := range cc.Imports {
		require.Equal(t, monitoring.Status.PublicKey, i.Account)
		if i.Name == "execute" {
			execute = i
		}
	}
	require.NotNil(t, execute)
	act, err := jwt.DecodeActivationClaims(execute.Token)
	require.NoError(t, err)
	require.Equal(t, core.Status.PublicKey, act.Subject)
	require.Equal(t, monitoring.Status.PublicKey, act.IssuerAccount)
	require.Contains(t, mc.SigningKeys.Keys(), act.Issuer, "minted with the exporter's signing key")
}

// testServed boots a nats-server from the trust object's JWTs and the
// accounts' JWTs, and connects users signed with the generated account
// signing keys: story 5's stream and private service imports carry traffic.
func (e *env) testServed(t *testing.T) {
	var trust natsv1beta1.NatsOperatorTrust
	require.NoError(t, e.c.Get(t.Context(), demo, &trust))
	oc, err := jwt.DecodeOperatorClaims(trust.Status.OperatorJWT)
	require.NoError(t, err)
	res := &server.MemAccResolver{}
	require.NoError(t, res.Store(oc.SystemAccount, trust.Status.SystemAccountJWT))
	accounts := map[string]*authv1beta1.NatsAccount{}
	for _, name := range []string{"orders", "monitoring", "core"} {
		var acc authv1beta1.NatsAccount
		require.NoError(t, e.c.Get(t.Context(), key("nats-system", name), &acc))
		require.NoError(t, res.Store(acc.Status.PublicKey, acc.Status.JWT))
		accounts[name] = &acc
	}
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

	connect := func(account string) *nats.Conn {
		acc := accounts[account]
		var sec corev1.Secret
		require.NoError(t, e.c.Get(t.Context(), key("nats-system", account+"-account-signing-1"), &sec))
		sk, err := nkeys.FromSeed(sec.Data[authctl.SeedKey])
		require.NoError(t, err)
		u, err := nkeys.CreateUser()
		require.NoError(t, err)
		upub, err := u.PublicKey()
		require.NoError(t, err)
		useed, err := u.Seed()
		require.NoError(t, err)
		token, err := jwtplane.SignUser(jwtplane.User{Name: account, PublicKey: upub},
			jwtplane.Keys{PublicKey: acc.Status.PublicKey, Signing: []jwtplane.SigningKey{{Name: "s", Pair: sk}}})
		require.NoError(t, err)
		nc, err := nats.Connect(srv.ClientURL(), nats.UserJWTAndSeed(token, string(useed)), nats.NoReconnect())
		require.NoError(t, err)
		t.Cleanup(nc.Close)
		return nc
	}
	mon, core := connect("monitoring"), connect("core")
	_ = connect("orders")

	_, err = mon.Subscribe("monitoring.execute", func(m *nats.Msg) { _ = m.Respond([]byte("done")) })
	require.NoError(t, err)
	results, err := core.SubscribeSync("upstream.results.>")
	require.NoError(t, err)
	require.NoError(t, mon.Flush())
	require.NoError(t, core.Flush())

	reply, err := core.Request("monitoring.execute", nil, 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, "done", string(reply.Data))
	require.NoError(t, mon.Publish("monitoring.results.http", []byte("up")))
	msg, err := results.NextMsg(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, "upstream.results.http", msg.Subject)
}

// testCrossNamespaceImport pins the two consents of an import: a grant in the
// exporter's namespace for any import, and the importer listed for a
// private export; dropping the grant drops the import from the JWT.
func (e *env) testCrossNamespaceImport(t *testing.T) {
	e.apply(t, `
apiVersion: nats.mikluko.io/v1beta1
kind: NatsReferenceGrant
metadata: {name: team-a, namespace: nats-system}
spec:
  from: [{group: auth.nats.mikluko.io, kind: NatsAccount, namespace: team-a}]
  to: [{group: auth.nats.mikluko.io, kind: NatsOperator, name: demo}]
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: billing, namespace: team-a}
spec:
  operatorRef: {name: demo, namespace: nats-system}
  imports:
    - accountRef: {kind: NatsAccount, name: monitoring, namespace: nats-system}
      export: check-results
    - accountRef: {kind: NatsAccount, name: monitoring, namespace: nats-system}
      export: execute
`)
	billing := key("team-a", "billing")
	imports := func(ct *assert.CollectT, acc *authv1beta1.NatsAccount) []string {
		c, err := jwt.DecodeAccountClaims(acc.Status.JWT)
		if !assert.NoError(ct, err) {
			return nil
		}
		var out []string
		for _, i := range c.Imports {
			out = append(out, i.Name)
		}
		return out
	}
	state := func(wantReady bool, wantReason, wantResolvedReason string, wantImports ...string) func(*assert.CollectT) {
		return func(ct *assert.CollectT) {
			var acc authv1beta1.NatsAccount
			e.get(ct, billing, &acc)
			cond := meta.FindStatusCondition(acc.Status.Conditions, authctl.ConditionReady)
			if !assert.NotNil(ct, cond) {
				return
			}
			assert.Equal(ct, wantReady, cond.Status == metav1.ConditionTrue)
			assert.Equal(ct, wantReason, cond.Reason)
			rr := meta.FindStatusCondition(acc.Status.Conditions, grant.ConditionReferencesResolved)
			if assert.NotNil(ct, rr) {
				assert.Equal(ct, wantResolvedReason, rr.Reason)
			}
			assert.ElementsMatch(ct, wantImports, imports(ct, &acc))
		}
	}

	e.eventually(t, state(false, grant.ReasonReferenceNotPermitted, grant.ReasonNoGrant))

	e.update(t, key("nats-system", "team-a"), &natsv1beta1.NatsReferenceGrant{}, func(o client.Object) {
		g := o.(*natsv1beta1.NatsReferenceGrant)
		g.Spec.To = append(g.Spec.To, natsv1beta1.ReferenceGrantTo{Group: "auth.nats.mikluko.io", Kind: "NatsAccount", Name: "monitoring"})
	})
	e.eventually(t, state(false, authctl.ReasonImportsUnresolved, authctl.ReasonImportsUnresolved, "check-results"))

	e.update(t, key("nats-system", "monitoring"), &authv1beta1.NatsAccount{}, func(o client.Object) {
		acc := o.(*authv1beta1.NatsAccount)
		acc.Spec.Exports[1].Importers = append(acc.Spec.Exports[1].Importers,
			authv1beta1.AccountReference{Kind: authv1beta1.AccountKindAccount, ObjectReference: natsv1beta1.ObjectReference{Name: "billing", Namespace: "team-a"}})
	})
	e.eventually(t, state(true, authctl.ReasonSigned, authctl.ReasonAllImportsResolved, "check-results", "execute"))

	e.update(t, key("nats-system", "team-a"), &natsv1beta1.NatsReferenceGrant{}, func(o client.Object) {
		g := o.(*natsv1beta1.NatsReferenceGrant)
		g.Spec.To = g.Spec.To[:1]
	})
	e.eventually(t, state(false, grant.ReasonReferenceNotPermitted, grant.ReasonNoGrant))
}

// testImportFromOtherOperator pins that an import from a NatsAccount
// signed by another NatsOperator is left out of the JWT and keeps the
// importer from Ready.
func (e *env) testImportFromOtherOperator(t *testing.T) {
	e.apply(t, `
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata: {name: foreign, namespace: foreign}
spec:
  systemAccountRef: {name: sys}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata: {name: sys, namespace: foreign}
spec:
  operatorRef: {name: foreign}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: exporter, namespace: foreign}
spec:
  operatorRef: {name: foreign}
  exports:
    - {name: events, type: Stream, subject: "foreign.events.>"}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: importer, namespace: foreign}
spec:
  operatorRef: {name: demo, namespace: nats-system}
  imports:
    - accountRef: {kind: NatsAccount, name: exporter}
      export: events
---
apiVersion: nats.mikluko.io/v1beta1
kind: NatsReferenceGrant
metadata: {name: foreign, namespace: nats-system}
spec:
  from: [{group: auth.nats.mikluko.io, kind: NatsAccount, namespace: foreign}]
  to: [{group: auth.nats.mikluko.io, kind: NatsOperator, name: demo}]
`)
	var exporter, importer authv1beta1.NatsAccount
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("foreign", "exporter"), &exporter)
		e.get(ct, key("foreign", "importer"), &importer)
		ready(ct, exporter.Status.Conditions, exporter.Generation, authctl.ReasonSigned)
		cond := meta.FindStatusCondition(importer.Status.Conditions, authctl.ConditionReady)
		if assert.NotNil(ct, cond) {
			assert.Equal(ct, metav1.ConditionFalse, cond.Status)
			assert.Equal(ct, authctl.ReasonImportsUnresolved, cond.Reason)
		}
		rr := meta.FindStatusCondition(importer.Status.Conditions, grant.ConditionReferencesResolved)
		if assert.NotNil(ct, rr) {
			assert.Equal(ct, authctl.ReasonImportsUnresolved, rr.Reason)
			assert.Equal(ct, "exporter/events: NatsAccount foreign/exporter is signed by NatsOperator foreign/foreign, not nats-system/demo", rr.Message)
		}
		assert.NotEmpty(ct, importer.Status.JWT)
	})
	require.Empty(t, importer.Status.Imports)
	c, err := jwt.DecodeAccountClaims(importer.Status.JWT)
	require.NoError(t, err)
	require.Empty(t, c.Imports)
}

// testNoExpiryAndAccountTrust pins jwtTTL: 0 and the reference form
// of NatsAccountTrust, within its namespace and, without a grant, across.
func (e *env) testNoExpiryAndAccountTrust(t *testing.T) {
	e.apply(t, `
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: telemetry, namespace: nats-system}
spec:
  operatorRef: {name: demo}
  jwtTTL: 0s
---
apiVersion: nats.mikluko.io/v1beta1
kind: NatsAccountTrust
metadata: {name: telemetry, namespace: nats-system}
spec:
  accountRef: {name: telemetry}
---
apiVersion: nats.mikluko.io/v1beta1
kind: NatsAccountTrust
metadata: {name: telemetry, namespace: team-a}
spec:
  accountRef: {name: telemetry, namespace: nats-system}
`)
	acc := &authv1beta1.NatsAccount{}
	trust := &natsv1beta1.NatsAccountTrust{}
	foreign := &natsv1beta1.NatsAccountTrust{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("nats-system", "telemetry"), acc)
		e.get(ct, key("nats-system", "telemetry"), trust)
		e.get(ct, key("team-a", "telemetry"), foreign)
		ready(ct, acc.Status.Conditions, acc.Generation, authctl.ReasonSigned)
		ready(ct, trust.Status.Conditions, trust.Generation, authctl.ReasonMirrored)
		assert.Equal(ct, acc.Status.PublicKey, trust.Status.PublicKey)
		assert.Equal(ct, acc.Status.JWT, trust.Status.JWT)
		cond := meta.FindStatusCondition(foreign.Status.Conditions, authctl.ConditionReady)
		if assert.NotNil(ct, cond) {
			assert.Equal(ct, grant.ReasonReferenceNotPermitted, cond.Reason)
		}
	})
	require.Empty(t, foreign.Status.JWT)
	c, err := jwt.DecodeAccountClaims(acc.Status.JWT)
	require.NoError(t, err)
	require.Zero(t, c.Expires)
}

// testRotation adds a signing key to a NatsOperator with adopted keys, marks
// the old one retiring, and removes it: accounts are re-signed with the new
// key before the old one leaves the NATS operator JWT.
func (e *env) testRotation(t *testing.T) {
	seeds := map[string]nkeys.PrefixByte{"rot-id": nkeys.PrefixByteOperator, "rot-k1": nkeys.PrefixByteOperator, "rot-k2": nkeys.PrefixByteOperator}
	pubs := map[string]string{}
	for name, prefix := range seeds {
		pubs[name] = e.seedSecret(t, "rot", name, prefix)
	}
	e.apply(t, `
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata: {name: rot, namespace: rot}
spec:
  keys:
    identity: {secretKeyRef: {name: rot-id, key: seed}}
    signing: [{name: k1, secretKeyRef: {name: rot-k1, key: seed}}]
  systemAccountRef: {name: sys}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata: {name: sys, namespace: rot}
spec:
  operatorRef: {name: rot}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: app, namespace: rot}
spec:
  operatorRef: {name: rot}
`)
	rot := key("rot", "rot")
	signedBy := func(signing []string, want string) func(*assert.CollectT) {
		return func(ct *assert.CollectT) {
			var op authv1beta1.NatsOperator
			var acc authv1beta1.NatsAccount
			e.get(ct, rot, &op)
			e.get(ct, key("rot", "app"), &acc)
			ready(ct, op.Status.Conditions, op.Generation, authctl.ReasonSigned)
			assert.Equal(ct, pubs["rot-id"], op.Status.PublicKey)
			assert.Nil(ct, op.Status.SeedSecrets)
			oc, err := jwt.DecodeOperatorClaims(op.Status.JWT)
			if assert.NoError(ct, err) {
				assert.Equal(ct, signing, []string(oc.SigningKeys))
			}
			if op.Status.SystemAccount != nil {
				assert.Equal(ct, want, issuerOf(ct, op.Status.SystemAccount.JWT))
			}
			assert.Equal(ct, want, issuerOf(ct, acc.Status.JWT))
			assert.True(ct, e.d.pushed(rot, acc.Status.JWT))
			retiring := meta.FindStatusCondition(op.Status.Conditions, authctl.ConditionRetiringKeysInUse)
			if assert.NotNil(ct, retiring) {
				assert.Equal(ct, metav1.ConditionFalse, retiring.Status)
			}
		}
	}
	e.eventually(t, signedBy([]string{pubs["rot-k1"]}, pubs["rot-k1"]))

	e.update(t, rot, &authv1beta1.NatsOperator{}, func(o client.Object) {
		op := o.(*authv1beta1.NatsOperator)
		op.Spec.Keys.Signing = []authv1beta1.SigningKey{
			{Name: "k1", SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: "rot-k1", Key: "seed"}, Retiring: true},
			{Name: "k2", SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: "rot-k2", Key: "seed"}},
		}
	})
	e.eventually(t, signedBy([]string{pubs["rot-k1"], pubs["rot-k2"]}, pubs["rot-k2"]))

	e.update(t, rot, &authv1beta1.NatsOperator{}, func(o client.Object) {
		op := o.(*authv1beta1.NatsOperator)
		op.Spec.Keys.Signing = op.Spec.Keys.Signing[1:]
	})
	e.eventually(t, signedBy([]string{pubs["rot-k2"]}, pubs["rot-k2"]))
}

// testOfflineIdentities pins a NATS operator JWT signed offline, a system
// account and an account whose identities are public keys only, and the
// refusal of an offline JWT that names another system account.
func (e *env) testOfflineIdentities(t *testing.T) {
	opID, err := nkeys.CreateOperator()
	require.NoError(t, err)
	opPub, err := opID.PublicKey()
	require.NoError(t, err)
	opSigning := e.seedSecret(t, "offline", "op-signing", nkeys.PrefixByteOperator)
	sysID, err := nkeys.CreateAccount()
	require.NoError(t, err)
	sysPub, err := sysID.PublicKey()
	require.NoError(t, err)
	e.seedSecret(t, "offline", "sys-signing", nkeys.PrefixByteAccount)
	accID, err := nkeys.CreateAccount()
	require.NoError(t, err)
	accPub, err := accID.PublicKey()
	require.NoError(t, err)
	accSigning := e.seedSecret(t, "offline", "acc-signing", nkeys.PrefixByteAccount)

	var signingSeed corev1.Secret
	require.NoError(t, e.c.Get(t.Context(), key("offline", "op-signing"), &signingSeed))
	sk, err := nkeys.FromSeed(signingSeed.Data[authctl.SeedKey])
	require.NoError(t, err)
	offlineJWT, err := jwtplane.SignOperator(jwtplane.Operator{
		Name:          "offline",
		Keys:          jwtplane.Keys{Identity: opID, Signing: []jwtplane.SigningKey{{Name: "s", Pair: sk}}},
		SystemAccount: sysPub,
	})
	require.NoError(t, err)
	strayJWT, err := jwtplane.SignOperator(jwtplane.Operator{
		Name:          "stray",
		Keys:          jwtplane.Keys{Identity: opID, Signing: []jwtplane.SigningKey{{Name: "s", Pair: sk}}},
		SystemAccount: accPub,
	})
	require.NoError(t, err)

	e.apply(t, fmt.Sprintf(`
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata: {name: offline, namespace: offline}
spec:
  jwt: %[1]s
  keys:
    signing: [{name: s, secretKeyRef: {name: op-signing, key: seed}}]
  systemAccountRef: {name: sys}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata: {name: sys, namespace: offline}
spec:
  operatorRef: {name: offline}
  publicKey: %[2]s
  keys:
    signing: [{name: s, secretKeyRef: {name: sys-signing, key: seed}}]
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: app, namespace: offline}
spec:
  operatorRef: {name: offline}
  publicKey: %[3]s
  keys:
    signing: [{name: s, secretKeyRef: {name: acc-signing, key: seed}}]
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata: {name: stray, namespace: offline}
spec:
  jwt: %[4]s
  keys:
    signing: [{name: s, secretKeyRef: {name: op-signing, key: seed}}]
  systemAccountRef: {name: stray-sys}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata: {name: stray-sys, namespace: offline}
spec:
  operatorRef: {name: stray}
`, offlineJWT, sysPub, accPub, strayJWT))

	e.eventually(t, func(ct *assert.CollectT) {
		var op, stray authv1beta1.NatsOperator
		var acc authv1beta1.NatsAccount
		e.get(ct, key("offline", "offline"), &op)
		e.get(ct, key("offline", "stray"), &stray)
		e.get(ct, key("offline", "app"), &acc)
		ready(ct, op.Status.Conditions, op.Generation, authctl.ReasonSigned)
		assert.Equal(ct, offlineJWT, op.Status.JWT, "the offline JWT is served unchanged")
		assert.Equal(ct, opPub, op.Status.PublicKey)
		assert.Nil(ct, op.Status.SeedSecrets)
		if assert.NotNil(ct, op.Status.SystemAccount) {
			assert.Equal(ct, sysPub, op.Status.SystemAccount.PublicKey)
			assert.Equal(ct, opSigning, issuerOf(ct, op.Status.SystemAccount.JWT))
		}
		ready(ct, acc.Status.Conditions, acc.Generation, authctl.ReasonSigned)
		assert.Equal(ct, accPub, acc.Status.PublicKey)
		if c, err := jwt.DecodeAccountClaims(acc.Status.JWT); assert.NoError(ct, err) {
			assert.Equal(ct, accPub, c.Subject)
			assert.Equal(ct, []string{accSigning}, c.SigningKeys.Keys())
			assert.Equal(ct, opSigning, c.Issuer)
		}
		cond := meta.FindStatusCondition(stray.Status.Conditions, authctl.ConditionReady)
		if assert.NotNil(ct, cond) {
			assert.Equal(ct, authctl.ReasonInvalidJWT, cond.Reason)
		}
	})
	var secrets corev1.SecretList
	require.NoError(t, e.c.List(t.Context(), &secrets, client.InNamespace("offline")))
	var names []string
	for _, s := range secrets.Items {
		if _, generated := s.Annotations[authctl.GeneratedForAnnotation]; generated {
			names = append(names, s.Name)
		}
	}
	require.ElementsMatch(t, []string{"stray-sys-systemaccount-identity", "stray-sys-systemaccount-signing-1"}, names,
		"nothing is generated for identities held offline")
}

// testFlipAndStepdown flips a NatsOperator's systemAccountRef between two
// NatsSystemAccounts, and signs the stepdown imports of an account carrying
// the jetstream-stepdown preset into the system account JWT.
func (e *env) testFlipAndStepdown(t *testing.T) {
	e.apply(t, `
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata: {name: flip, namespace: flip}
spec:
  systemAccountRef: {name: sys-a}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata: {name: sys-a, namespace: flip}
spec:
  operatorRef: {name: flip}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata: {name: sys-b, namespace: flip}
spec:
  operatorRef: {name: flip}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: js, namespace: flip}
spec:
  operatorRef: {name: flip}
  exports: [{preset: jetstream-stepdown}]
---
apiVersion: nats.mikluko.io/v1beta1
kind: NatsOperatorTrust
metadata: {name: flip, namespace: flip}
spec:
  operatorRef: {name: flip}
`)
	live := func(liveName, idleName string) func(*assert.CollectT) {
		return func(ct *assert.CollectT) {
			var op authv1beta1.NatsOperator
			var liveSys, idleSys authv1beta1.NatsSystemAccount
			var acc authv1beta1.NatsAccount
			var trust natsv1beta1.NatsOperatorTrust
			e.get(ct, key("flip", "flip"), &op)
			e.get(ct, key("flip", liveName), &liveSys)
			e.get(ct, key("flip", idleName), &idleSys)
			e.get(ct, key("flip", "js"), &acc)
			e.get(ct, key("flip", "flip"), &trust)
			ready(ct, liveSys.Status.Conditions, liveSys.Generation, authctl.ReasonSigned)
			cond := meta.FindStatusCondition(idleSys.Status.Conditions, authctl.ConditionReady)
			if assert.NotNil(ct, cond) {
				assert.Equal(ct, authctl.ReasonNotReferenced, cond.Reason)
			}
			assert.Empty(ct, idleSys.Status.JWTHash)
			if !assert.NotNil(ct, op.Status.SystemAccount) {
				return
			}
			assert.Equal(ct, liveName, op.Status.SystemAccount.Name)
			assert.Equal(ct, op.Status.SystemAccount.JWT, trust.Status.SystemAccountJWT)
			if oc, err := jwt.DecodeOperatorClaims(op.Status.JWT); assert.NoError(ct, err) {
				assert.Equal(ct, liveSys.Status.PublicKey, oc.SystemAccount)
			}
			sc, err := jwt.DecodeAccountClaims(op.Status.SystemAccount.JWT)
			if !assert.NoError(ct, err) || !assert.NotEmpty(ct, acc.Status.PublicKey) {
				return
			}
			var subjects []string
			for _, i := range sc.Imports {
				assert.Equal(ct, acc.Status.PublicKey, i.Account)
				subjects = append(subjects, string(i.LocalSubject))
			}
			assert.ElementsMatch(ct, []string{
				"acc." + acc.Status.PublicKey + ".$JS.API.STREAM.LEADER.STEPDOWN.*",
				"acc." + acc.Status.PublicKey + ".$JS.API.CONSUMER.LEADER.STEPDOWN.*.*",
			}, subjects)
		}
	}
	e.eventually(t, live("sys-a", "sys-b"))
	e.update(t, key("flip", "flip"), &authv1beta1.NatsOperator{}, func(o client.Object) {
		o.(*authv1beta1.NatsOperator).Spec.SystemAccountRef.Name = "sys-b"
	})
	e.eventually(t, live("sys-b", "sys-a"))
}

func key(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}

func (e *env) eventually(t *testing.T, f func(*assert.CollectT)) {
	t.Helper()
	require.EventuallyWithT(t, f, 30*time.Second, 100*time.Millisecond)
}

func (e *env) get(ct *assert.CollectT, k types.NamespacedName, obj client.Object) {
	assert.NoError(ct, e.c.Get(e.ctx, k, obj))
}

// update applies mutate to the object at k, retrying on conflict.
func (e *env) update(t *testing.T, k types.NamespacedName, obj client.Object, mutate func(client.Object)) {
	t.Helper()
	require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := e.c.Get(t.Context(), k, obj); err != nil {
			return err
		}
		mutate(obj)
		return e.c.Update(t.Context(), obj)
	}))
}

// apply creates every object of a multi-document YAML manifest.
func (e *env) apply(t *testing.T, manifest string) {
	t.Helper()
	r := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewBufferString(manifest)))
	for {
		raw, err := r.Read()
		if errors.Is(err, io.EOF) {
			return
		}
		require.NoError(t, err)
		var m map[string]any
		require.NoError(t, yaml.Unmarshal(raw, &m))
		if len(m) == 0 {
			continue
		}
		require.NoError(t, e.c.Create(t.Context(), &unstructured.Unstructured{Object: m}))
	}
}

// seedSecret creates a Secret holding a new seed under authctl.SeedKey and
// returns its public key.
func (e *env) seedSecret(t *testing.T, namespace, name string, prefix nkeys.PrefixByte) string {
	t.Helper()
	seed, err := jwtplane.GenerateSeed(prefix)
	require.NoError(t, err)
	kp, err := nkeys.FromSeed(seed)
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	require.NoError(t, e.c.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Data:       map[string][]byte{authctl.SeedKey: seed},
	}))
	return pub
}

// ready asserts Ready=True with reason on conds, observed at gen.
func ready(ct *assert.CollectT, conds []metav1.Condition, gen int64, reason string) {
	cond := meta.FindStatusCondition(conds, authctl.ConditionReady)
	if !assert.NotNil(ct, cond) {
		return
	}
	assert.Equal(ct, metav1.ConditionTrue, cond.Status, cond.Message)
	assert.Equal(ct, reason, cond.Reason)
	assert.Equal(ct, gen, cond.ObservedGeneration)
}

func issuerOf(ct *assert.CollectT, accountJWT string) string {
	c, err := jwt.DecodeAccountClaims(accountJWT)
	if !assert.NoError(ct, err) {
		return ""
	}
	return c.Issuer
}
