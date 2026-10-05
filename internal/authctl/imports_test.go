package authctl

import (
	"context"
	"testing"

	"github.com/nats-io/jwt/v2"
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
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// builderIndexer registers field indexes on a fake client under construction.
type builderIndexer struct{ b *fake.ClientBuilder }

func (i builderIndexer) IndexField(_ context.Context, obj client.Object, field string, fn client.IndexerFunc) error {
	i.b.WithIndex(obj, field, fn)
	return nil
}

// pushLog is a Distributor of one server that keeps every JWT pushed.
type pushLog struct{ pushed []string }

func (d *pushLog) Push(_ context.Context, _ types.NamespacedName, token string) error {
	d.pushed = append(d.pushed, token)
	return nil
}

func (*pushLog) Current(context.Context, types.NamespacedName, string) (authv1beta1.Distribution, error) {
	return authv1beta1.Distribution{Servers: 1, Current: 1}, nil
}

func (*pushLog) Lookup(context.Context, types.NamespacedName, string) (string, error) {
	return "", nil
}

func (*pushLog) Delete(context.Context, types.NamespacedName, string) error { return nil }

func (*pushLog) Distrusting(context.Context, types.NamespacedName, string) (int, int, error) {
	return 1, 0, nil
}

// importsPushed returns, per JWT pushed for the account named name, how many
// imports it carries.
func (d *pushLog) importsPushed(t *testing.T, name string) []int {
	t.Helper()
	var out []int
	for _, token := range d.pushed {
		c, err := jwt.DecodeAccountClaims(token)
		require.NoError(t, err)
		if c.Name == name {
			out = append(out, len(c.Imports))
		}
	}
	return out
}

type importsEnv struct {
	c   client.Client
	d   *pushLog
	acc *AccountReconciler
}

// newImportsEnv reconciles a NatsOperator ns/op to signed and leaves accounts
// unreconciled.
func newImportsEnv(t *testing.T, accounts ...*authv1beta1.NatsAccount) *importsEnv {
	t.Helper()
	e := buildImportsEnv(t, accounts...)
	e.reconcileAuthPlane(t)
	return e
}

// buildImportsEnv holds a NatsOperator ns/op, its NatsSystemAccount ns/sys
// and accounts, none reconciled.
func buildImportsEnv(t *testing.T, accounts ...*authv1beta1.NatsAccount) *importsEnv {
	t.Helper()
	s := testScheme(t)
	require.NoError(t, natsv1beta1.AddToScheme(s))
	op := &authv1beta1.NatsOperator{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "op", UID: "op"},
		Spec:       authv1beta1.NatsOperatorSpec{SystemAccountRef: natsv1beta1.ObjectReference{Name: "sys"}},
	}
	sys := &authv1beta1.NatsSystemAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "sys", UID: "sys"},
		Spec:       authv1beta1.NatsSystemAccountSpec{OperatorRef: natsv1beta1.ObjectReference{Name: "op"}},
	}
	spare := &authv1beta1.NatsSystemAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "spare", UID: "spare"},
		Spec:       authv1beta1.NatsSystemAccountSpec{OperatorRef: natsv1beta1.ObjectReference{Name: "op"}},
	}
	objs := []client.Object{op, sys, spare}
	for _, a := range accounts {
		objs = append(objs, a)
	}
	b := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(objs...)
	require.NoError(t, indexes(t.Context(), builderIndexer{b}))
	e := &importsEnv{c: b.Build(), d: &pushLog{}}
	e.acc = &AccountReconciler{Client: e.c, Distributor: e.d}
	return e
}

// reconcileAuthPlane reconciles ns/op and ns/sys to signed.
func (e *importsEnv) reconcileAuthPlane(t *testing.T) {
	t.Helper()
	for range 2 {
		e.reconcileOperator(t)
		e.reconcileSystemAccount(t)
	}
}

func (e *importsEnv) reconcileOperator(t *testing.T) {
	t.Helper()
	or := &OperatorReconciler{Client: e.c, Distributor: e.d}
	_, err := or.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "op"}})
	require.NoError(t, err)
}

func (e *importsEnv) reconcileSystemAccount(t *testing.T) {
	t.Helper()
	sr := &SystemAccountReconciler{Client: e.c, Distributor: e.d}
	_, err := sr.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "sys"}})
	require.NoError(t, err)
}

func (e *importsEnv) reconcile(t *testing.T, name string) authv1beta1.NatsAccountStatus {
	t.Helper()
	k := types.NamespacedName{Namespace: "ns", Name: name}
	_, err := e.acc.Reconcile(t.Context(), reconcile.Request{NamespacedName: k})
	require.NoError(t, err)
	var acc authv1beta1.NatsAccount
	require.NoError(t, e.c.Get(t.Context(), k, &acc))
	return acc.Status
}

func importingAccount(name string, exports []string, importsFrom ...string) *authv1beta1.NatsAccount {
	acc := &authv1beta1.NatsAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID(name)},
		Spec:       authv1beta1.NatsAccountSpec{OperatorRef: natsv1beta1.ObjectReference{Name: "op"}},
	}
	for _, e := range exports {
		acc.Spec.Exports = append(acc.Spec.Exports, authv1beta1.Export{Name: e, Type: authv1beta1.ExportTypeStream, Subject: name + "." + e + ".>"})
	}
	for _, from := range importsFrom {
		acc.Spec.Imports = append(acc.Spec.Imports, authv1beta1.Import{
			AccountRef: &authv1beta1.AccountReference{Kind: authv1beta1.AccountKindAccount, ObjectReference: natsv1beta1.ObjectReference{Name: from}},
			Export:     "events",
		})
	}
	return acc
}

func requireReady(t *testing.T, st authv1beta1.NatsAccountStatus, status metav1.ConditionStatus, reason, resolvedReason string) {
	t.Helper()
	ready := meta.FindStatusCondition(st.Conditions, ConditionReady)
	require.NotNil(t, ready)
	require.Equal(t, status, ready.Status, ready.Message)
	require.Equal(t, reason, ready.Reason, ready.Message)
	resolved := meta.FindStatusCondition(st.Conditions, grant.ConditionReferencesResolved)
	require.NotNil(t, resolved)
	require.Equal(t, resolvedReason, resolved.Reason, resolved.Message)
}

func TestAccountReconciler_ImporterWaitsForExporterKey(t *testing.T) {
	e := newImportsEnv(t,
		importingAccount("importer", nil, "exporter"),
		importingAccount("exporter", []string{"events"}),
	)

	st := e.reconcile(t, "importer")
	require.Empty(t, e.d.importsPushed(t, "importer"))
	require.Empty(t, st.JWT)
	require.NotEmpty(t, st.PublicKey)
	requireReady(t, st, metav1.ConditionFalse, ReasonExporterPending, ReasonExporterPending)
	ready := meta.FindStatusCondition(st.Conditions, ConditionReady)
	require.Equal(t, "exporter/events: NatsAccount ns/exporter has no public key yet", ready.Message)

	e.reconcile(t, "exporter")
	st = e.reconcile(t, "importer")
	requireReady(t, st, metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
	require.Equal(t, []int{1}, e.d.importsPushed(t, "importer"))
}

func TestAccountReconciler_ExporterFirst(t *testing.T) {
	e := newImportsEnv(t,
		importingAccount("importer", nil, "exporter"),
		importingAccount("exporter", []string{"events"}),
	)
	e.reconcile(t, "exporter")
	st := e.reconcile(t, "importer")
	requireReady(t, st, metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
	require.Equal(t, []int{1}, e.d.importsPushed(t, "importer"))
}

func TestAccountReconciler_MutualImportsConverge(t *testing.T) {
	e := newImportsEnv(t,
		importingAccount("a", []string{"events"}, "b"),
		importingAccount("b", []string{"events"}, "a"),
	)
	requireReady(t, e.reconcile(t, "a"), metav1.ConditionFalse, ReasonExporterPending, ReasonExporterPending)
	requireReady(t, e.reconcile(t, "b"), metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
	requireReady(t, e.reconcile(t, "a"), metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
	require.Equal(t, []int{1}, e.d.importsPushed(t, "a"))
	require.Equal(t, []int{1}, e.d.importsPushed(t, "b"))
}

func TestAccountReconciler_SelfImportConverges(t *testing.T) {
	e := newImportsEnv(t, importingAccount("a", []string{"events"}, "a"))
	requireReady(t, e.reconcile(t, "a"), metav1.ConditionFalse, ReasonExporterPending, ReasonExporterPending)
	st := e.reconcile(t, "a")
	require.NotEqual(t, ReasonExporterPending, meta.FindStatusCondition(st.Conditions, grant.ConditionReferencesResolved).Reason)
	require.Equal(t, []int{1}, e.d.importsPushed(t, "a"))
}

func TestAccountReconciler_AbsentExporterIsSignedWithout(t *testing.T) {
	e := newImportsEnv(t, importingAccount("importer", nil, "exporter"))
	st := e.reconcile(t, "importer")
	requireReady(t, st, metav1.ConditionFalse, ReasonImportsUnresolved, ReasonImportsUnresolved)
	require.NotEmpty(t, st.JWT)
	require.Equal(t, []int{0}, e.d.importsPushed(t, "importer"))
}

// lastImports returns the imports of the last JWT pushed for the account
// named name.
func (d *pushLog) lastImports(t *testing.T, name string) jwt.Imports {
	t.Helper()
	var out jwt.Imports
	for _, token := range d.pushed {
		c, err := jwt.DecodeAccountClaims(token)
		require.NoError(t, err)
		if c.Name == name {
			out = c.Imports
		}
	}
	return out
}

func systemImport(name, export string) authv1beta1.Import {
	return authv1beta1.Import{
		AccountRef: &authv1beta1.AccountReference{Kind: authv1beta1.AccountKindSystemAccount, ObjectReference: natsv1beta1.ObjectReference{Name: name}},
		Export:     export,
	}
}

func requireUnresolved(t *testing.T, st authv1beta1.NatsAccountStatus, message string) {
	t.Helper()
	requireReady(t, st, metav1.ConditionFalse, ReasonImportsUnresolved, ReasonImportsUnresolved)
	require.Equal(t, message, meta.FindStatusCondition(st.Conditions, grant.ConditionReferencesResolved).Message)
	require.NotEmpty(t, st.JWT)
	require.Empty(t, st.Imports)
}

func TestAccountReconciler_SystemAccountImport(t *testing.T) {
	acc := importingAccount("monitor", nil)
	acc.Spec.Imports = []authv1beta1.Import{systemImport("sys", "account-monitoring-services"), systemImport("sys", "account-monitoring-streams")}
	e := buildImportsEnv(t, acc)
	e.reconcileOperator(t)

	st := e.reconcile(t, "monitor")
	requireReady(t, st, metav1.ConditionFalse, ReasonExporterPending, ReasonExporterPending)
	require.Equal(t, "sys/account-monitoring-services: NatsSystemAccount ns/sys has no public key yet; sys/account-monitoring-streams: NatsSystemAccount ns/sys has no public key yet",
		meta.FindStatusCondition(st.Conditions, ConditionReady).Message)
	require.Empty(t, st.JWT)

	e.reconcileAuthPlane(t)
	var sys authv1beta1.NatsSystemAccount
	require.NoError(t, e.c.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "sys"}, &sys))
	st = e.reconcile(t, "monitor")
	requireReady(t, st, metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
	require.Equal(t, []authv1beta1.ImportStatus{
		{Export: "sys/account-monitoring-services", Subject: "$SYS.REQ.ACCOUNT." + st.PublicKey + ".*", Type: authv1beta1.ExportTypeService},
		{Export: "sys/account-monitoring-streams", Subject: "$SYS.ACCOUNT." + st.PublicKey + ".>", Type: authv1beta1.ExportTypeStream},
	}, st.Imports)
	imports := e.d.lastImports(t, "monitor")
	require.Len(t, imports, 2)
	for _, i := range imports {
		require.Equal(t, sys.Status.PublicKey, i.Account)
		require.Empty(t, i.Token)
	}
}

func TestAccountReconciler_SystemAccountImportRefused(t *testing.T) {
	tests := []struct {
		name    string
		imp     authv1beta1.Import
		message string
	}{
		{"not the system account", systemImport("spare", "account-monitoring-services"), "spare/account-monitoring-services: NatsSystemAccount ns/spare is not the system account of NatsOperator ns/op"},
		{"absent", systemImport("nope", "account-monitoring-services"), "nope/account-monitoring-services: NatsSystemAccount ns/nope does not exist"},
		{"no such export", systemImport("sys", "events"), "sys/events: NatsSystemAccount ns/sys has no export \"events\""},
		{"allow trace on a service", func() authv1beta1.Import {
			i := systemImport("sys", "account-monitoring-services")
			i.AllowTrace = true
			return i
		}(), "sys/account-monitoring-services: allowTrace is set on a Service import"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := importingAccount("monitor", nil)
			acc.Spec.Imports = []authv1beta1.Import{tt.imp}
			e := newImportsEnv(t, acc)
			requireUnresolved(t, e.reconcile(t, "monitor"), tt.message)
		})
	}
}

func TestAccountReconciler_ShareOnStreamImportRefused(t *testing.T) {
	importer := importingAccount("importer", nil, "exporter")
	importer.Spec.Imports[0].Share = true
	e := newImportsEnv(t, importer, importingAccount("exporter", []string{"events"}))
	e.reconcile(t, "exporter")
	requireUnresolved(t, e.reconcile(t, "importer"), "exporter/events: share is set on a Stream import")
}

// outsideExporter is an account the auth controller does not manage, with a
// private stream export billing.events.>.
type outsideExporter struct {
	keys   jwtplane.Keys
	pub    string
	export jwtplane.Export
}

func newOutsideExporter(t *testing.T) outsideExporter {
	t.Helper()
	identity, pub, _ := seededPair(t, nkeys.PrefixByteAccount)
	signing, _, _ := seededPair(t, nkeys.PrefixByteAccount)
	return outsideExporter{
		keys:   jwtplane.Keys{Identity: identity, Signing: []jwtplane.SigningKey{{Name: "s", Pair: signing}}},
		pub:    pub,
		export: jwtplane.Export{Name: "events", Type: jwt.Stream, Subject: "billing.events.>", Private: true},
	}
}

// activation mints the token admitting importer to the export.
func (x outsideExporter) activation(t *testing.T, importer string) string {
	t.Helper()
	e := x.export
	e.Importers = []string{importer}
	token, err := jwtplane.SignActivation(x.keys, e, importer)
	require.NoError(t, err)
	return token
}

func keyImport(x outsideExporter, activation bool) authv1beta1.Import {
	imp := authv1beta1.Import{PublicKey: x.pub, Export: "events", Subject: "billing.events.>", Type: authv1beta1.ExportTypeStream, LocalSubject: "upstream.events.>"}
	if activation {
		imp.Activation = &authv1beta1.Activation{SecretKeyRef: authv1beta1.ActivationSecretKeySelector{Name: "billing-activation", Key: "token"}}
	}
	return imp
}

func TestAccountReconciler_KeyImport(t *testing.T) {
	x := newOutsideExporter(t)
	acc := importingAccount("orders", nil)
	acc.Spec.Imports = []authv1beta1.Import{keyImport(x, false)}
	acc.Spec.Imports[0].AllowTrace = true
	e := newImportsEnv(t, acc)

	st := e.reconcile(t, "orders")
	requireReady(t, st, metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
	require.Equal(t, []authv1beta1.ImportStatus{{Export: x.pub + "/events", Subject: "billing.events.>", LocalSubject: "upstream.events.>", Type: authv1beta1.ExportTypeStream}}, st.Imports)
	imports := e.d.lastImports(t, "orders")
	require.Len(t, imports, 1)
	require.Equal(t, &jwt.Import{Name: "events", Account: x.pub, Subject: "billing.events.>", LocalSubject: "upstream.events.>", Type: jwt.Stream, AllowTrace: true}, imports[0])
}

func TestAccountReconciler_KeyImportWithActivation(t *testing.T) {
	x := newOutsideExporter(t)
	acc := importingAccount("orders", nil)
	acc.Spec.Imports = []authv1beta1.Import{keyImport(x, true)}
	e := newImportsEnv(t, acc)

	requireUnresolved(t, e.reconcile(t, "orders"), x.pub+"/events: Secret ns/billing-activation does not exist")

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "billing-activation"}, Data: map[string][]byte{"other": []byte("x")}}
	require.NoError(t, e.c.Create(t.Context(), secret))
	requireUnresolved(t, e.reconcile(t, "orders"), x.pub+"/events: Secret ns/billing-activation has no key \"token\"")

	var orders authv1beta1.NatsAccount
	require.NoError(t, e.c.Get(t.Context(), types.NamespacedName{Namespace: "ns", Name: "orders"}, &orders))
	_, otherPub, _ := seededPair(t, nkeys.PrefixByteAccount)
	secret.Data["token"] = []byte(x.activation(t, otherPub))
	require.NoError(t, e.c.Update(t.Context(), secret))
	st := e.reconcile(t, "orders")
	requireReady(t, st, metav1.ConditionFalse, ReasonImportsUnresolved, ReasonImportsUnresolved)
	require.Contains(t, meta.FindStatusCondition(st.Conditions, grant.ConditionReferencesResolved).Message, "activation token doesn't match account it is being included in")

	token := x.activation(t, orders.Status.PublicKey)
	secret.Data["token"] = []byte(token + "\n")
	require.NoError(t, e.c.Update(t.Context(), secret))
	st = e.reconcile(t, "orders")
	requireReady(t, st, metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
	require.Equal(t, []authv1beta1.ImportStatus{{Export: x.pub + "/events", Subject: "billing.events.>", LocalSubject: "upstream.events.>", Type: authv1beta1.ExportTypeStream, Activation: authv1beta1.ActivationSupplied}}, st.Imports)
	imports := e.d.lastImports(t, "orders")
	require.Len(t, imports, 1)
	require.Equal(t, token, imports[0].Token)
}

func TestIndexes_ActivationSecret(t *testing.T) {
	x := newOutsideExporter(t)
	acc := importingAccount("orders", nil)
	acc.Spec.Imports = []authv1beta1.Import{keyImport(x, true)}
	e := newImportsEnv(t, acc)
	var list authv1beta1.NatsAccountList
	require.NoError(t, e.c.List(t.Context(), &list, client.MatchingFields{seedSecretField: "ns/billing-activation"}))
	require.Len(t, list.Items, 1)
	require.Equal(t, "orders", list.Items[0].Name)
}
