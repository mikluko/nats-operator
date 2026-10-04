package authctl

import (
	"context"
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
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
	objs := []client.Object{op, sys}
	for _, a := range accounts {
		objs = append(objs, a)
	}
	b := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(objs...)
	require.NoError(t, indexes(t.Context(), builderIndexer{b}))
	e := &importsEnv{c: b.Build(), d: &pushLog{}}
	e.acc = &AccountReconciler{Client: e.c, Distributor: e.d}
	or := &OperatorReconciler{Client: e.c, Distributor: e.d}
	sr := &SystemAccountReconciler{Client: e.c, Distributor: e.d}
	for range 2 {
		_, err := or.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(op)})
		require.NoError(t, err)
		_, err = sr.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(sys)})
		require.NoError(t, err)
	}
	return e
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
			AccountRef: authv1beta1.AccountReference{Kind: authv1beta1.AccountKindAccount, ObjectReference: natsv1beta1.ObjectReference{Name: from}},
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
