package authctl

import (
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
)

// TestAccountTakeover_HeldThenRefused pins that an account signed while a
// server was silent, and so held, is refused once the servers answer with a
// JWT the signing would drop claims of: the JWT signed while held leaves
// status, and the account is signed and pushed once the loss is accepted.
func TestAccountTakeover_HeldThenRefused(t *testing.T) {
	e := newImportsEnv(t, importingAccount("orders", nil))
	d := &silentServer{}
	e.acc.Distributor = d
	k := types.NamespacedName{Namespace: "ns", Name: "orders"}
	reconciled := func() authv1beta1.NatsAccountStatus {
		t.Helper()
		_, err := e.acc.Reconcile(t.Context(), reconcile.Request{NamespacedName: k})
		require.NoError(t, err)
		var acc authv1beta1.NatsAccount
		require.NoError(t, e.c.Get(t.Context(), k, &acc))
		return acc.Status
	}

	st := reconciled()
	require.NotEmpty(t, st.JWT, "signed while a server is silent")
	require.True(t, unrecovered(st.Conditions))
	require.Empty(t, d.pushed)

	opKey, err := nkeys.CreateOperator()
	require.NoError(t, err)
	made := jwt.NewAccountClaims(st.PublicKey)
	made.Tags.Add("team:orders")
	d.held, err = made.Encode(opKey)
	require.NoError(t, err)
	d.answers = true

	st = reconciled()
	ready := meta.FindStatusCondition(st.Conditions, ConditionReady)
	require.Equal(t, metav1.ConditionFalse, ready.Status)
	require.Equal(t, ReasonTakeoverDropsClaims, ready.Reason)
	require.Contains(t, ready.Message, "tags")
	require.Empty(t, st.JWT, "the JWT signed while held leaves status")
	require.Nil(t, meta.FindStatusCondition(st.Conditions, ConditionRevocationsUnrecovered))
	require.Nil(t, meta.FindStatusCondition(st.Conditions, ConditionDistributed))
	require.Empty(t, d.pushed)

	var acc authv1beta1.NatsAccount
	require.NoError(t, e.c.Get(t.Context(), k, &acc))
	acc.Spec.Takeover = &authv1beta1.Takeover{DroppedClaims: authv1beta1.TakeoverAcceptDroppedClaims}
	require.NoError(t, e.c.Update(t.Context(), &acc))
	st = reconciled()
	requireReady(t, st, metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
	require.NotEmpty(t, st.JWT)
	require.Len(t, d.pushed, 1)
	require.NotNil(t, st.Distribution.LastPushTime)
}
