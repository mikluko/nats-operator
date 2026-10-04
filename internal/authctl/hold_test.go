package authctl

import (
	"context"
	"fmt"
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

// silentServer is a pushLog whose lookup wraps ErrUnreachable until answers
// is set, and returns held from then on.
type silentServer struct {
	pushLog
	answers bool
	held    string
}

func (d *silentServer) Lookup(context.Context, types.NamespacedName, string) (string, error) {
	if !d.answers {
		return "", fmt.Errorf("%w: 2 of 3 servers answered", ErrUnreachable)
	}
	return d.held, nil
}

// revocationsPushed returns, per JWT pushed, the keys it revokes.
func (d *silentServer) revocationsPushed(t *testing.T) [][]string {
	t.Helper()
	var out [][]string
	for _, token := range d.pushed {
		c, err := jwt.DecodeAccountClaims(token)
		require.NoError(t, err)
		keys := []string{}
		for k := range c.Revocations {
			keys = append(keys, k)
		}
		out = append(out, keys)
	}
	return out
}

func TestAccountReconciler_NotPushedUntilEveryServerAnswers(t *testing.T) {
	e := newImportsEnv(t, importingAccount("orders", nil))
	d := &silentServer{}
	e.acc.Distributor = d
	k := types.NamespacedName{Namespace: "ns", Name: "orders"}
	reconciled := func() (reconcile.Result, authv1beta1.NatsAccountStatus) {
		t.Helper()
		res, err := e.acc.Reconcile(t.Context(), reconcile.Request{NamespacedName: k})
		require.NoError(t, err)
		var acc authv1beta1.NatsAccount
		require.NoError(t, e.c.Get(t.Context(), k, &acc))
		return res, acc.Status
	}

	res, st := reconciled()
	require.Empty(t, d.pushed, "a server that does not answer may hold a JWT whose revocations a push would drop")
	require.NotEmpty(t, st.JWT)
	require.True(t, meta.IsStatusConditionTrue(st.Conditions, ConditionRevocationsUnrecovered))
	ready := meta.FindStatusCondition(st.Conditions, ConditionReady)
	require.NotNil(t, ready)
	require.Equal(t, metav1.ConditionTrue, ready.Status, ready.Message)
	require.Equal(t, ReasonSigned, ready.Reason)
	dist := meta.FindStatusCondition(st.Conditions, ConditionDistributed)
	require.NotNil(t, dist)
	require.Equal(t, metav1.ConditionFalse, dist.Status)
	require.Equal(t, ReasonUnreachable, dist.Reason)
	require.Nil(t, st.Distribution.LastPushTime)
	require.Equal(t, distributionRecheck, res.RequeueAfter)

	_, st = reconciled()
	require.Empty(t, d.pushed, "still silent")
	require.True(t, meta.IsStatusConditionTrue(st.Conditions, ConditionRevocationsUnrecovered))

	opKey, err := nkeys.CreateOperator()
	require.NoError(t, err)
	revoked := userKey(t)
	made := jwt.NewAccountClaims(st.PublicKey)
	made.Revoke(revoked)
	d.held, err = made.Encode(opKey)
	require.NoError(t, err)
	d.answers = true

	_, st = reconciled()
	require.Equal(t, [][]string{{revoked}}, d.revocationsPushed(t), "the one JWT pushed carries what the servers held")
	require.Nil(t, meta.FindStatusCondition(st.Conditions, ConditionRevocationsUnrecovered))
	require.NotNil(t, st.Distribution.LastPushTime)
	requireReady(t, st, metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
}
