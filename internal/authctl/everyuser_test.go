package authctl

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// heldLog is a pushLog whose server holds held for every account.
type heldLog struct {
	*pushLog
	held string
}

func (d heldLog) Lookup(context.Context, types.NamespacedName, string) (string, error) {
	return d.held, nil
}

func TestAccountReconciler_TakesOverRevocationOfEveryUser(t *testing.T) {
	id, err := nkeys.CreateAccount()
	require.NoError(t, err)
	seed, err := id.Seed()
	require.NoError(t, err)
	pub := testPub(t, id)
	t0 := time.Unix(1_800_000_000, 0)
	revoked := userKey(t)

	c := jwt.NewAccountClaims(pub)
	c.RevokeAt(jwt.All, t0)
	c.RevokeAt(revoked, t0.Add(time.Hour))
	held, err := c.Encode(testKeys(t, nkeys.PrefixByteOperator).Identity)
	require.NoError(t, err)

	e := newImportsEnv(t, &authv1beta1.NatsAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "a", UID: "a"},
		Spec: authv1beta1.NatsAccountSpec{
			OperatorRef: natsv1beta1.ObjectReference{Name: "op"},
			Keys:        &authv1beta1.Keys{Identity: &authv1beta1.IdentityKey{SecretKeyRef: authv1beta1.SeedSecretKeySelector{Name: "a-id", Key: "seed"}}},
		},
	})
	require.NoError(t, e.c.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "a-id"},
		Data:       map[string][]byte{"seed": seed},
	}))
	e.acc.Distributor = heldLog{pushLog: e.d, held: held}

	st := e.reconcile(t, "a")
	requireReady(t, st, metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
	require.Equal(t, pub, st.PublicKey)

	var every *authv1beta1.Revocation
	for i := range st.Revocations {
		if st.Revocations[i].PublicKey == jwt.All {
			every = &st.Revocations[i]
		}
	}
	require.NotNil(t, every, "status.revocations: %v", st.Revocations)
	require.Equal(t, t0.Unix(), every.At.Unix())
	require.Equal(t, []string{pub}, every.Issuers)

	signed, err := jwt.DecodeAccountClaims(st.JWT)
	require.NoError(t, err)
	require.Equal(t, jwt.RevocationList{jwt.All: t0.Unix(), revoked: t0.Add(time.Hour).Unix()}, signed.Revocations)
	require.Equal(t, st.JWT, e.d.pushed[len(e.d.pushed)-1])

	st = e.reconcile(t, "a")
	requireReady(t, st, metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
	signed, err = jwt.DecodeAccountClaims(st.JWT)
	require.NoError(t, err)
	require.Contains(t, signed.Revocations, jwt.All)
}
