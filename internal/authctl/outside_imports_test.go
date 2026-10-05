package authctl

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// userOf returns the creds of a new user of the account pub, signed by its
// signing key signer.
func userOf(t *testing.T, pub string, signer nkeys.KeyPair) nscSystemUser {
	t.Helper()
	_, userPub, seed := seededPair(t, nkeys.PrefixByteUser)
	uc := jwt.NewUserClaims(userPub)
	uc.IssuerAccount = pub
	token, err := uc.Encode(signer)
	require.NoError(t, err)
	return nscSystemUser{pub: userPub, jwt: token, seed: seed}
}

// TestAdoption_OutsideImports pins, on an in-process server, that a
// NatsAccount's import from an account the auth controller does not manage,
// by public key with the activation token the exporter issued, and its
// import of a monitoring service of the system account both carry
// messages, the latter for the account's own key only.
func TestAdoption_OutsideImports(t *testing.T) {
	x := newOutsideExporter(t)
	billingSK := x.keys.Signing[0].Pair
	bc := jwt.NewAccountClaims(x.pub)
	bc.Name = "billing"
	bc.SigningKeys.Add(pubOf(t, billingSK))
	bc.Exports.Add(&jwt.Export{Name: "events", Subject: "billing.events.>", Type: jwt.Stream, TokenReq: true})

	_, ordersPub, ordersSeed := seededPair(t, nkeys.PrefixByteAccount)
	ordersSK, _, ordersSKSeed := seededPair(t, nkeys.PrefixByteAccount)
	ref := func(key string) authv1beta1.SeedSecretKeySelector {
		return authv1beta1.SeedSecretKeySelector{Name: "orders-keys", Key: key}
	}
	orders := &authv1beta1.NatsAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "orders", UID: "orders"},
		Spec: authv1beta1.NatsAccountSpec{
			OperatorRef: natsv1beta1.ObjectReference{Name: adoptionOperator.Name},
			Keys: &authv1beta1.Keys{
				Identity: &authv1beta1.IdentityKey{SecretKeyRef: ref("identity")},
				Signing:  []authv1beta1.SigningKey{{Name: "signing-1", SecretKeyRef: ref("signing")}},
			},
			Imports: []authv1beta1.Import{keyImport(x, true), systemImport(adoptionSystem.Name, "account-monitoring-services")},
		},
	}
	n := newNscSystem(t, orders)
	billingJWT, err := bc.Encode(n.opSigning)
	require.NoError(t, err)
	url := n.serve(t, billingJWT)
	for _, s := range []*corev1.Secret{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "orders-keys"}, Data: map[string][]byte{"identity": ordersSeed, "signing": ordersSKSeed}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "billing-activation"}, Data: map[string][]byte{"token": []byte(x.activation(t, ordersPub))}},
	} {
		require.NoError(t, n.c.Create(t.Context(), s))
	}

	nc, err := n.sys.connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	d := &Resolvers{Conn: func(context.Context, types.NamespacedName) (*nats.Conn, error) { return nc, nil }, Wait: 500 * time.Millisecond}
	n.reconcileOperator(t, d)
	n.reconcileSystemAccount(t, d)
	ar := &AccountReconciler{Client: n.c, Distributor: d}
	for range 2 {
		_, err := ar.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(orders)})
		require.NoError(t, err)
	}
	require.NoError(t, n.c.Get(t.Context(), client.ObjectKeyFromObject(orders), orders))
	requireReady(t, orders.Status, metav1.ConditionTrue, ReasonDistributed, ReasonAllImportsResolved)
	held, err := d.Lookup(t.Context(), adoptionOperator, ordersPub)
	require.NoError(t, err)
	require.Equal(t, orders.Status.JWT, held)

	ordersConn, err := userOf(t, ordersPub, ordersSK).connect(url)
	require.NoError(t, err)
	t.Cleanup(ordersConn.Close)
	billingConn, err := userOf(t, x.pub, billingSK).connect(url)
	require.NoError(t, err)
	t.Cleanup(billingConn.Close)

	sub, err := ordersConn.SubscribeSync("upstream.events.>")
	require.NoError(t, err)
	require.NoError(t, ordersConn.Flush())
	require.NoError(t, billingConn.Publish("billing.events.created", []byte("1")))
	msg, err := sub.NextMsg(5 * time.Second)
	require.NoError(t, err, "the private export crosses the import")
	require.Equal(t, "upstream.events.created", msg.Subject)
	require.Equal(t, "1", string(msg.Data))

	reply, err := ordersConn.Request("$SYS.REQ.ACCOUNT."+ordersPub+".SUBSZ", nil, 5*time.Second)
	require.NoError(t, err, "the system account answers for the importer's own key")
	require.Contains(t, string(reply.Data), `"num_subscriptions"`)
	_, err = ordersConn.Request("$SYS.REQ.ACCOUNT."+x.pub+".SUBSZ", nil, 5*time.Second)
	require.ErrorIs(t, err, nats.ErrNoResponders, "and for no other key")
}

func pubOf(t *testing.T, kp nkeys.KeyPair) string {
	t.Helper()
	p, err := kp.PublicKey()
	require.NoError(t, err)
	return p
}
