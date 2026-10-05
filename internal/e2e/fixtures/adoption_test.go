package fixtures

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/conf"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
)

// checkAdoption requires that story 14's existing NATS cluster runs a full
// resolver preloading the system account alone, that its NATS operator,
// system account, account and users are what nsc makes and chain to the
// seeds the story adopts, that the system account and the account each
// revoke one of their users and no other, and that the status patches name the keys of those seeds.
func checkAdoption(t *testing.T, dir string) {
	t.Helper()
	t.Setenv("POD_NAME", "nats-0")
	d := decodeDir(t, dir)
	seed := func(name, key string, kind nkeys.PrefixByte) string {
		return pub(t, d.secrets[name+"-keys"], key, kind)
	}
	operator, operatorSigning := seed("acme-operator", "identity", nkeys.PrefixByteOperator), seed("acme-operator", "signing-1", nkeys.PrefixByteOperator)
	system, systemSigning := seed("sys", "identity", nkeys.PrefixByteAccount), seed("sys", "signing-1", nkeys.PrefixByteAccount)
	orders, ordersSigning := seed("orders", "identity", nkeys.PrefixByteAccount), seed("orders", "signing-1", nkeys.PrefixByteAccount)

	cm := d.configMaps["nats-config"]
	require.NotNil(t, cm)
	cfg, err := conf.Parse(cm.Data["nats.conf"])
	require.NoError(t, err)
	oc, err := jwt.DecodeOperatorClaims(cfg["operator"].(string))
	require.NoError(t, err)
	require.Equal(t, operator, oc.Subject)
	require.Equal(t, operator, oc.Issuer)
	require.Equal(t, jwt.StringList{operatorSigning}, oc.SigningKeys)
	require.Equal(t, system, oc.SystemAccount)
	require.Equal(t, system, cfg["system_account"])
	resolver, ok := cfg["resolver"].(map[string]any)
	require.True(t, ok, "resolver is a map")
	require.Equal(t, "full", resolver["type"])

	preload, ok := cfg["resolver_preload"].(map[string]any)
	require.True(t, ok, "resolver_preload is a map")
	require.Len(t, preload, 1)
	sc, err := jwt.DecodeAccountClaims(preload[system].(string))
	require.NoError(t, err)
	require.Equal(t, system, sc.Subject)
	require.True(t, oc.DidSign(sc), "the system account is signed by the NATS operator")
	require.Equal(t, []string{systemSigning}, sc.SigningKeys.Keys())
	require.Len(t, sc.Exports, 2)
	require.Len(t, sc.Revocations, 1)

	nsc := d.secrets["nsc"]
	require.NotNil(t, nsc)
	ac, err := jwt.DecodeAccountClaims(nsc.StringData["orders.jwt"])
	require.NoError(t, err)
	require.Equal(t, orders, ac.Subject)
	require.True(t, oc.DidSign(ac), "orders is signed by the NATS operator")
	require.Equal(t, []string{ordersSigning}, ac.SigningKeys.Keys())
	require.Zero(t, ac.Expires)
	require.True(t, ac.Limits.IsJSEnabled())
	require.Len(t, ac.Exports, 1)

	requireCreds(t, nsc, "sys.creds", system, systemSigning)
	require.Equal(t, nsc.StringData["sys.creds"], d.secrets["auth-controller-creds"].StringData["user.creds"])
	requireCreds(t, nsc, "sys-old.creds", system, systemSigning)
	sysOld := userClaims(t, nsc, "sys-old.creds")
	require.True(t, sc.IsClaimRevoked(sysOld))
	require.False(t, sc.IsClaimRevoked(userClaims(t, nsc, "sys.creds")))
	requireCreds(t, nsc, "orders-worker.creds", orders, ordersSigning)
	requireCreds(t, nsc, "orders-old.creds", orders, ordersSigning)
	app := userClaims(t, nsc, "orders-app.creds")
	require.Equal(t, orders, app.Issuer)
	require.Empty(t, app.IssuerAccount)

	old := userClaims(t, nsc, "orders-old.creds")
	require.True(t, ac.IsClaimRevoked(old))
	require.False(t, ac.IsClaimRevoked(app))
	require.False(t, ac.IsClaimRevoked(userClaims(t, nsc, "orders-worker.creds")))
	require.Len(t, ac.Revocations, 1)

	var op authv1beta1.NatsOperator
	readStatusPatch(t, dir, "natsoperator-status.json", &op)
	require.Equal(t, operator, op.Status.PublicKey)
	require.Equal(t, []string{operatorSigning}, op.Status.SigningKeys)
	require.Equal(t, system, op.Status.SystemAccount.PublicKey)
	var sys authv1beta1.NatsSystemAccount
	readStatusPatch(t, dir, "natssystemaccount-status.json", &sys)
	require.Equal(t, system, sys.Status.PublicKey)
	require.Len(t, sys.Status.Revocations, 1)
	require.Equal(t, sysOld.Subject, sys.Status.Revocations[0].PublicKey)
	require.Equal(t, sc.Revocations[sysOld.Subject], sys.Status.Revocations[0].At.Unix())
	require.Equal(t, []string{systemSigning}, sys.Status.Revocations[0].Issuers)
	var acc authv1beta1.NatsAccount
	readStatusPatch(t, dir, "natsaccount-status.json", &acc)
	require.Equal(t, orders, acc.Status.PublicKey)
	require.Len(t, acc.Status.Revocations, 1)
	require.Equal(t, old.Subject, acc.Status.Revocations[0].PublicKey)
	require.Equal(t, ac.Revocations[old.Subject], acc.Status.Revocations[0].At.Unix())
	require.True(t, slices.IsSorted(acc.Status.Revocations[0].Issuers))
	require.ElementsMatch(t, []string{orders, ordersSigning}, acc.Status.Revocations[0].Issuers)
}

// userClaims decodes the user JWT of the creds under key in secret.
func userClaims(t *testing.T, secret *corev1.Secret, key string) *jwt.UserClaims {
	t.Helper()
	token, err := jwt.ParseDecoratedJWT([]byte(secret.StringData[key]))
	require.NoError(t, err)
	claims, err := jwt.DecodeUserClaims(token)
	require.NoError(t, err)
	return claims
}

// readStatusPatch strictly decodes dir/name into obj.
func readStatusPatch(t *testing.T, dir, name string, obj any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)
	require.NoError(t, yaml.UnmarshalStrict(raw, obj))
}
