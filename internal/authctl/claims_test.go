package authctl

import (
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

func testKeys(t *testing.T, prefix nkeys.PrefixByte, signing ...bool) jwtplane.Keys {
	t.Helper()
	id, err := nkeys.CreatePair(prefix)
	require.NoError(t, err)
	k := jwtplane.Keys{Identity: id}
	for _, retiring := range signing {
		kp, err := nkeys.CreatePair(prefix)
		require.NoError(t, err)
		k.Signing = append(k.Signing, jwtplane.SigningKey{Name: "s", Pair: kp, Retiring: retiring})
	}
	return k
}

func testPub(t *testing.T, kp nkeys.KeyPair) string {
	t.Helper()
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	return pub
}

func TestSameAccountClaims(t *testing.T) {
	op := testKeys(t, nkeys.PrefixByteOperator, false)
	rotated := op
	rotated.Signing = append([]jwtplane.SigningKey{{Name: "old", Pair: op.Signing[0].Pair, Retiring: true}}, testKeys(t, nkeys.PrefixByteOperator, false).Signing...)
	exporter := testKeys(t, nkeys.PrefixByteAccount, false)
	importer := testKeys(t, nkeys.PrefixByteAccount, false)
	importerPub := testPub(t, importer.Identity)
	execute := jwtplane.Export{Name: "execute", Type: jwt.Service, Subject: "x.execute", Private: true, Importers: []string{importerPub}}
	account := func(t *testing.T, mutate func(*jwtplane.Account)) jwtplane.Account {
		t.Helper()
		token, err := jwtplane.SignActivation(exporter, execute, importerPub)
		require.NoError(t, err)
		a := jwtplane.Account{
			Name:    "core",
			Keys:    importer,
			Limits:  jwtplane.Limits{Connections: 10},
			Imports: []jwtplane.Import{{Account: testPub(t, exporter.Identity), Export: execute, Token: token}},
		}
		if mutate != nil {
			mutate(&a)
		}
		return a
	}
	t0 := time.Unix(1_800_000_000, 0)
	base, err := jwtplane.SignAccount(account(t, nil), op, t0)
	require.NoError(t, err)

	tests := []struct {
		name   string
		a      jwtplane.Account
		op     jwtplane.Keys
		at     time.Time
		wantEq bool
	}{
		{name: "signed later, activation re-minted", a: account(t, nil), op: op, at: t0.Add(time.Hour), wantEq: true},
		{name: "limit changed", a: account(t, func(a *jwtplane.Account) { a.Limits.Connections = 11 }), op: op, at: t0},
		{name: "lifetime changed, lifetimeDiffers decides", a: account(t, func(a *jwtplane.Account) { a.TTL = time.Hour }), op: op, at: t0, wantEq: true},
		{name: "no expiry", a: account(t, func(a *jwtplane.Account) { a.NoExpiry = true }), op: op, at: t0},
		{name: "operator signing key rotated", a: account(t, nil), op: rotated, at: t0},
		{name: "import dropped", a: account(t, func(a *jwtplane.Account) { a.Imports = nil }), op: op, at: t0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			other, err := jwtplane.SignAccount(tt.a, tt.op, tt.at)
			require.NoError(t, err)
			require.Equal(t, tt.wantEq, sameAccountClaims(base, other))
		})
	}
	t.Run("empty or undecodable is never the same", func(t *testing.T) {
		require.False(t, sameAccountClaims("", base))
		require.False(t, sameAccountClaims(base, ""))
		require.False(t, sameAccountClaims("garbage", "garbage"))
	})
}

func TestLifetimeDiffers(t *testing.T) {
	op := testKeys(t, nkeys.PrefixByteOperator, false)
	acc := testKeys(t, nkeys.PrefixByteAccount)
	sign := func(a jwtplane.Account) string {
		a.Keys = acc
		tok, err := jwtplane.SignAccount(a, op, time.Now())
		require.NoError(t, err)
		return tok
	}
	tests := []struct {
		name  string
		token string
		ttl   time.Duration
		want  bool
	}{
		{name: "default lifetime", token: sign(jwtplane.Account{}), ttl: 48 * time.Hour},
		{name: "shortened", token: sign(jwtplane.Account{}), ttl: time.Hour, want: true},
		{name: "never expires", token: sign(jwtplane.Account{NoExpiry: true}), ttl: time.Hour, want: true},
		{name: "undecodable", token: "garbage", ttl: time.Hour, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, lifetimeDiffers(tt.token, tt.ttl))
		})
	}
}

func TestSameOperatorClaims(t *testing.T) {
	op := testKeys(t, nkeys.PrefixByteOperator, false)
	sys := testPub(t, testKeys(t, nkeys.PrefixByteAccount).Identity)
	a, err := jwtplane.SignOperator(jwtplane.Operator{Name: "demo", Keys: op, SystemAccount: sys})
	require.NoError(t, err)
	b, err := jwtplane.SignOperator(jwtplane.Operator{Name: "demo", Keys: op, SystemAccount: sys})
	require.NoError(t, err)
	require.True(t, sameOperatorClaims(a, b))

	added := op
	added.Signing = append(added.Signing, testKeys(t, nkeys.PrefixByteOperator, false).Signing...)
	c, err := jwtplane.SignOperator(jwtplane.Operator{Name: "demo", Keys: added, SystemAccount: sys})
	require.NoError(t, err)
	require.False(t, sameOperatorClaims(a, c))
}

func TestAccountExports(t *testing.T) {
	acc := &authv1beta1.NatsAccount{Spec: authv1beta1.NatsAccountSpec{Exports: []authv1beta1.Export{
		{Name: "results", Type: authv1beta1.ExportTypeStream, Subject: "r.>"},
		{Name: "execute", Type: authv1beta1.ExportTypeService, Subject: "x", Access: authv1beta1.ExportAccessPrivate,
			Importers: []authv1beta1.Importer{{Kind: authv1beta1.AccountKindAccount}}},
		{Preset: authv1beta1.ExportPresetJetStreamStepdown},
	}}}
	got, err := accountExports(acc)
	require.NoError(t, err)
	var names []string
	for _, e := range got {
		names = append(names, e.Name)
	}
	require.Equal(t, []string{"results", "execute", "jetstream-stepdown-stream", "jetstream-stepdown-consumer"}, names)
	require.Equal(t, jwt.Stream, got[0].Type)
	require.False(t, got[0].Private)
	require.Equal(t, jwt.Service, got[1].Type)
	require.True(t, got[1].Private)
	require.Len(t, got[1].importers, 1)
}

func TestAccountLimits(t *testing.T) {
	n := func(v int64) *int64 { return &v }
	q := func(s string) *resource.Quantity { v := resource.MustParse(s); return &v }
	got := accountLimits(&authv1beta1.AccountLimits{
		Connections: n(500),
		Payload:     q("1Mi"),
		JetStream:   &authv1beta1.AccountJetStreamLimits{DiskStorage: q("50Gi"), Streams: n(20)},
	})
	require.Equal(t, jwtplane.Limits{
		Connections: 500,
		Payload:     1 << 20,
		JetStream:   &jwtplane.JetStreamLimits{DiskStorage: 50 << 30, Streams: 20},
	}, got)
	require.Equal(t, jwtplane.Limits{}, accountLimits(nil))

	require.Equal(t, jwtplane.Limits{JetStream: &jwtplane.JetStreamLimits{
		MaxAckPending: 1000, MemoryMaxStreamBytes: 1 << 20, DiskMaxStreamBytes: 1 << 30, MaxBytesRequired: true,
	}}, accountLimits(&authv1beta1.AccountLimits{JetStream: &authv1beta1.AccountJetStreamLimits{
		MaxAckPending: n(1000), MemoryMaxStreamBytes: q("1Mi"), DiskMaxStreamBytes: q("1Gi"), MaxBytesRequired: true,
	}}))

	tiers := []authv1beta1.AccountJetStreamTier{
		{Name: "R1", DiskStorage: q("10Gi"), Streams: n(10)},
		{Name: "R3", MemoryStorage: q("1Gi"), Consumers: n(100), MaxAckPending: n(1000), MemoryMaxStreamBytes: q("1Mi"), DiskMaxStreamBytes: q("1Gi"), MaxBytesRequired: true},
	}
	wantTiers := map[string]jwtplane.JetStreamLimits{
		"R1": {DiskStorage: 10 << 30, Streams: 10},
		"R3": {MemoryStorage: 1 << 30, Consumers: 100, MaxAckPending: 1000, MemoryMaxStreamBytes: 1 << 20, DiskMaxStreamBytes: 1 << 30, MaxBytesRequired: true},
	}
	require.Equal(t, jwtplane.Limits{JetStreamTiers: wantTiers},
		accountLimits(&authv1beta1.AccountLimits{JetStream: &authv1beta1.AccountJetStreamLimits{Tiers: tiers}}))
	require.Equal(t, jwtplane.Limits{JetStream: &jwtplane.JetStreamLimits{Streams: 5}, JetStreamTiers: wantTiers},
		accountLimits(&authv1beta1.AccountLimits{JetStream: &authv1beta1.AccountJetStreamLimits{Streams: n(5), Tiers: tiers}}),
		"limits for the account beside tiers reach the signer, which refuses them")
}

func TestImportLabel(t *testing.T) {
	require.Equal(t, "monitoring/execute", importLabel("nats-system", types.NamespacedName{Namespace: "nats-system", Name: "monitoring"}, "execute"))
	require.Equal(t, "team-a/monitoring/execute", importLabel("nats-system", types.NamespacedName{Namespace: "team-a", Name: "monitoring"}, "execute"))
}

func TestSetRetiringCondition(t *testing.T) {
	op := testKeys(t, nkeys.PrefixByteOperator, true, false)
	old, current := testPub(t, op.Signing[0].Pair), testPub(t, op.Signing[1].Pair)
	signWith := func(k jwtplane.SigningKey) string {
		acc := testKeys(t, nkeys.PrefixByteAccount)
		tok, err := jwtplane.SignAccount(jwtplane.Account{Keys: acc}, jwtplane.Keys{Signing: []jwtplane.SigningKey{k}}, time.Now())
		require.NoError(t, err)
		return tok
	}
	byOld := signWith(jwtplane.SigningKey{Name: "old", Pair: op.Signing[0].Pair})
	byCurrent := signWith(op.Signing[1])
	tests := []struct {
		name   string
		jwts   []string
		status metav1.ConditionStatus
	}{
		{name: "one still signed by the retiring key", jwts: []string{byCurrent, byOld, ""}, status: metav1.ConditionTrue},
		{name: "all re-signed", jwts: []string{byCurrent, ""}, status: metav1.ConditionFalse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &authv1beta1.NatsOperator{}
			setRetiringCondition(o, []string{old}, tt.jwts)
			cond := meta.FindStatusCondition(o.Status.Conditions, ConditionRetiringKeysInUse)
			require.NotNil(t, cond)
			require.Equal(t, tt.status, cond.Status)
		})
	}
	require.NotEqual(t, old, current)
}
