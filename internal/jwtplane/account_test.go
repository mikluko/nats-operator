package jwtplane_test

import (
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

func TestSignAccountSigner(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "old", "active")
	op.Signing[0].Retiring, op.Signing[1].Retiring = true, false
	acc := newKeys(t, nkeys.PrefixByteAccount, "a1", "a2")

	for name, keys := range map[string]jwtplane.Keys{"identity held": acc, "identity offline": offline(t, acc)} {
		t.Run(name, func(t *testing.T) {
			tok, err := jwtplane.SignAccount(jwtplane.Account{Name: "orders", Keys: keys}, op, time.Now())
			require.NoError(t, err)
			c, err := jwt.DecodeAccountClaims(tok)
			require.NoError(t, err)
			require.Equal(t, pub(t, acc.Identity), c.Subject)
			require.Equal(t, pub(t, op.Signing[1].Pair), c.Issuer, "the first signing key not retiring")
			require.ElementsMatch(t, []string{pub(t, acc.Signing[0].Pair), pub(t, acc.Signing[1].Pair)}, c.SigningKeys.Keys())
		})
	}
}

func TestSignAccountTTL(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	acc := newKeys(t, nkeys.PrefixByteAccount, "s")
	now := time.Now()
	tests := []struct {
		name    string
		ttl     time.Duration
		want    time.Duration
		wantErr bool
	}{
		{name: "default", want: 48 * time.Hour},
		{name: "explicit", ttl: 2 * time.Hour, want: 2 * time.Hour},
		{name: "negative", ttl: -time.Hour, wantErr: true},
	}
	t.Run("no expiry", func(t *testing.T) {
		tok, err := jwtplane.SignAccount(jwtplane.Account{Keys: acc, TTL: time.Hour, NoExpiry: true}, op, now)
		require.NoError(t, err)
		c, err := jwt.DecodeAccountClaims(tok)
		require.NoError(t, err)
		require.Zero(t, c.Expires)
		renew, err := jwtplane.RenewAt(tok)
		require.NoError(t, err)
		require.True(t, renew.IsZero())
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok, err := jwtplane.SignAccount(jwtplane.Account{Keys: acc, TTL: tt.ttl}, op, now)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			c, err := jwt.DecodeAccountClaims(tok)
			require.NoError(t, err)
			require.Equal(t, now.Add(tt.want).Unix(), c.Expires)

			renew, err := jwtplane.RenewAt(tok)
			require.NoError(t, err)
			require.Equal(t, time.Unix(c.IssuedAt+int64(tt.want/time.Second)/2, 0), renew)
		})
	}
}

func TestSignAccountLimits(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	acc := newKeys(t, nkeys.PrefixByteAccount, "s")
	tests := []struct {
		name    string
		limits  jwtplane.Limits
		want    jwt.OperatorLimits
		wantErr error
	}{
		{
			name: "zero is unlimited, JetStream disabled",
			want: jwt.OperatorLimits{
				NatsLimits:      jwt.NatsLimits{Subs: -1, Data: -1, Payload: -1},
				AccountLimits:   jwt.AccountLimits{Imports: -1, Exports: -1, WildcardExports: true, Conn: -1, LeafNodeConn: -1},
				JetStreamLimits: jwt.JetStreamLimits{},
			},
		},
		{
			name: "story 2 orders",
			limits: jwtplane.Limits{
				Connections: 500, Subscriptions: 10000, Payload: 1 << 20,
				JetStream: &jwtplane.JetStreamLimits{MemoryStorage: 1 << 30, DiskStorage: 50 << 30, Streams: 20, Consumers: 200},
			},
			want: jwt.OperatorLimits{
				NatsLimits:      jwt.NatsLimits{Subs: 10000, Data: -1, Payload: 1 << 20},
				AccountLimits:   jwt.AccountLimits{Imports: -1, Exports: -1, WildcardExports: true, Conn: 500, LeafNodeConn: -1},
				JetStreamLimits: jwt.JetStreamLimits{MemoryStorage: 1 << 30, DiskStorage: 50 << 30, Streams: 20, Consumer: 200},
			},
		},
		{
			name:   "JetStream with only disk storage set",
			limits: jwtplane.Limits{JetStream: &jwtplane.JetStreamLimits{DiskStorage: 50 << 30}},
			want: jwt.OperatorLimits{
				NatsLimits:      jwt.NatsLimits{Subs: -1, Data: -1, Payload: -1},
				AccountLimits:   jwt.AccountLimits{Imports: -1, Exports: -1, WildcardExports: true, Conn: -1, LeafNodeConn: -1},
				JetStreamLimits: jwt.JetStreamLimits{MemoryStorage: -1, DiskStorage: 50 << 30, Streams: -1, Consumer: -1},
			},
		},
		{
			name:    "negative",
			limits:  jwtplane.Limits{Connections: -5},
			wantErr: jwtplane.ErrNegativeLimit,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok, err := jwtplane.SignAccount(jwtplane.Account{Keys: acc, Limits: tt.limits}, op, time.Now())
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			c, err := jwt.DecodeAccountClaims(tok)
			require.NoError(t, err)
			require.Equal(t, tt.want.NatsLimits, c.Limits.NatsLimits)
			require.Equal(t, tt.want.AccountLimits, c.Limits.AccountLimits)
			require.Equal(t, tt.want.JetStreamLimits, c.Limits.JetStreamLimits)
		})
	}
}

func TestSignAccountExports(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	acc := newKeys(t, nkeys.PrefixByteAccount, "s")
	core := pub(t, newPair(t, nkeys.PrefixByteAccount))
	stepdown, err := jwtplane.ExportPreset(jwtplane.ExportPresetJetStreamStepdown)
	require.NoError(t, err)

	exports := append([]jwtplane.Export{
		{Name: "check-results", Type: jwt.Stream, Subject: "monitoring.results.>"},
		{Name: "execute", Type: jwt.Service, Subject: "monitoring.execute", Private: true, Importers: []string{core}},
		{Name: "stream-reply", Type: jwt.Service, Subject: "monitoring.stream", ResponseType: jwt.ResponseTypeStream},
	}, stepdown...)
	tok, err := jwtplane.SignAccount(jwtplane.Account{Keys: acc, Exports: exports}, op, time.Now())
	require.NoError(t, err)
	c, err := jwt.DecodeAccountClaims(tok)
	require.NoError(t, err)

	type got struct {
		Subject  jwt.Subject
		Type     jwt.ExportType
		TokenReq bool
		Response jwt.ResponseType
	}
	var exps []got
	for _, e := range c.Exports {
		exps = append(exps, got{e.Subject, e.Type, e.TokenReq, e.ResponseType})
	}
	require.ElementsMatch(t, []got{
		{"monitoring.results.>", jwt.Stream, false, ""},
		{"monitoring.execute", jwt.Service, true, jwt.ResponseTypeSingleton},
		{"monitoring.stream", jwt.Service, false, jwt.ResponseTypeStream},
		{"$JS.API.STREAM.LEADER.STEPDOWN.*", jwt.Service, false, jwt.ResponseTypeSingleton},
		{"$JS.API.CONSUMER.LEADER.STEPDOWN.*.*", jwt.Service, false, jwt.ResponseTypeSingleton},
	}, exps)

	_, err = jwtplane.ExportPreset("nope")
	require.Error(t, err)
}

func TestSignAccountImports(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	monitoring := newKeys(t, nkeys.PrefixByteAccount, "s")
	core := newKeys(t, nkeys.PrefixByteAccount, "s")
	monPub, corePub := pub(t, monitoring.Identity), pub(t, core.Identity)

	results := jwtplane.Export{Name: "check-results", Type: jwt.Stream, Subject: "monitoring.results.>"}
	execute := jwtplane.Export{Name: "execute", Type: jwt.Service, Subject: "monitoring.execute", Private: true, Importers: []string{corePub}}
	token, err := jwtplane.SignActivation(monitoring, execute, corePub)
	require.NoError(t, err)
	otherToken, err := jwtplane.SignActivation(monitoring, jwtplane.Export{
		Name: "execute", Type: jwt.Service, Subject: "monitoring.other", Private: true, Importers: []string{corePub},
	}, corePub)
	require.NoError(t, err)

	tests := []struct {
		name      string
		imports   []jwtplane.Import
		wantLocal []jwt.RenamingSubject
		wantErr   error
		anyErr    bool
	}{
		{
			name: "public stream renamed, private service with token",
			imports: []jwtplane.Import{
				{Account: monPub, Export: results, LocalSubject: "upstream.results.>"},
				{Account: monPub, Export: execute, Token: token},
			},
			wantLocal: []jwt.RenamingSubject{"upstream.results.>", ""},
		},
		{
			name:      "local subject equal to the export's is left unset",
			imports:   []jwtplane.Import{{Account: monPub, Export: results, LocalSubject: "monitoring.results.>"}},
			wantLocal: []jwt.RenamingSubject{""},
		},
		{
			name:    "private without token",
			imports: []jwtplane.Import{{Account: monPub, Export: execute}},
			wantErr: jwtplane.ErrActivationRequired,
		},
		{
			name:    "token for another subject",
			imports: []jwtplane.Import{{Account: monPub, Export: execute, Token: otherToken}},
			anyErr:  true,
		},
		{
			name:    "exporter not an account",
			imports: []jwtplane.Import{{Account: corePub[1:], Export: results}},
			wantErr: jwtplane.ErrWrongKeyType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok, err := jwtplane.SignAccount(jwtplane.Account{Keys: core, Imports: tt.imports}, op, time.Now())
			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
				return
			case tt.anyErr:
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			c, err := jwt.DecodeAccountClaims(tok)
			require.NoError(t, err)
			require.Len(t, c.Imports, len(tt.wantLocal))
			byName := map[string]*jwt.Import{}
			for _, imp := range c.Imports {
				byName[imp.Name] = imp
			}
			for i := range tt.imports {
				imp := byName[tt.imports[i].Export.Name]
				require.NotNil(t, imp)
				require.Equal(t, monPub, imp.Account)
				require.Equal(t, jwt.Subject(tt.imports[i].Export.Subject), imp.Subject)
				require.Equal(t, tt.imports[i].Export.Type, imp.Type)
				require.Equal(t, tt.wantLocal[i], imp.LocalSubject)
				require.Equal(t, tt.imports[i].Token, imp.Token)
			}
		})
	}
}

func TestSignAccountRevocations(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	acc := newKeys(t, nkeys.PrefixByteAccount, "s")
	now := time.Now()
	current := pub(t, newPair(t, nkeys.PrefixByteUser))
	earlier := pub(t, newPair(t, nkeys.PrefixByteUser))

	tok, err := jwtplane.SignAccount(jwtplane.Account{Keys: acc, Revocations: []jwtplane.Revocation{
		{PublicKey: current, At: now},
		{PublicKey: earlier, At: now.Add(-2 * time.Hour)},
	}}, op, now)
	require.NoError(t, err)
	c, err := jwt.DecodeAccountClaims(tok)
	require.NoError(t, err)
	require.Equal(t, jwt.RevocationList{current: now.Unix(), earlier: now.Add(-2 * time.Hour).Unix()}, c.Revocations)

	_, err = jwtplane.SignAccount(jwtplane.Account{Keys: acc, Revocations: []jwtplane.Revocation{{PublicKey: pub(t, acc.Identity), At: now}}}, op, now)
	require.ErrorIs(t, err, jwtplane.ErrWrongKeyType)
}

func TestSignSystemAccount(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	sys := newKeys(t, nkeys.PrefixByteAccount, "s")
	payments := pub(t, newPair(t, nkeys.PrefixByteAccount))

	tok, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "sys", Keys: sys, StepdownAccounts: []string{payments}}, op, time.Now())
	require.NoError(t, err)
	c, err := jwt.DecodeAccountClaims(tok)
	require.NoError(t, err)
	require.Zero(t, c.Expires)
	require.False(t, c.Limits.IsJSEnabled())
	require.Equal(t, pub(t, op.Signing[0].Pair), c.Issuer)

	type got struct {
		Subject jwt.Subject
		Local   jwt.RenamingSubject
		Type    jwt.ExportType
	}
	var imps []got
	for _, i := range c.Imports {
		require.Equal(t, payments, i.Account)
		imps = append(imps, got{i.Subject, i.LocalSubject, i.Type})
	}
	require.ElementsMatch(t, []got{
		{"$JS.API.STREAM.LEADER.STEPDOWN.*", jwt.RenamingSubject("acc." + payments + ".$JS.API.STREAM.LEADER.STEPDOWN.*"), jwt.Service},
		{"$JS.API.CONSUMER.LEADER.STEPDOWN.*.*", jwt.RenamingSubject("acc." + payments + ".$JS.API.CONSUMER.LEADER.STEPDOWN.*.*"), jwt.Service},
	}, imps)
	require.Equal(t, "acc."+payments+".$JS.API.STREAM.LEADER.STEPDOWN.S", jwtplane.StreamStepdownSubject(payments, "S"))
	require.Equal(t, "acc."+payments+".$JS.API.CONSUMER.LEADER.STEPDOWN.S.D", jwtplane.ConsumerStepdownSubject(payments, "S", "D"))
}

func TestSignActivation(t *testing.T) {
	exporter := newKeys(t, nkeys.PrefixByteAccount, "s")
	importer := pub(t, newPair(t, nkeys.PrefixByteAccount))
	other := pub(t, newPair(t, nkeys.PrefixByteAccount))
	private := jwtplane.Export{Name: "execute", Type: jwt.Service, Subject: "monitoring.execute", Private: true, Importers: []string{importer}}

	tests := []struct {
		name     string
		exporter jwtplane.Keys
		export   jwtplane.Export
		importer string
		wantErr  error
	}{
		{name: "listed importer", exporter: exporter, export: private, importer: importer},
		{name: "offline exporter identity", exporter: offline(t, exporter), export: private, importer: importer},
		{name: "unlisted importer", exporter: exporter, export: private, importer: other, wantErr: jwtplane.ErrNotImporter},
		{name: "public export", exporter: exporter, export: jwtplane.Export{Name: "x", Type: jwt.Stream, Subject: "x"}, importer: importer, wantErr: jwtplane.ErrNotPrivate},
		{name: "importer not an account", exporter: exporter, export: private, importer: pub(t, newPair(t, nkeys.PrefixByteUser)), wantErr: jwtplane.ErrWrongKeyType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok, err := jwtplane.SignActivation(tt.exporter, tt.export, tt.importer)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			c, err := jwt.DecodeActivationClaims(tok)
			require.NoError(t, err)
			require.Equal(t, tt.importer, c.Subject)
			require.Equal(t, pub(t, exporter.Signing[0].Pair), c.Issuer, "signed by the signing key")
			require.Equal(t, pub(t, exporter.Identity), c.IssuerAccount)
			require.Equal(t, jwt.Subject(tt.export.Subject), c.ImportSubject)
			require.Equal(t, tt.export.Type, c.ImportType)
			require.Zero(t, c.Expires)
		})
	}
}
