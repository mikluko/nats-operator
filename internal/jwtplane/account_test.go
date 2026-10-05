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
			require.Equal(t, time.Unix(c.IssuedAt+(c.Expires-c.IssuedAt)/2, 0), renew)
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
			name: "JetStream with stream and consumer bounds",
			limits: jwtplane.Limits{JetStream: &jwtplane.JetStreamLimits{
				MaxAckPending: 1000, MemoryMaxStreamBytes: 1 << 20, DiskMaxStreamBytes: 1 << 30, MaxBytesRequired: true,
			}},
			want: jwt.OperatorLimits{
				NatsLimits:    jwt.NatsLimits{Subs: -1, Data: -1, Payload: -1},
				AccountLimits: jwt.AccountLimits{Imports: -1, Exports: -1, WildcardExports: true, Conn: -1, LeafNodeConn: -1},
				JetStreamLimits: jwt.JetStreamLimits{
					MemoryStorage: -1, DiskStorage: -1, Streams: -1, Consumer: -1,
					MaxAckPending: 1000, MemoryMaxStreamBytes: 1 << 20, DiskMaxStreamBytes: 1 << 30, MaxBytesRequired: true,
				},
			},
		},
		{
			name: "JetStream by tier",
			limits: jwtplane.Limits{JetStreamTiers: map[string]jwtplane.JetStreamLimits{
				"R1": {DiskStorage: 10 << 30, Streams: 10},
				"R3": {MemoryStorage: 1 << 30, DiskStorage: 50 << 30, Consumers: 100, MaxAckPending: 1000, DiskMaxStreamBytes: 1 << 30, MaxBytesRequired: true},
			}},
			want: jwt.OperatorLimits{
				NatsLimits:    jwt.NatsLimits{Subs: -1, Data: -1, Payload: -1},
				AccountLimits: jwt.AccountLimits{Imports: -1, Exports: -1, WildcardExports: true, Conn: -1, LeafNodeConn: -1},
				JetStreamTieredLimits: jwt.JetStreamTieredLimits{
					"R1": {MemoryStorage: -1, DiskStorage: 10 << 30, Streams: 10, Consumer: -1},
					"R3": {MemoryStorage: 1 << 30, DiskStorage: 50 << 30, Streams: -1, Consumer: 100, MaxAckPending: 1000, DiskMaxStreamBytes: 1 << 30, MaxBytesRequired: true},
				},
			},
		},
		{
			name:    "negative",
			limits:  jwtplane.Limits{Connections: -5},
			wantErr: jwtplane.ErrNegativeLimit,
		},
		{
			name:    "negative in a tier",
			limits:  jwtplane.Limits{JetStreamTiers: map[string]jwtplane.JetStreamLimits{"R1": {MaxAckPending: -1}}},
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
			require.Equal(t, tt.want.JetStreamTieredLimits, c.Limits.JetStreamTieredLimits)
		})
	}
}

func TestSignAccountTiersBesideAccountLimits(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	acc := newKeys(t, nkeys.PrefixByteAccount, "s")
	limits := jwtplane.Limits{
		JetStream:      &jwtplane.JetStreamLimits{},
		JetStreamTiers: map[string]jwtplane.JetStreamLimits{"R1": {}},
	}
	_, err := jwtplane.SignAccount(jwtplane.Account{Keys: acc, Limits: limits}, op, time.Now())
	require.ErrorContains(t, err, "mutually exclusive")
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

func TestSignAccountImportFlags(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	core := newKeys(t, nkeys.PrefixByteAccount, "s")
	exporter := pub(t, newPair(t, nkeys.PrefixByteAccount))
	stream := jwtplane.Export{Name: "events", Type: jwt.Stream, Subject: "events.>"}
	service := jwtplane.Export{Name: "execute", Type: jwt.Service, Subject: "execute"}

	tok, err := jwtplane.SignAccount(jwtplane.Account{Keys: core, Imports: []jwtplane.Import{
		{Account: exporter, Export: stream, AllowTrace: true},
		{Account: exporter, Export: service, Share: true},
	}}, op, time.Now())
	require.NoError(t, err)
	c, err := jwt.DecodeAccountClaims(tok)
	require.NoError(t, err)
	flags := map[string][2]bool{}
	for _, i := range c.Imports {
		flags[i.Name] = [2]bool{i.Share, i.AllowTrace}
	}
	require.Equal(t, map[string][2]bool{"events": {false, true}, "execute": {true, false}}, flags)

	for name, imp := range map[string]jwtplane.Import{
		"share on a stream":        {Account: exporter, Export: stream, Share: true},
		"allow trace on a service": {Account: exporter, Export: service, AllowTrace: true},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := jwtplane.SignAccount(jwtplane.Account{Keys: core, Imports: []jwtplane.Import{imp}}, op, time.Now())
			require.Error(t, err)
		})
	}
}

func TestValidateImport(t *testing.T) {
	exporter := newKeys(t, nkeys.PrefixByteAccount, "s")
	exporterPub := pub(t, exporter.Identity)
	importer := pub(t, newPair(t, nkeys.PrefixByteAccount))
	other := pub(t, newPair(t, nkeys.PrefixByteAccount))
	private := jwtplane.Export{Name: "events", Type: jwt.Stream, Subject: "billing.events.>", Private: true, Importers: []string{importer, other}}
	token, err := jwtplane.SignActivation(exporter, private, importer)
	require.NoError(t, err)
	forOther, err := jwtplane.SignActivation(exporter, private, other)
	require.NoError(t, err)
	byOther, err := jwtplane.SignActivation(newKeys(t, nkeys.PrefixByteAccount, "s"), private, importer)
	require.NoError(t, err)

	tests := []struct {
		name    string
		imp     jwtplane.Import
		wantErr string
	}{
		{name: "public", imp: jwtplane.Import{Account: exporterPub, Export: jwtplane.Export{Name: "events", Type: jwt.Stream, Subject: "billing.events.>"}}},
		{name: "private with its token", imp: jwtplane.Import{Account: exporterPub, Export: private, Token: token}},
		{name: "narrower than the token", imp: jwtplane.Import{Account: exporterPub, Export: jwtplane.Export{Name: "events", Type: jwt.Stream, Subject: "billing.events.eu.>", Private: true}, Token: token}},
		{name: "private without a token", imp: jwtplane.Import{Account: exporterPub, Export: private}, wantErr: jwtplane.ErrActivationRequired.Error()},
		{name: "token for another account", imp: jwtplane.Import{Account: exporterPub, Export: private, Token: forOther}, wantErr: "doesn't match account it is being included in"},
		{name: "token by another account", imp: jwtplane.Import{Account: exporterPub, Export: private, Token: byOther}, wantErr: "doesn't match account for import"},
		{name: "token for another subject", imp: jwtplane.Import{Account: exporterPub, Export: jwtplane.Export{Name: "events", Type: jwt.Stream, Subject: "billing.other.>", Private: true}, Token: token}, wantErr: "doesn't match import"},
		{name: "token of another type", imp: jwtplane.Import{Account: exporterPub, Export: jwtplane.Export{Name: "events", Type: jwt.Service, Subject: "billing.events.>", Private: true}, Token: token}, wantErr: "mismatch between token import type"},
		{name: "not a token", imp: jwtplane.Import{Account: exporterPub, Export: private, Token: "nope"}, wantErr: "invalid activation token"},
		{name: "share on a stream", imp: jwtplane.Import{Account: exporterPub, Export: jwtplane.Export{Name: "events", Type: jwt.Stream, Subject: "billing.events.>"}, Share: true}, wantErr: "only valid for services"},
		{name: "exporter not an account", imp: jwtplane.Import{Account: importer[1:], Export: private}, wantErr: jwtplane.ErrWrongKeyType.Error()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := jwtplane.ValidateImport(tt.imp, importer)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestMonitoringImport(t *testing.T) {
	sys := pub(t, newPair(t, nkeys.PrefixByteAccount))
	importer := pub(t, newPair(t, nkeys.PrefixByteAccount))

	services, ok := jwtplane.MonitoringImport("account-monitoring-services", sys, importer)
	require.True(t, ok)
	require.Equal(t, jwtplane.Import{Account: sys, Export: jwtplane.Export{
		Name: "account-monitoring-services", Type: jwt.Service, Subject: "$SYS.REQ.ACCOUNT." + importer + ".*", ResponseType: jwt.ResponseTypeStream,
	}}, services)

	streams, ok := jwtplane.MonitoringImport("account-monitoring-streams", sys, importer)
	require.True(t, ok)
	require.Equal(t, jwtplane.Import{Account: sys, Export: jwtplane.Export{
		Name: "account-monitoring-streams", Type: jwt.Stream, Subject: "$SYS.ACCOUNT." + importer + ".>",
	}}, streams)

	_, ok = jwtplane.MonitoringImport("events", sys, importer)
	require.False(t, ok)

	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	tok, err := jwtplane.SignAccount(jwtplane.Account{Keys: newKeys(t, nkeys.PrefixByteAccount, "s"), Imports: []jwtplane.Import{services, streams}}, op, time.Now())
	require.NoError(t, err)
	c, err := jwt.DecodeAccountClaims(tok)
	require.NoError(t, err)
	require.Len(t, c.Imports, 2)
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
		{PublicKey: jwt.All, At: now.Add(-time.Hour)},
	}}, op, now)
	require.NoError(t, err)
	c, err := jwt.DecodeAccountClaims(tok)
	require.NoError(t, err)
	require.Equal(t, jwt.RevocationList{current: now.Unix(), earlier: now.Add(-2 * time.Hour).Unix(), jwt.All: now.Add(-time.Hour).Unix()}, c.Revocations)

	_, err = jwtplane.SignAccount(jwtplane.Account{Keys: acc, Revocations: []jwtplane.Revocation{{PublicKey: pub(t, acc.Identity), At: now}}}, op, now)
	require.ErrorIs(t, err, jwtplane.ErrWrongKeyType)
}

func TestSignSystemAccount(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	sys := newKeys(t, nkeys.PrefixByteAccount, "s")
	payments := pub(t, newPair(t, nkeys.PrefixByteAccount))

	tok, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "sys", Keys: sys, StepdownAccounts: []string{payments}}, op)
	require.NoError(t, err)
	c, err := jwt.DecodeAccountClaims(tok)
	require.NoError(t, err)
	require.Zero(t, c.Expires)
	require.False(t, c.Limits.IsJSEnabled())
	require.Equal(t, pub(t, op.Signing[0].Pair), c.Issuer)
	require.ElementsMatch(t, jwt.Exports{
		{Name: "account-monitoring-services", Subject: "$SYS.REQ.ACCOUNT.*.*", Type: jwt.Service, ResponseType: jwt.ResponseTypeStream, AccountTokenPosition: 4},
		{Name: "account-monitoring-streams", Subject: "$SYS.ACCOUNT.*.>", Type: jwt.Stream, AccountTokenPosition: 3},
	}, c.Exports)

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

func TestKeepActivation(t *testing.T) {
	exporter := newKeys(t, nkeys.PrefixByteAccount, "s")
	importer := pub(t, newPair(t, nkeys.PrefixByteAccount))
	private := jwtplane.Export{Name: "events", Type: jwt.Stream, Subject: "billing.events.>", Private: true, Importers: []string{importer}}
	hc := jwt.NewActivationClaims(importer)
	hc.Name, hc.ImportSubject, hc.ImportType = private.Name, jwt.Subject(private.Subject), private.Type
	hc.IssuerAccount = pub(t, exporter.Identity)
	hc.Tags.Add("held")
	held, err := hc.Encode(exporter.Signing[0].Pair)
	require.NoError(t, err)

	rotated := exporter
	rotated.Signing = []jwtplane.SigningKey{{Name: "s2", Pair: newPair(t, nkeys.PrefixByteAccount)}, {Name: "s", Pair: exporter.Signing[0].Pair, Retiring: true}}
	moved := private
	moved.Subject = "billing.other.>"

	tests := []struct {
		name     string
		held     string
		exporter jwtplane.Keys
		export   jwtplane.Export
		kept     bool
	}{
		{name: "same key and claims", held: held, exporter: exporter, export: private, kept: true},
		{name: "none held", exporter: exporter, export: private},
		{name: "signing key rotated", held: held, exporter: rotated, export: private},
		{name: "subject changed", held: held, exporter: exporter, export: moved},
		{name: "not a token", held: "x", exporter: exporter, export: private},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok, err := jwtplane.KeepActivation(tt.held, tt.exporter, tt.export, importer)
			require.NoError(t, err)
			if tt.kept {
				require.Equal(t, held, tok)
				return
			}
			c, err := jwt.DecodeActivationClaims(tok)
			require.NoError(t, err)
			require.Empty(t, c.Tags, "minted afresh")
			require.Equal(t, pub(t, tt.exporter.Signing[0].Pair), c.Issuer)
			require.Equal(t, importer, c.Subject)
			require.Equal(t, jwt.Subject(tt.export.Subject), c.ImportSubject)
		})
	}
}
