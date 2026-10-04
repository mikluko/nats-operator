package jwtplane_test

import (
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

func scopedKeys(t *testing.T) jwtplane.Keys {
	t.Helper()
	acc := newKeys(t, nkeys.PrefixByteAccount, "old-reader", "reader", "plain", "writer")
	for i := range acc.Signing {
		acc.Signing[i].Retiring = i == 0
	}
	acc.Signing[0].Scope = &jwtplane.UserScope{Role: "reader"}
	acc.Signing[1].Scope = &jwtplane.UserScope{
		Role: "reader",
		Permissions: &jwtplane.Permissions{
			Publish:   jwtplane.SubjectPermissions{Allow: []string{"in.>"}, Deny: []string{"in.secret"}},
			Subscribe: jwtplane.SubjectPermissions{Allow: []string{"out.>"}, Deny: []string{"out.secret"}},
		},
		AllowedConnectionTypes: []string{jwt.ConnectionTypeStandard},
		Subscriptions:          5,
		Payload:                1024,
	}
	acc.Signing[3].Scope = &jwtplane.UserScope{Role: "writer"}
	return acc
}

func TestSignAccountScopedSigningKeys(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	acc := scopedKeys(t)

	sysTok, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "SYS", Keys: acc}, op)
	require.NoError(t, err)
	accTok, err := jwtplane.SignAccount(jwtplane.Account{Name: "a", Keys: acc}, op, time.Now())
	require.NoError(t, err)

	for name, tok := range map[string]string{"account": accTok, "system account": sysTok} {
		t.Run(name, func(t *testing.T) {
			c, err := jwt.DecodeAccountClaims(tok)
			require.NoError(t, err)
			require.Len(t, c.SigningKeys, 4)

			plain, listed := c.SigningKeys.GetScope(pub(t, acc.Signing[2].Pair))
			require.True(t, listed)
			require.Nil(t, plain)

			want := jwt.NewUserScope()
			want.Key, want.Role = pub(t, acc.Signing[1].Pair), "reader"
			want.Template.Pub.Allow.Add("in.>")
			want.Template.Pub.Deny.Add("in.secret")
			want.Template.Sub.Allow.Add("out.>")
			want.Template.Sub.Deny.Add("out.secret")
			want.Template.AllowedConnectionTypes.Add(jwt.ConnectionTypeStandard)
			want.Template.Subs, want.Template.Payload = 5, 1024
			got, _ := c.SigningKeys.GetScope(want.Key)
			require.Equal(t, want, got)

			unlimited := jwt.NewUserScope()
			unlimited.Key, unlimited.Role = pub(t, acc.Signing[3].Pair), "writer"
			got, _ = c.SigningKeys.GetScope(unlimited.Key)
			require.Equal(t, unlimited, got, "an unset limit is unlimited")
		})
	}
}

func TestSignAccountScopeRefused(t *testing.T) {
	op := newKeys(t, nkeys.PrefixByteOperator, "s")
	tests := []struct {
		name    string
		scope   jwtplane.UserScope
		wantErr error
	}{
		{name: "negative subscriptions", scope: jwtplane.UserScope{Role: "r", Subscriptions: -1}, wantErr: jwtplane.ErrNegativeLimit},
		{name: "negative payload", scope: jwtplane.UserScope{Role: "r", Payload: -1}, wantErr: jwtplane.ErrNegativeLimit},
		{name: "unknown connection type", scope: jwtplane.UserScope{Role: "r", AllowedConnectionTypes: []string{"CARRIER_PIGEON"}}, wantErr: jwtplane.ErrConnectionType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := newKeys(t, nkeys.PrefixByteAccount, "s")
			acc.Signing[0].Scope = &tt.scope
			_, err := jwtplane.SignAccount(jwtplane.Account{Keys: acc}, op, time.Now())
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestSignOperatorScopedKeyRefused(t *testing.T) {
	sys := pub(t, newPair(t, nkeys.PrefixByteAccount))
	op := newKeys(t, nkeys.PrefixByteOperator, "plain", "scoped")
	op.Signing[1].Scope = &jwtplane.UserScope{Role: "r"}
	_, err := jwtplane.SignOperator(jwtplane.Operator{Keys: op, SystemAccount: sys})
	require.ErrorIs(t, err, jwtplane.ErrScopedOperatorKey)
}

func TestSignUserRole(t *testing.T) {
	acc := scopedKeys(t)
	user := pub(t, newPair(t, nkeys.PrefixByteUser))

	t.Run("role is signed by its scoped key that is not retiring, with empty claims", func(t *testing.T) {
		tok, err := jwtplane.SignUser(jwtplane.User{PublicKey: user, Role: "reader"}, acc)
		require.NoError(t, err)
		c, err := jwt.DecodeUserClaims(tok)
		require.NoError(t, err)
		require.Equal(t, pub(t, acc.Signing[1].Pair), c.Issuer)
		require.Equal(t, pub(t, acc.Identity), c.IssuerAccount)
		require.True(t, c.HasEmptyPermissions())
		require.NoError(t, jwt.UserScope{Key: c.Issuer}.ValidateScopedSigner(c))
	})

	t.Run("no role is signed by the first key that is neither retiring nor scoped", func(t *testing.T) {
		tok, err := jwtplane.SignUser(jwtplane.User{PublicKey: user}, acc)
		require.NoError(t, err)
		c, err := jwt.DecodeUserClaims(tok)
		require.NoError(t, err)
		require.Equal(t, pub(t, acc.Signing[2].Pair), c.Issuer)
		require.False(t, c.HasEmptyPermissions())
	})

	t.Run("activation is signed by the first key that is neither retiring nor scoped", func(t *testing.T) {
		importer := pub(t, newPair(t, nkeys.PrefixByteAccount))
		tok, err := jwtplane.SignActivation(acc, jwtplane.Export{Name: "e", Type: jwt.Service, Subject: "e", Private: true, Importers: []string{importer}}, importer)
		require.NoError(t, err)
		c, err := jwt.DecodeActivationClaims(tok)
		require.NoError(t, err)
		require.Equal(t, pub(t, acc.Signing[2].Pair), c.Issuer)
	})

	refused := []struct {
		name    string
		user    jwtplane.User
		keys    jwtplane.Keys
		wantErr error
	}{
		{name: "role no key has", user: jwtplane.User{Role: "admin"}, keys: acc, wantErr: jwtplane.ErrNoScopedSigningKey},
		{name: "role whose every key is retiring", user: jwtplane.User{Role: "reader"}, keys: jwtplane.Keys{Identity: acc.Identity, Signing: acc.Signing[:1]}, wantErr: jwtplane.ErrNoScopedSigningKey},
		{name: "no role where every key is scoped", user: jwtplane.User{}, keys: jwtplane.Keys{Identity: acc.Identity, Signing: acc.Signing[:2]}, wantErr: jwtplane.ErrNoActiveSigningKey},
		{name: "role beside permissions", user: jwtplane.User{Role: "reader", Permissions: &jwtplane.Permissions{}}, keys: acc, wantErr: jwtplane.ErrRoleAndClaims},
		{name: "role beside connection types", user: jwtplane.User{Role: "reader", AllowedConnectionTypes: []string{jwt.ConnectionTypeStandard}}, keys: acc, wantErr: jwtplane.ErrRoleAndClaims},
		{name: "role beside preset", user: jwtplane.User{Role: "reader", Preset: jwtplane.PresetLeafnode}, keys: acc, wantErr: jwtplane.ErrRoleAndClaims},
	}
	for _, tt := range refused {
		t.Run(tt.name, func(t *testing.T) {
			tt.user.PublicKey = user
			_, err := jwtplane.SignUser(tt.user, tt.keys)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
