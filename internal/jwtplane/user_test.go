package jwtplane_test

import (
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

func TestSignUser(t *testing.T) {
	acc := newKeys(t, nkeys.PrefixByteAccount, "s")
	user := pub(t, newPair(t, nkeys.PrefixByteUser))

	type want struct {
		pubAllow, pubDeny, subAllow, subDeny, connTypes []string
	}
	tests := []struct {
		name    string
		user    jwtplane.User
		want    want
		wantErr error
		anyErr  bool
	}{
		{
			name: "permissions",
			user: jwtplane.User{Permissions: &jwtplane.Permissions{
				Publish:   jwtplane.SubjectPermissions{Allow: []string{"orders.>", "$JS.API.>"}, Deny: []string{"orders.secret"}},
				Subscribe: jwtplane.SubjectPermissions{Allow: []string{"orders.>", "_INBOX.>"}},
			}},
			want: want{
				pubAllow: []string{"orders.>", "$JS.API.>"}, pubDeny: []string{"orders.secret"},
				subAllow: []string{"orders.>", "_INBOX.>"},
			},
		},
		{name: "no permissions allows everything"},
		{
			name: "connection types",
			user: jwtplane.User{AllowedConnectionTypes: []string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeWebsocket}},
			want: want{connTypes: []string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeWebsocket}},
		},
		{
			name:    "unknown connection type",
			user:    jwtplane.User{AllowedConnectionTypes: []string{"CARRIER_PIGEON"}},
			wantErr: jwtplane.ErrConnectionType,
		},
		{
			name: "readonly",
			user: jwtplane.User{Preset: jwtplane.PresetReadonly},
			want: want{
				pubAllow: []string{
					"$JS.API.INFO", "$JS.API.STREAM.NAMES", "$JS.API.STREAM.LIST", "$JS.API.STREAM.INFO.*",
					"$JS.API.CONSUMER.NAMES.*", "$JS.API.CONSUMER.LIST.*", "$JS.API.CONSUMER.INFO.*.*",
				},
				subAllow: []string{">"},
			},
		},
		{
			name: "leafnode in an ordinary account",
			user: jwtplane.User{Preset: jwtplane.PresetLeafnode},
			want: want{connTypes: []string{jwt.ConnectionTypeLeafnode}},
		},
		{
			name: "leafnode in the system account",
			user: jwtplane.User{Preset: jwtplane.PresetLeafnode, SystemAccount: true},
			want: want{connTypes: []string{jwt.ConnectionTypeLeafnode}},
		},
		{
			name: "auth controller",
			user: jwtplane.User{Preset: jwtplane.PresetAuthController, SystemAccount: true},
			want: want{
				pubAllow: []string{
					"$SYS.REQ.CLAIMS.UPDATE", "$SYS.REQ.CLAIMS.DELETE", "$SYS.REQ.ACCOUNT.*.CLAIMS.LOOKUP",
					"$SYS.REQ.SERVER.PING.STATSZ", "$SYS.REQ.SERVER.PING.CONNZ", "$SYS.REQ.SERVER.*.KICK",
				},
				subAllow: []string{"_INBOX.>", "$SYS.SERVER.*.STATSZ"},
			},
		},
		{
			name: "jetstream controller",
			user: jwtplane.User{Preset: jwtplane.PresetJetStreamController, SystemAccount: true},
			want: want{
				pubAllow: []string{
					"$SYS.REQ.SERVER.PING.STATSZ", "$SYS.REQ.SERVER.PING.JSZ", "$SYS.REQ.SERVER.*.JSZ",
					"$JS.API.ACCOUNT.STREAM.MOVE.*.*", "$JS.API.ACCOUNT.STREAM.CANCEL_MOVE.*.*",
					"acc.*.$JS.API.STREAM.LEADER.STEPDOWN.*", "acc.*.$JS.API.CONSUMER.LEADER.STEPDOWN.*.*",
				},
				subAllow: []string{"_INBOX.>"},
			},
		},
		{
			name: "cluster controller",
			user: jwtplane.User{Preset: jwtplane.PresetClusterController, SystemAccount: true},
			want: want{
				pubAllow: []string{
					"$SYS.REQ.SERVER.PING.STATSZ", "$SYS.REQ.SERVER.PING.JSZ", "$SYS.REQ.SERVER.PING.GATEWAYZ", "$SYS.REQ.SERVER.PING.LEAFZ",
					"$SYS.REQ.SERVER.*.STATSZ", "$SYS.REQ.SERVER.*.JSZ", "$SYS.REQ.SERVER.*.VARZ",
					"$SYS.REQ.SERVER.*.HEALTHZ", "$SYS.REQ.SERVER.*.RELOAD",
					"$JS.API.SERVER.EVACUATE", "$JS.API.SERVER.REMOVE", "$JS.API.META.LEADER.STEPDOWN",
					"acc.*.$JS.API.STREAM.LEADER.STEPDOWN.*", "acc.*.$JS.API.CONSUMER.LEADER.STEPDOWN.*.*",
				},
				subAllow: []string{"_INBOX.>"},
			},
		},
		{
			name:    "controller preset in an ordinary account",
			user:    jwtplane.User{Preset: jwtplane.PresetClusterController},
			wantErr: jwtplane.ErrPresetAccount,
		},
		{
			name:    "readonly in the system account",
			user:    jwtplane.User{Preset: jwtplane.PresetReadonly, SystemAccount: true},
			wantErr: jwtplane.ErrPresetAccount,
		},
		{
			name:    "preset and permissions",
			user:    jwtplane.User{Preset: jwtplane.PresetReadonly, Permissions: &jwtplane.Permissions{}},
			wantErr: jwtplane.ErrPresetAndPermissions,
		},
		{
			name:    "preset and connection types",
			user:    jwtplane.User{Preset: jwtplane.PresetLeafnode, AllowedConnectionTypes: []string{jwt.ConnectionTypeLeafnodeWS}},
			wantErr: jwtplane.ErrConnectionType,
		},
		{
			name:   "unknown preset",
			user:   jwtplane.User{Preset: "superuser"},
			anyErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := tt.user
			u.Name, u.PublicKey = "u", user
			tok, err := jwtplane.SignUser(u, acc)
			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
				return
			case tt.anyErr:
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			c, err := jwt.DecodeUserClaims(tok)
			require.NoError(t, err)
			require.Equal(t, user, c.Subject)
			require.Equal(t, pub(t, acc.Signing[0].Pair), c.Issuer)
			require.Equal(t, pub(t, acc.Identity), c.IssuerAccount)
			require.Zero(t, c.Expires)
			require.ElementsMatch(t, tt.want.pubAllow, c.Pub.Allow)
			require.ElementsMatch(t, tt.want.pubDeny, c.Pub.Deny)
			require.ElementsMatch(t, tt.want.subAllow, c.Sub.Allow)
			require.ElementsMatch(t, tt.want.subDeny, c.Sub.Deny)
			require.ElementsMatch(t, tt.want.connTypes, c.AllowedConnectionTypes)
		})
	}
}

func TestSignUserKeys(t *testing.T) {
	acc := newKeys(t, nkeys.PrefixByteAccount, "s")
	_, err := jwtplane.SignUser(jwtplane.User{PublicKey: pub(t, acc.Identity)}, acc)
	require.ErrorIs(t, err, jwtplane.ErrWrongKeyType)

	_, err = jwtplane.SignUser(jwtplane.User{PublicKey: pub(t, newPair(t, nkeys.PrefixByteUser))}, jwtplane.Keys{Identity: acc.Identity})
	require.ErrorIs(t, err, jwtplane.ErrNoActiveSigningKey)

	mismatched := acc
	mismatched.PublicKey = pub(t, newPair(t, nkeys.PrefixByteAccount))
	_, err = jwtplane.SignUser(jwtplane.User{PublicKey: pub(t, newPair(t, nkeys.PrefixByteUser))}, mismatched)
	require.ErrorIs(t, err, jwtplane.ErrIdentityConflict)
}

func TestUserPresetGrant(t *testing.T) {
	acc := newKeys(t, nkeys.PrefixByteAccount, "s")
	for _, preset := range jwtplane.UserPresets() {
		t.Run(string(preset), func(t *testing.T) {
			g, ok := jwtplane.UserPresetGrant(preset)
			require.True(t, ok)
			tok, err := jwtplane.SignUser(jwtplane.User{
				Name: "u", PublicKey: pub(t, newPair(t, nkeys.PrefixByteUser)), SystemAccount: g.SystemAccount, Preset: preset,
			}, acc)
			require.NoError(t, err)
			c, err := jwt.DecodeUserClaims(tok)
			require.NoError(t, err)
			require.ElementsMatch(t, g.Publish, c.Pub.Allow)
			require.ElementsMatch(t, g.Subscribe, c.Sub.Allow)
			require.ElementsMatch(t, g.ConnectionTypes, c.AllowedConnectionTypes)
			require.Empty(t, c.Pub.Deny)
			require.Empty(t, c.Sub.Deny)
		})
	}
	_, ok := jwtplane.UserPresetGrant("unknown")
	require.False(t, ok)
}
