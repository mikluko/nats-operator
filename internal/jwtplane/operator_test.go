package jwtplane_test

import (
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

func TestSignOperator(t *testing.T) {
	keys := newKeys(t, nkeys.PrefixByteOperator, "active", "old")
	sys := pub(t, newPair(t, nkeys.PrefixByteAccount))

	tok, err := jwtplane.SignOperator(jwtplane.Operator{Name: "demo", Keys: keys, SystemAccount: sys})
	require.NoError(t, err)
	c, err := jwt.DecodeOperatorClaims(tok)
	require.NoError(t, err)
	require.Equal(t, pub(t, keys.Identity), c.Subject)
	require.Equal(t, c.Subject, c.Issuer, "signed by its identity")
	require.Equal(t, sys, c.SystemAccount)
	require.ElementsMatch(t, []string{pub(t, keys.Signing[0].Pair), pub(t, keys.Signing[1].Pair)}, []string(c.SigningKeys), "retiring keys stay listed")
	require.Zero(t, c.Expires)
	require.False(t, c.StrictSigningKeyUsage)

	offlineKeys := offline(t, keys)

	tests := []struct {
		name    string
		op      jwtplane.Operator
		want    string
		wantErr error
	}{
		{
			name: "offline JWT that lists the active signing key is returned unchanged",
			op:   jwtplane.Operator{Keys: offlineKeys, SystemAccount: sys, JWT: tok},
			want: tok,
		},
		{
			name: "offline JWT missing the active signing key",
			op: jwtplane.Operator{
				Keys:          jwtplane.Keys{PublicKey: offlineKeys.PublicKey, Signing: []jwtplane.SigningKey{{Name: "new", Pair: newPair(t, nkeys.PrefixByteOperator)}}},
				SystemAccount: sys, JWT: tok,
			},
			wantErr: jwtplane.ErrOfflineOperatorMismatch,
		},
		{
			name:    "offline JWT naming another system account",
			op:      jwtplane.Operator{Keys: offlineKeys, SystemAccount: pub(t, newPair(t, nkeys.PrefixByteAccount)), JWT: tok},
			wantErr: jwtplane.ErrOfflineOperatorMismatch,
		},
		{
			name: "offline JWT of another identity",
			op: jwtplane.Operator{
				Keys:          jwtplane.Keys{PublicKey: pub(t, newPair(t, nkeys.PrefixByteOperator)), Signing: keys.Signing},
				SystemAccount: sys, JWT: tok,
			},
			wantErr: jwtplane.ErrOfflineOperatorMismatch,
		},
		{
			name:    "offline JWT not signed by its identity",
			op:      jwtplane.Operator{Keys: offlineKeys, SystemAccount: sys, JWT: signedBy(t, keys, keys.Signing[0].Pair, sys)},
			wantErr: jwtplane.ErrOfflineOperatorMismatch,
		},
		{
			name:    "identity key beside offline JWT",
			op:      jwtplane.Operator{Keys: keys, SystemAccount: sys, JWT: tok},
			wantErr: jwtplane.ErrIdentityConflict,
		},
		{
			name:    "no signing key",
			op:      jwtplane.Operator{Keys: jwtplane.Keys{Identity: keys.Identity}, SystemAccount: sys},
			wantErr: jwtplane.ErrNoActiveSigningKey,
		},
		{
			name: "only retiring signing keys",
			op: jwtplane.Operator{
				Keys:          jwtplane.Keys{Identity: keys.Identity, Signing: []jwtplane.SigningKey{{Name: "old", Pair: keys.Signing[0].Pair, Retiring: true}}},
				SystemAccount: sys,
			},
			wantErr: jwtplane.ErrNoActiveSigningKey,
		},
		{
			name:    "system account not an account key",
			op:      jwtplane.Operator{Keys: keys, SystemAccount: pub(t, keys.Identity)},
			wantErr: jwtplane.ErrWrongKeyType,
		},
		{
			name: "account key as signing key",
			op: jwtplane.Operator{
				Keys:          jwtplane.Keys{Identity: keys.Identity, Signing: []jwtplane.SigningKey{{Name: "acc", Pair: newPair(t, nkeys.PrefixByteAccount)}}},
				SystemAccount: sys,
			},
			wantErr: jwtplane.ErrWrongKeyType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := jwtplane.SignOperator(tt.op)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// signedBy returns an operator JWT for keys' identity signed by signer.
func signedBy(t *testing.T, keys jwtplane.Keys, signer nkeys.KeyPair, sys string) string {
	t.Helper()
	c := jwt.NewOperatorClaims(pub(t, keys.Identity))
	c.SystemAccount = sys
	c.SigningKeys.Add(pub(t, keys.Signing[0].Pair))
	tok, err := c.Encode(signer)
	require.NoError(t, err)
	return tok
}
