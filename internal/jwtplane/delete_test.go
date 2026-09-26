package jwtplane_test

import (
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// TestSignDelete pins the request nats-server's handleDeleteRequest
// accepts: self-signed by an operator key, accounts listed under
// "accounts".
func TestSignDelete(t *testing.T) {
	keys := newKeys(t, nkeys.PrefixByteOperator, "active", "old")
	a, b := pub(t, newPair(t, nkeys.PrefixByteAccount)), pub(t, newPair(t, nkeys.PrefixByteAccount))

	tests := []struct {
		name     string
		keys     jwtplane.Keys
		accounts []string
		wantErr  error
	}{
		{name: "signed by the active signing key", keys: keys, accounts: []string{b, a}},
		{name: "every signing key retiring", keys: jwtplane.Keys{Identity: keys.Identity, Signing: keys.Signing[1:]}, accounts: []string{a}, wantErr: jwtplane.ErrNoActiveSigningKey},
		{name: "not an account key", keys: keys, accounts: []string{pub(t, keys.Identity)}, wantErr: jwtplane.ErrWrongKeyType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tok, err := jwtplane.SignDelete(tt.keys, tt.accounts)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			c, err := jwt.DecodeGeneric(tok)
			require.NoError(t, err)
			signer := pub(t, keys.Signing[0].Pair)
			require.Equal(t, signer, c.Issuer)
			require.Equal(t, signer, c.Subject, "self-signed")
			require.Equal(t, []any{min(a, b), max(a, b)}, c.Data["accounts"], "sorted")
		})
	}
}
