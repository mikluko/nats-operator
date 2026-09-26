package jwtplane_test

import (
	"testing"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// newKeys returns an identity key and one signing key per name, the first
// active and the rest retiring.
func newKeys(t *testing.T, kind nkeys.PrefixByte, signing ...string) jwtplane.Keys {
	t.Helper()
	k := jwtplane.Keys{Identity: newPair(t, kind)}
	for i, name := range signing {
		k.Signing = append(k.Signing, jwtplane.SigningKey{Name: name, Pair: newPair(t, kind), Retiring: i > 0})
	}
	return k
}

func newPair(t *testing.T, kind nkeys.PrefixByte) nkeys.KeyPair {
	t.Helper()
	seed, err := jwtplane.GenerateSeed(kind)
	require.NoError(t, err)
	kp, err := jwtplane.ParseSeed(seed, kind)
	require.NoError(t, err)
	return kp
}

func pub(t *testing.T, kp nkeys.KeyPair) string {
	t.Helper()
	p, err := kp.PublicKey()
	require.NoError(t, err)
	return p
}

// offline returns k with its identity reduced to its public key.
func offline(t *testing.T, k jwtplane.Keys) jwtplane.Keys {
	t.Helper()
	k.PublicKey = pub(t, k.Identity)
	k.Identity = nil
	return k
}

func TestGenerateSeed(t *testing.T) {
	tests := []struct {
		kind    nkeys.PrefixByte
		wantErr bool
	}{
		{nkeys.PrefixByteOperator, false},
		{nkeys.PrefixByteAccount, false},
		{nkeys.PrefixByteUser, false},
		{nkeys.PrefixByteServer, true},
		{nkeys.PrefixByteCluster, true},
	}
	for _, tt := range tests {
		t.Run(tt.kind.String(), func(t *testing.T) {
			seed, err := jwtplane.GenerateSeed(tt.kind)
			if tt.wantErr {
				require.ErrorIs(t, err, jwtplane.ErrWrongKeyType)
				return
			}
			require.NoError(t, err)
			prefix, _, err := nkeys.DecodeSeed(seed)
			require.NoError(t, err)
			require.Equal(t, tt.kind, prefix)
		})
	}
}

func TestParseSeed(t *testing.T) {
	account, err := jwtplane.GenerateSeed(nkeys.PrefixByteAccount)
	require.NoError(t, err)
	tests := []struct {
		name    string
		seed    []byte
		kind    nkeys.PrefixByte
		wantErr error
		anyErr  bool
	}{
		{name: "matching kind", seed: account, kind: nkeys.PrefixByteAccount},
		{name: "account seed as operator", seed: account, kind: nkeys.PrefixByteOperator, wantErr: jwtplane.ErrWrongKeyType},
		{name: "account seed as user", seed: account, kind: nkeys.PrefixByteUser, wantErr: jwtplane.ErrWrongKeyType},
		{name: "garbage", seed: []byte("SAnotaseed"), kind: nkeys.PrefixByteAccount, anyErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kp, err := jwtplane.ParseSeed(tt.seed, tt.kind)
			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case tt.anyErr:
				require.Error(t, err)
			default:
				require.NoError(t, err)
				want, err := nkeys.FromSeed(tt.seed)
				require.NoError(t, err)
				require.Equal(t, pub(t, want), pub(t, kp))
			}
		})
	}
}
