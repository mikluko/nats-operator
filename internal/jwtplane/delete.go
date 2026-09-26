package jwtplane

import (
	"fmt"
	"slices"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// SignDelete returns the request that deletes accounts from a full
// resolver with deletes allowed: a generic JWT listing them, self-signed by
// the operator's active signing key.
func SignDelete(operator Keys, accounts []string) (string, error) {
	signer, err := operator.signer(nkeys.PrefixByteOperator)
	if err != nil {
		return "", err
	}
	pub, err := signer.PublicKey()
	if err != nil {
		return "", err
	}
	list := slices.Sorted(slices.Values(accounts))
	for _, acc := range list {
		if !nkeys.IsValidPublicAccountKey(acc) {
			return "", fmt.Errorf("%w: %q is not an account public key", ErrWrongKeyType, acc)
		}
	}
	c := jwt.NewGenericClaims(pub)
	c.Data["accounts"] = list
	return c.Encode(signer)
}
