package jwtplane

import (
	"errors"
	"fmt"
	"slices"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// ErrOfflineOperatorMismatch is returned when a NATS operator JWT signed offline
// does not carry what the spec requires of it.
var ErrOfflineOperatorMismatch = errors.New("offline NATS operator JWT does not match spec")

// Operator is a NATS operator. Exactly one of Keys.Identity and JWT is set:
// JWT is a NATS operator JWT signed offline.
type Operator struct {
	Name string
	Keys Keys
	// SystemAccount is the public key of the system account.
	SystemAccount string
	JWT           string
}

// SignOperator returns the NATS operator JWT. With an identity key it signs
// one from o that lists every signing key, never expires, and leaves strict
// signing-key usage off so adopted accounts keep user JWTs their identity
// keys signed. With an offline JWT it returns that JWT unchanged, or an error
// wrapping ErrOfflineOperatorMismatch unless the JWT is self-signed, its
// subject is the spec's public key where one is set, it names SystemAccount
// and it lists every signing key not retiring.
func SignOperator(o Operator) (string, error) {
	if !nkeys.IsValidPublicAccountKey(o.SystemAccount) {
		return "", fmt.Errorf("%w: system account %q is not an account public key", ErrWrongKeyType, o.SystemAccount)
	}
	if _, err := o.Keys.signer(nkeys.PrefixByteOperator); err != nil {
		return "", err
	}
	signing, err := o.Keys.signingPublicKeys(nkeys.PrefixByteOperator)
	if err != nil {
		return "", err
	}
	if o.JWT != "" {
		if o.Keys.Identity != nil {
			return "", fmt.Errorf("%w: identity key and offline JWT are exclusive", ErrIdentityConflict)
		}
		return checkOfflineOperator(o)
	}
	if o.Keys.Identity == nil {
		return "", fmt.Errorf("%w: NATS operator has neither identity key nor JWT", ErrWrongKeyType)
	}
	pub, err := o.Keys.publicKey(nkeys.PrefixByteOperator)
	if err != nil {
		return "", err
	}
	c := jwt.NewOperatorClaims(pub)
	c.Name = o.Name
	c.SystemAccount = o.SystemAccount
	c.SigningKeys.Add(signing...)
	if err := validate(c); err != nil {
		return "", err
	}
	return c.Encode(o.Keys.Identity)
}

func checkOfflineOperator(o Operator) (string, error) {
	c, err := jwt.DecodeOperatorClaims(o.JWT)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrOfflineOperatorMismatch, err)
	}
	if c.Issuer != c.Subject {
		return "", fmt.Errorf("%w: issued by %s, not by its identity %s", ErrOfflineOperatorMismatch, c.Issuer, c.Subject)
	}
	if o.Keys.PublicKey != "" && o.Keys.PublicKey != c.Subject {
		return "", fmt.Errorf("%w: identity is %s, spec names %s", ErrOfflineOperatorMismatch, c.Subject, o.Keys.PublicKey)
	}
	if c.SystemAccount != o.SystemAccount {
		return "", fmt.Errorf("%w: names system account %q, want %q", ErrOfflineOperatorMismatch, c.SystemAccount, o.SystemAccount)
	}
	for _, sk := range o.Keys.Signing {
		if sk.Retiring {
			continue
		}
		pub, err := sk.Pair.PublicKey()
		if err != nil {
			return "", err
		}
		if !slices.Contains(c.SigningKeys, pub) {
			return "", fmt.Errorf("%w: signing key %q (%s) is not listed", ErrOfflineOperatorMismatch, sk.Name, pub)
		}
	}
	return o.JWT, nil
}

// validate runs the jwt/v2 validation Encode skips, refusing any blocking
// issue.
func validate(c interface{ Validate(*jwt.ValidationResults) }) error {
	vr := jwt.CreateValidationResults()
	c.Validate(vr)
	if vr.IsBlocking(true) {
		return fmt.Errorf("invalid claims: %w", errors.Join(vr.Errors()...))
	}
	return nil
}
