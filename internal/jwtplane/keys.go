package jwtplane

import (
	"errors"
	"fmt"

	"github.com/nats-io/nkeys"
)

var (
	// ErrWrongKeyType is returned when a seed or public key is not of the
	// kind its role requires.
	ErrWrongKeyType = errors.New("wrong key type")

	// ErrNoActiveSigningKey is returned when every signing key is retiring,
	// or none is given.
	ErrNoActiveSigningKey = errors.New("no signing key that is not retiring")

	// ErrIdentityConflict is returned when Keys carries both an identity
	// key pair and a different public key.
	ErrIdentityConflict = errors.New("identity key pair and public key disagree")
)

// GenerateSeed returns a new seed for an operator, account or user key.
func GenerateSeed(kind nkeys.PrefixByte) ([]byte, error) {
	if !validKind(kind) {
		return nil, fmt.Errorf("%w: cannot generate %s keys", ErrWrongKeyType, kind)
	}
	kp, err := nkeys.CreatePair(kind)
	if err != nil {
		return nil, err
	}
	return kp.Seed()
}

// ParseSeed adopts an existing seed, refusing one that is not of kind.
func ParseSeed(seed []byte, kind nkeys.PrefixByte) (nkeys.KeyPair, error) {
	prefix, _, err := nkeys.DecodeSeed(seed)
	if err != nil {
		return nil, fmt.Errorf("decode seed: %w", err)
	}
	if prefix != kind {
		return nil, fmt.Errorf("%w: seed is %s, want %s", ErrWrongKeyType, prefix, kind)
	}
	return nkeys.FromSeed(seed)
}

func validKind(kind nkeys.PrefixByte) bool {
	switch kind {
	case nkeys.PrefixByteOperator, nkeys.PrefixByteAccount, nkeys.PrefixByteUser:
		return true
	}
	return false
}

// SigningKey is one signing key of an operator or account.
type SigningKey struct {
	Name string
	Pair nkeys.KeyPair
	// Retiring keeps the key listed in the JWT, so what it signed stays
	// valid, while nothing new is signed with it.
	Retiring bool
}

// Keys are the keys of an operator or account. Identity is nil when the
// identity is held offline; PublicKey then names it.
type Keys struct {
	Identity  nkeys.KeyPair
	PublicKey string
	Signing   []SigningKey
}

// publicKey returns the identity public key, checking it is of kind.
func (k Keys) publicKey(kind nkeys.PrefixByte) (string, error) {
	pub := k.PublicKey
	if k.Identity != nil {
		idPub, err := k.Identity.PublicKey()
		if err != nil {
			return "", err
		}
		if pub != "" && pub != idPub {
			return "", fmt.Errorf("%w: %s and %s", ErrIdentityConflict, idPub, pub)
		}
		pub = idPub
	}
	if !isPublicKey(pub, kind) {
		return "", fmt.Errorf("%w: identity %q is not a %s public key", ErrWrongKeyType, pub, kind)
	}
	return pub, nil
}

// signer returns the first signing key that is not retiring.
func (k Keys) signer(kind nkeys.PrefixByte) (nkeys.KeyPair, error) {
	for _, sk := range k.Signing {
		if sk.Retiring {
			continue
		}
		pub, err := sk.Pair.PublicKey()
		if err != nil {
			return nil, err
		}
		if !isPublicKey(pub, kind) {
			return nil, fmt.Errorf("%w: signing key %q is not a %s key", ErrWrongKeyType, sk.Name, kind)
		}
		return sk.Pair, nil
	}
	return nil, ErrNoActiveSigningKey
}

// signingPublicKeys lists every signing key, retiring ones included.
func (k Keys) signingPublicKeys(kind nkeys.PrefixByte) ([]string, error) {
	pubs := make([]string, 0, len(k.Signing))
	for _, sk := range k.Signing {
		pub, err := sk.Pair.PublicKey()
		if err != nil {
			return nil, err
		}
		if !isPublicKey(pub, kind) {
			return nil, fmt.Errorf("%w: signing key %q is not a %s key", ErrWrongKeyType, sk.Name, kind)
		}
		pubs = append(pubs, pub)
	}
	return pubs, nil
}

func isPublicKey(pub string, kind nkeys.PrefixByte) bool {
	switch kind {
	case nkeys.PrefixByteOperator:
		return nkeys.IsValidPublicOperatorKey(pub)
	case nkeys.PrefixByteAccount:
		return nkeys.IsValidPublicAccountKey(pub)
	case nkeys.PrefixByteUser:
		return nkeys.IsValidPublicUserKey(pub)
	}
	return false
}
