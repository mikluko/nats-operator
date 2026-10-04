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

	// ErrNoActiveSigningKey is returned when every signing key is retiring
	// or scoped, or none is given.
	ErrNoActiveSigningKey = errors.New("no signing key that is neither retiring nor scoped")

	// ErrNoScopedSigningKey is returned when no signing key that is not
	// retiring has a scope of the role asked for.
	ErrNoScopedSigningKey = errors.New("no scoped signing key that is not retiring has the role")

	// ErrScopedOperatorKey is returned for a NATS operator's signing key
	// that carries a scope.
	ErrScopedOperatorKey = errors.New("a NATS operator's signing key takes no scope")

	// ErrIdentityConflict is returned when Keys carries both an identity
	// key pair and a different public key.
	ErrIdentityConflict = errors.New("identity key pair and public key disagree")
)

// GenerateSeed returns a new seed for a NATS operator, account or user key.
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

// SigningKey is one signing key of a NATS operator or account.
type SigningKey struct {
	Name string
	Pair nkeys.KeyPair
	// Retiring keeps the key listed in the JWT, so what it signed stays
	// valid, while nothing new is signed with it.
	Retiring bool
	// Scope makes the key a scoped signing key of an account; a NATS
	// operator's key carrying one is refused with ErrScopedOperatorKey.
	Scope *UserScope
}

// UserScope is what the servers hold every user signed by a scoped signing
// key to.
type UserScope struct {
	Role        string
	Permissions *Permissions
	// AllowedConnectionTypes are jwt.ConnectionType* values; empty allows any.
	AllowedConnectionTypes []string
	// Subscriptions and Payload limit each user; zero is unlimited.
	Subscriptions int64
	Payload       int64
}

// Keys are the keys of a NATS operator or account. Identity is nil when the
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

// signer returns the first signing key that is neither retiring nor scoped.
func (k Keys) signer(kind nkeys.PrefixByte) (nkeys.KeyPair, error) {
	return k.firstSigner(kind, func(sk SigningKey) bool { return sk.Scope == nil }, ErrNoActiveSigningKey)
}

// scopedSigner returns the first signing key of an account that is not
// retiring and whose scope has role.
func (k Keys) scopedSigner(role string) (nkeys.KeyPair, error) {
	return k.firstSigner(nkeys.PrefixByteAccount,
		func(sk SigningKey) bool { return sk.Scope != nil && sk.Scope.Role == role },
		fmt.Errorf("%w: %q", ErrNoScopedSigningKey, role))
}

func (k Keys) firstSigner(kind nkeys.PrefixByte, match func(SigningKey) bool, none error) (nkeys.KeyPair, error) {
	for _, sk := range k.Signing {
		if sk.Retiring || !match(sk) {
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
	return nil, none
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
		if sk.Scope != nil && kind != nkeys.PrefixByteAccount {
			return nil, fmt.Errorf("%w: signing key %q", ErrScopedOperatorKey, sk.Name)
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
