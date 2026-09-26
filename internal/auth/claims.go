package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
)

// JWTHash returns the hash a status's jwtHash carries for token.
func JWTHash(token string) string {
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// offlineOperatorSubject returns the identity an operator JWT names.
func offlineOperatorSubject(token string) (string, error) {
	c, err := jwt.DecodeOperatorClaims(token)
	if err != nil {
		return "", fmt.Errorf("decode operator JWT: %w", err)
	}
	return c.Subject, nil
}

// issuer returns the key that signed an account JWT, or "" for one that
// does not decode.
func issuer(accountJWT string) string {
	c, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		return ""
	}
	return c.Issuer
}

// normalizeClaimsData clears what differs between two signings of the same
// claims: the issue time, the ID, and the expiry, of which only whether
// there is one is kept.
func normalizeClaimsData(c *jwt.ClaimsData) {
	if c.Expires != 0 {
		c.Expires = 1
	}
	c.IssuedAt = 0
	c.ID = ""
}

// lifetimeTolerance absorbs the second an expiry and an issue time, taken
// apart, may straddle.
const lifetimeTolerance = 2 * time.Second

// lifetimeDiffers reports whether an account JWT's lifetime is further than
// lifetimeTolerance from ttl. A JWT that never expires, or does not decode,
// differs from every ttl.
func lifetimeDiffers(accountJWT string, ttl time.Duration) bool {
	c, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil || c.Expires == 0 {
		return true
	}
	d := time.Duration(c.Expires-c.IssuedAt)*time.Second - ttl
	return d > lifetimeTolerance || d < -lifetimeTolerance
}

// sameOperatorClaims reports whether two operator JWTs carry the same
// claims, signed by the same key, whenever they were signed. A JWT that does
// not decode is never the same.
func sameOperatorClaims(a, b string) bool {
	return sameClaims(a, b, func(token string) (any, error) {
		c, err := jwt.DecodeOperatorClaims(token)
		if err != nil {
			return nil, err
		}
		normalizeClaimsData(&c.ClaimsData)
		return c, nil
	})
}

// sameAccountClaims reports whether two account JWTs carry the same claims,
// signed by the same key, whenever they were signed and however long they
// live, so long as both expire or neither does. The activation tokens of
// imports compare the same way. A JWT that does not decode is never the
// same.
func sameAccountClaims(a, b string) bool {
	return sameClaims(a, b, func(token string) (any, error) {
		c, err := jwt.DecodeAccountClaims(token)
		if err != nil {
			return nil, err
		}
		normalizeClaimsData(&c.ClaimsData)
		for _, imp := range c.Imports {
			if imp.Token == "" {
				continue
			}
			ac, err := jwt.DecodeActivationClaims(imp.Token)
			if err != nil {
				return nil, err
			}
			normalizeClaimsData(&ac.ClaimsData)
			raw, err := json.Marshal(ac)
			if err != nil {
				return nil, err
			}
			imp.Token = string(raw)
		}
		return c, nil
	})
}

func sameClaims(a, b string, normalize func(string) (any, error)) bool {
	if a == "" || b == "" {
		return false
	}
	na, err := normalize(a)
	if err != nil {
		return false
	}
	nb, err := normalize(b)
	if err != nil {
		return false
	}
	ra, err := json.Marshal(na)
	if err != nil {
		return false
	}
	rb, err := json.Marshal(nb)
	if err != nil {
		return false
	}
	return bytes.Equal(ra, rb)
}
