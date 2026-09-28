// Package natsconn dials NATS for the controllers: through a NatsConnection,
// or from servers and a credentials reference such as a NatsCluster's
// auth.systemCredentials.
package natsconn

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// Default Secret keys, applied where a selector leaves Key empty.
const (
	DefaultCAKey          = "ca.crt"
	DefaultCredentialsKey = "user.creds"
)

// Errors a resolve or a dial wraps; none of them is cured by retrying
// until the Secret or the spec changes.
var (
	ErrSecretNotFound     = errors.New("secret not found")
	ErrKeyNotFound        = errors.New("key not found in secret")
	ErrInvalidCA          = errors.New("no PEM certificate in CA bundle")
	ErrInvalidCredentials = errors.New("invalid NATS creds")
)

// Endpoint is everything a dial needs, every Secret already read. A nil CA
// verifies servers against the system roots; nil Creds dials without
// credentials.
type Endpoint struct {
	Servers []string
	CA      []byte
	Creds   []byte
}

// ReadEndpoint resolves spec against the Secrets in namespace, the
// namespace of the NatsConnection that holds spec.
func ReadEndpoint(ctx context.Context, r client.Reader, namespace string, spec *natsv1beta1.NatsConnectionSpec) (Endpoint, error) {
	ep := Endpoint{Servers: spec.Servers}
	if spec.TLS != nil {
		ca, err := ReadCA(ctx, r, namespace, spec.TLS.CA)
		if err != nil {
			return Endpoint{}, err
		}
		ep.CA = ca
	}
	creds, err := ReadCredentials(ctx, r, namespace, spec.Credentials)
	if err != nil {
		return Endpoint{}, err
	}
	ep.Creds = creds
	return ep, nil
}

// ReadCredentials returns the creds file c selects from a Secret in
// namespace, or nil when c is nil.
func ReadCredentials(ctx context.Context, r client.Reader, namespace string, c *natsv1beta1.Credentials) ([]byte, error) {
	if c == nil {
		return nil, nil
	}
	return readKey(ctx, r, namespace, c.SecretKeyRef.Name, c.SecretKeyRef.Key, DefaultCredentialsKey)
}

// ReadCA returns the PEM bundle c selects from a Secret in namespace, or
// nil when c is nil.
func ReadCA(ctx context.Context, r client.Reader, namespace string, c *natsv1beta1.CA) ([]byte, error) {
	if c == nil {
		return nil, nil
	}
	return readKey(ctx, r, namespace, c.SecretKeyRef.Name, c.SecretKeyRef.Key, DefaultCAKey)
}

func readKey(ctx context.Context, r client.Reader, namespace, name, key, defaultKey string) ([]byte, error) {
	if key == "" {
		key = defaultKey
	}
	var s corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &s); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s/%s", ErrSecretNotFound, namespace, name)
		}
		return nil, fmt.Errorf("get secret %s/%s: %w", namespace, name, err)
	}
	v, ok := s.Data[key]
	if !ok {
		return nil, fmt.Errorf("%w: %q in %s/%s", ErrKeyNotFound, key, namespace, name)
	}
	return v, nil
}

// Dial connects to ep, reconnecting without limit once connected; the first
// connect is not retried. opts apply after the package's own, so a caller
// may override them.
func Dial(ep Endpoint, opts ...nats.Option) (*nats.Conn, error) {
	own, err := endpointOptions(ep)
	if err != nil {
		return nil, err
	}
	nc, err := nats.Connect(strings.Join(ep.Servers, ","), append(own, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("connect to %v: %w", ep.Servers, err)
	}
	return nc, nil
}

func endpointOptions(ep Endpoint) ([]nats.Option, error) {
	opts := []nats.Option{nats.MaxReconnects(-1)}
	if ep.CA != nil {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ep.CA) {
			return nil, ErrInvalidCA
		}
		opts = append(opts, nats.ClientTLSConfig(nil, func() (*x509.CertPool, error) { return pool, nil }))
	}
	if ep.Creds != nil {
		if err := validateCreds(ep.Creds); err != nil {
			return nil, err
		}
		opts = append(opts, nats.UserCredentialBytes(ep.Creds))
	}
	return opts, nil
}

// CredentialsAccount returns the public key of the account whose user creds
// carries; its error wraps ErrInvalidCredentials.
func CredentialsAccount(creds []byte) (string, error) {
	if creds == nil {
		return "", fmt.Errorf("%w: no credentials", ErrInvalidCredentials)
	}
	token, err := nkeys.ParseDecoratedJWT(creds)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidCredentials, err)
	}
	c, err := jwt.DecodeUserClaims(token)
	if err != nil {
		return "", fmt.Errorf("%w: user JWT: %w", ErrInvalidCredentials, err)
	}
	if c.IssuerAccount != "" {
		return c.IssuerAccount, nil
	}
	return c.Issuer, nil
}

// validateCreds returns ErrInvalidCredentials unless creds carries a user JWT
// and a seed.
func validateCreds(creds []byte) error {
	token, err := nkeys.ParseDecoratedJWT(creds)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCredentials, err)
	}
	if _, err := jwt.DecodeUserClaims(token); err != nil {
		return fmt.Errorf("%w: user JWT: %w", ErrInvalidCredentials, err)
	}
	kp, err := nkeys.ParseDecoratedNKey(creds)
	if err != nil {
		return fmt.Errorf("%w: seed: %w", ErrInvalidCredentials, err)
	}
	kp.Wipe()
	return nil
}
