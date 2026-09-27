package authctl

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// ErrForeignConnection is returned by SystemConnection.Conn when the
// connection's credentials are not those of a user of the operator's
// system account.
var ErrForeignConnection = errors.New("system connection is not a user of the operator's system account")

// SystemConnection is the auth controller's system connection: the
// NatsConnection Name, pooled in Pool. It serves a NatsOperator only if the
// connection's creds are those of a user of the system account the
// operator signed, which for the auth controller is a NatsUser holding the
// auth-controller preset.
type SystemConnection struct {
	Reader client.Reader
	Pool   *natsconn.Pool
	Name   types.NamespacedName
}

// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsconnections,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsoperators,verbs=get;list;watch

// Conn returns the connection for operator. The error wraps
// ErrOperatorGone when operator does not exist, and ErrForeignConnection
// when the creds belong to another account.
func (s *SystemConnection) Conn(ctx context.Context, operator types.NamespacedName) (*nats.Conn, error) {
	var op authv1beta1.NatsOperator
	if err := s.Reader.Get(ctx, operator, &op); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrOperatorGone, operator)
		}
		return nil, err
	}
	sysPub := signedSystemAccount(&op)
	if sysPub == "" {
		return nil, fmt.Errorf("NatsOperator %s has not signed its system account", operator)
	}
	var nc natsv1beta1.NatsConnection
	if err := s.Reader.Get(ctx, s.Name, &nc); err != nil {
		return nil, fmt.Errorf("get NatsConnection %s: %w", s.Name, err)
	}
	ep, err := natsconn.ReadEndpoint(ctx, s.Reader, nc.Namespace, &nc.Spec)
	if err != nil {
		return nil, err
	}
	account, err := credsAccount(ep.Creds)
	if err != nil {
		return nil, fmt.Errorf("NatsConnection %s: %w", s.Name, err)
	}
	if account != sysPub {
		return nil, fmt.Errorf("%w: NatsConnection %s is a user of %s, NatsOperator %s's system account is %s",
			ErrForeignConnection, s.Name, account, operator, sysPub)
	}
	return s.Pool.Get(ctx, natsconn.ConnectionKey(s.Name), ep)
}

// signedSystemAccount returns the public key of op's system account as its
// status records it, in status.systemAccount or else in the operator JWT,
// or "" where it records none.
func signedSystemAccount(op *authv1beta1.NatsOperator) string {
	if sys := op.Status.SystemAccount; sys != nil && sys.PublicKey != "" {
		return sys.PublicKey
	}
	c, err := jwt.DecodeOperatorClaims(op.Status.JWT)
	if err != nil {
		return ""
	}
	return c.SystemAccount
}

// credsAccount returns the account whose user the creds file carries.
func credsAccount(creds []byte) (string, error) {
	if creds == nil {
		return "", fmt.Errorf("%w: no credentials", natsconn.ErrInvalidCredentials)
	}
	token, err := nkeys.ParseDecoratedJWT(creds)
	if err != nil {
		return "", fmt.Errorf("%w: %w", natsconn.ErrInvalidCredentials, err)
	}
	c, err := jwt.DecodeUserClaims(token)
	if err != nil {
		return "", fmt.Errorf("%w: user JWT: %w", natsconn.ErrInvalidCredentials, err)
	}
	if c.IssuerAccount != "" {
		return c.IssuerAccount, nil
	}
	return c.Issuer, nil
}
