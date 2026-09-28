package authctl

import (
	"context"
	"errors"

	"k8s.io/apimachinery/pkg/types"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
)

var (
	// ErrUnreachable is wrapped by a Distributor's errors when no server
	// trusting the NATS operator could be asked: no system connection, or no
	// server answering on it.
	ErrUnreachable = errors.New("no server reachable")

	// ErrStaleJWT is returned by Push for a JWT issued before one already
	// pushed, or held by a server, for the same account.
	ErrStaleJWT = errors.New("a newer JWT for the account exists")
)

// Distributor carries account JWTs, and deletes of accounts, to the
// resolvers of the NATS clusters that trust a NATS operator. Its methods
// are called concurrently from several reconcilers.
type Distributor interface {
	// Push sends accountJWT to every server trusting operator, unless a JWT
	// issued after it was already pushed or is held for the same account.
	Push(ctx context.Context, operator types.NamespacedName, accountJWT string) error

	// Current returns how many servers trust operator, how many of them
	// hold accountJWT, and when its account was last pushed, if it was.
	Current(ctx context.Context, operator types.NamespacedName, accountJWT string) (authv1beta1.Distribution, error)

	// Lookup returns the newest JWT for account that a server trusting
	// operator holds, or "" when every one of them answers that it holds
	// none. The error wraps ErrUnreachable unless every one of them
	// answered.
	Lookup(ctx context.Context, operator types.NamespacedName, account string) (string, error)

	// Delete makes request, from jwtplane.SignDelete, the delete that every
	// server trusting operator is sent now and whenever it joins; "" sends
	// none.
	Delete(ctx context.Context, operator types.NamespacedName, request string) error
}

// push hands token to d, which may be nil.
func push(ctx context.Context, d Distributor, operator types.NamespacedName, token string) error {
	if d == nil {
		return nil
	}
	return d.Push(ctx, operator, token)
}
