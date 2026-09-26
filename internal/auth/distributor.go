package auth

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
)

// Distributor pushes account JWTs to the resolvers of the NATS clusters
// that trust a NATS operator.
//
// Push is called with every account JWT the reconcilers newly sign, the
// system account's included, before the JWT is written to status; an error
// requeues the account, which is then signed and pushed afresh. It is called
// concurrently from several reconcilers.
type Distributor interface {
	Push(ctx context.Context, operator types.NamespacedName, accountJWT string) error
}

// push hands token to d, which may be nil.
func push(ctx context.Context, d Distributor, operator types.NamespacedName, token string) error {
	if d == nil {
		return nil
	}
	return d.Push(ctx, operator, token)
}
