package authctl

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// PollOperator polls operator's roster once.
func (r *Resolvers) PollOperator(ctx context.Context, operator types.NamespacedName) error {
	return r.pollOperator(ctx, operator)
}

const RosterMisses = rosterMisses

// IndexFake registers the package's field indexes on b.
func IndexFake(ctx context.Context, b *fake.ClientBuilder) error {
	return indexes(ctx, builderIndexer{b})
}
