package authctl

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
)

// PollOperator polls operator's roster once.
func (r *Resolvers) PollOperator(ctx context.Context, operator types.NamespacedName) error {
	return r.pollOperator(ctx, operator)
}

const RosterMisses = rosterMisses
