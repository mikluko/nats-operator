package auth

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
)

// Setup registers the field indexes and every reconciler of this package
// with mgr. d receives newly signed account JWTs; nil pushes nothing.
func Setup(ctx context.Context, mgr ctrl.Manager, d Distributor) error {
	if err := indexes(ctx, mgr.GetFieldIndexer()); err != nil {
		return fmt.Errorf("register indexes: %w", err)
	}
	c := mgr.GetClient()
	for _, r := range []interface{ SetupWithManager(ctrl.Manager) error }{
		&OperatorReconciler{Client: c, Distributor: d},
		&SystemAccountReconciler{Client: c},
		&AccountReconciler{Client: c, Distributor: d},
		&OperatorTrustReconciler{Client: c},
		&AccountTrustReconciler{Client: c},
	} {
		if err := r.SetupWithManager(mgr); err != nil {
			return err
		}
	}
	return nil
}
