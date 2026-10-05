package authctl

import (
	"context"
	"fmt"

	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// Setup registers the field indexes and every reconciler of this package
// with mgr; d, s and rec may each be nil. Where d is a *Resolvers, the
// NatsOperator, system account and account reconcilers follow its roster
// changes. mgr's
// client reads Secrets from the API server, as manager.ClientOptions sets.
func Setup(ctx context.Context, mgr ctrl.Manager, d Distributor, s Sessions, rec events.EventRecorder) error {
	if err := indexes(ctx, mgr.GetFieldIndexer()); err != nil {
		return fmt.Errorf("register indexes: %w", err)
	}
	var operators, accounts, systemAccounts <-chan event.GenericEvent
	if r, ok := d.(*Resolvers); ok {
		operators, accounts, systemAccounts = r.Subscribe(), r.Subscribe(), r.Subscribe()
	}
	c := mgr.GetClient()
	for _, r := range []interface{ SetupWithManager(ctrl.Manager) error }{
		&OperatorReconciler{Client: c, Distributor: d, Recorder: rec, RosterChanges: operators},
		&SystemAccountReconciler{Client: c, Distributor: d, RosterChanges: systemAccounts, Recorder: rec},
		&AccountReconciler{Client: c, Distributor: d, RosterChanges: accounts, Recorder: rec},
		&OperatorTrustReconciler{Client: c},
		&AccountTrustReconciler{Client: c},
		&UserReconciler{Client: c, Sessions: s, Recorder: rec},
	} {
		if err := r.SetupWithManager(mgr); err != nil {
			return err
		}
	}
	return nil
}
