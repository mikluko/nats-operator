package auth

import (
	"context"
	"fmt"

	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// Setup registers the field indexes and every reconciler of this package
// with mgr. d receives newly signed account JWTs and deletes; nil pushes
// nothing. A d that is also a RosterNotifier has accounts reconciled
// whenever their servers change. s closes deleted users' connections; nil
// reaches no NATS server. rec records the reconcilers' events; nil records
// none.
func Setup(ctx context.Context, mgr ctrl.Manager, d Distributor, s Sessions, rec events.EventRecorder) error {
	if err := indexes(ctx, mgr.GetFieldIndexer()); err != nil {
		return fmt.Errorf("register indexes: %w", err)
	}
	var accounts, systemAccounts <-chan event.GenericEvent
	if n, ok := d.(RosterNotifier); ok {
		accounts, systemAccounts = n.Subscribe(), n.Subscribe()
	}
	c := mgr.GetClient()
	for _, r := range []interface{ SetupWithManager(ctrl.Manager) error }{
		&OperatorReconciler{Client: c, Distributor: d, Recorder: rec},
		&SystemAccountReconciler{Client: c, Distributor: d, RosterChanges: systemAccounts},
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
