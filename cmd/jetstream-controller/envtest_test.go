package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/manager/managertest"
)

// TestEnvtestReadyUnderRoles pins config/rbac/jetstream-controller as enough
// for the JetStream controller to become ready in namespace-scoped mode.
func TestEnvtestReadyUnderRoles(t *testing.T) {
	scheme, err := manager.NewScheme(schemes...)
	require.NoError(t, err)
	withResync := func(ctx context.Context, mgr ctrl.Manager) error {
		return setup(ctx, mgr, lifecycle.DefaultResync)
	}
	managertest.ReadyUnderRoles(t, scheme, manager.Owned{}, withResync, "../../config/rbac/jetstream-controller", "../../config/crd")
}
