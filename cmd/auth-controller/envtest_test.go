package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/manager/managertest"
)

// TestEnvtestReadyUnderRoles pins config/rbac/auth-controller as enough for
// the auth controller, with a system connection, to become ready in
// namespace-scoped mode.
func TestEnvtestReadyUnderRoles(t *testing.T) {
	scheme, err := manager.NewScheme(schemes...)
	require.NoError(t, err)
	withSystem := func(ctx context.Context, mgr ctrl.Manager) error {
		return setup(ctx, mgr, "watched-a/system")
	}
	managertest.ReadyUnderRoles(t, scheme, manager.Owned{}, withSystem, "../../config/rbac/auth-controller", "../../config/crd")
}
