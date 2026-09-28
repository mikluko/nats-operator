package main

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/manager/managertest"
	"github.com/mikluko/nats-operator/internal/manager/secretreads"
)

// TestEnvtestSecretMetadata pins that setup, in the manager New builds,
// watches Secrets and never lists or watches whole Secrets.
func TestEnvtestSecretMetadata(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })

	scheme, err := manager.NewScheme(schemes...)
	require.NoError(t, err)
	recorded, reads := secretreads.Record(cfg)
	mgr, err := manager.New(recorded, &manager.Options{MetricsAddr: "0", ProbeAddr: "0"}, scheme, manager.Owned{})
	require.NoError(t, err)
	require.NoError(t, setup(t.Context(), mgr, ""))
	go func() { _ = mgr.Start(t.Context()) }()
	require.True(t, mgr.GetCache().WaitForCacheSync(t.Context()))
	reads.RequireMetadataOnly(t)
}

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
