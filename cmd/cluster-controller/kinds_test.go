package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"

	"sarak.sh/mikluko/nats-operator/internal/manager"
	"sarak.sh/mikluko/nats-operator/internal/manager/managertest"
	"sarak.sh/mikluko/nats-operator/internal/telemetry"
)

// TestReconciledKinds pins telemetry.ClusterKinds, whose conditions the
// condition gauge reports, to the reconcilers setup registers.
func TestReconciledKinds(t *testing.T) {
	scheme, err := manager.NewScheme(schemes...)
	require.NoError(t, err)
	got := managertest.ReconciledKinds(t, scheme, func(ctx context.Context, mgr ctrl.Manager) error {
		return setup(ctx, mgr, false)
	})
	var want []string
	for _, k := range telemetry.ClusterKinds {
		want = append(want, k.Name)
	}
	require.ElementsMatch(t, want, got)
}
