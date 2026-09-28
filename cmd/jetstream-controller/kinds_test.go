package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/manager/managertest"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// TestReconciledKinds pins telemetry.JetStreamKinds, whose conditions the
// condition gauge reports, to the reconcilers setup registers.
func TestReconciledKinds(t *testing.T) {
	scheme, err := manager.NewScheme(schemes...)
	require.NoError(t, err)
	got := managertest.ReconciledKinds(t, scheme, func(ctx context.Context, mgr ctrl.Manager) error {
		return setup(ctx, mgr, time.Minute)
	})
	var want []string
	for _, k := range telemetry.JetStreamKinds {
		want = append(want, k.Name)
	}
	require.ElementsMatch(t, want, got)
}
