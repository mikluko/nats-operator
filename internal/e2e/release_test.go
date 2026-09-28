package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/authctl"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natscluster"
)

// TestControllerFinalizers pins the runner's finalizer names to the ones the
// controllers add.
func TestControllerFinalizers(t *testing.T) {
	byKind := map[string]string{}
	for _, h := range controllerFinalizers {
		byKind[h.gvk.Kind] = h.finalizer
	}
	require.Equal(t, map[string]string{
		"NatsUser":              authctl.UserFinalizer,
		"NatsAccount":           authctl.AccountFinalizer,
		"NatsConsumer":          lifecycle.Finalizer,
		"NatsStream":            lifecycle.Finalizer,
		"NatsKeyValue":          lifecycle.Finalizer,
		"NatsObjectStore":       lifecycle.Finalizer,
		"NatsClusterEvacuation": lifecycle.Finalizer,
		"NatsCluster":           natscluster.FinalizerJetStreamData,
	}, byKind)
}
