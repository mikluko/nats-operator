package api_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// TestGroupVersions pins the four groups and their version to what the
// design and the story manifests name.
func TestGroupVersions(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"nats", natsv1beta1.GroupVersion.String(), "nats.mikluko.io/v1beta1"},
		{"cluster", clusterv1beta1.GroupVersion.String(), "cluster.nats.mikluko.io/v1beta1"},
		{"auth", authv1beta1.GroupVersion.String(), "auth.nats.mikluko.io/v1beta1"},
		{"jetstream", jetstreamv1beta1.GroupVersion.String(), "jetstream.nats.mikluko.io/v1beta1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.got)
		})
	}
}
