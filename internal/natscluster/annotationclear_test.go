package natscluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// TestAnnotationClear_ConflictsWithConcurrentChange pins that the
// replace-server and force-step annotations are cleared only from the
// NatsCluster they were read from: a value set since conflicts and stays.
func TestAnnotationClear_ConflictsWithConcurrentChange(t *testing.T) {
	tests := []struct {
		annotation string
		clear      func(ctx context.Context, r *Reconciler, nc *clusterv1beta1.NatsCluster) error
	}{
		{clusterv1beta1.AnnotationReplaceServer, func(ctx context.Context, r *Reconciler, nc *clusterv1beta1.NatsCluster) error {
			return r.requestReplacement(ctx, nc, &Plan{}, nil)
		}},
		{clusterv1beta1.AnnotationForceStep, func(ctx context.Context, r *Reconciler, nc *clusterv1beta1.NatsCluster) error {
			_, err := r.rollout(ctx, nc, &Plan{}, Observed{})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.annotation, func(t *testing.T) {
			stored := &clusterv1beta1.NatsCluster{ObjectMeta: metav1.ObjectMeta{
				Namespace: "nats-system", Name: "demo", Annotations: map[string]string{tt.annotation: "demo-1"},
			}}
			c := fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(stored).Build()
			r := &Reconciler{Client: c}
			key := client.ObjectKeyFromObject(stored)

			read := &clusterv1beta1.NatsCluster{}
			require.NoError(t, c.Get(t.Context(), key, read))
			changed := read.DeepCopy()
			changed.Annotations[tt.annotation] = "demo-2"
			require.NoError(t, c.Update(t.Context(), changed))

			err := tt.clear(t.Context(), r, read)
			require.True(t, apierrors.IsConflict(err), "got %v", err)
			got := &clusterv1beta1.NatsCluster{}
			require.NoError(t, c.Get(t.Context(), key, got))
			require.Equal(t, "demo-2", got.Annotations[tt.annotation])

			require.NoError(t, tt.clear(t.Context(), r, got))
			require.NoError(t, c.Get(t.Context(), key, got))
			require.NotContains(t, got.Annotations, tt.annotation, "read fresh, it is cleared")
		})
	}
}
