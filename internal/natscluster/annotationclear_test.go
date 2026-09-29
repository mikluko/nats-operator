package natscluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// TestAnnotationClear_ConflictsWithConcurrentChange pins that the
// replace-server and force-step annotations are cleared only while they
// hold the value that was read: a value set since conflicts and stays,
// including when it lands before a status patch that refreshes the
// NatsCluster the clear starts from.
func TestAnnotationClear_ConflictsWithConcurrentChange(t *testing.T) {
	demo1 := Server{Name: "demo-1"}
	tests := []struct {
		name       string
		annotation string
		removals   []clusterv1beta1.ServerRemoval
		// inStatusPatch lands the change just before the first status
		// patch rather than before the call.
		inStatusPatch bool
		clear         func(ctx context.Context, r *Reconciler, nc *clusterv1beta1.NatsCluster) error
		// wantRemovals is status.removals after the conflicting call.
		wantRemovals []clusterv1beta1.ServerRemoval
	}{
		{
			name:       "replace-server, empty plan",
			annotation: clusterv1beta1.AnnotationReplaceServer,
			clear: func(ctx context.Context, r *Reconciler, nc *clusterv1beta1.NatsCluster) error {
				return r.requestReplacement(ctx, nc, &Plan{}, nil)
			},
		},
		{
			name:          "replace-server, server marked for replacement",
			annotation:    clusterv1beta1.AnnotationReplaceServer,
			inStatusPatch: true,
			clear: func(ctx context.Context, r *Reconciler, nc *clusterv1beta1.NatsCluster) error {
				return r.requestReplacement(ctx, nc, &Plan{Servers: []Server{demo1}}, map[string]*appsv1.StatefulSet{"demo-1": {}})
			},
			wantRemovals: []clusterv1beta1.ServerRemoval{{Name: "demo-1", Phase: clusterv1beta1.RemovalRequested}},
		},
		{
			name:       "force-step, empty plan",
			annotation: clusterv1beta1.AnnotationForceStep,
			clear: func(ctx context.Context, r *Reconciler, nc *clusterv1beta1.NatsCluster) error {
				_, err := r.rollout(ctx, nc, &Plan{}, Observed{})
				return err
			},
		},
		{
			name:          "force-step, rejoined server forgotten",
			annotation:    clusterv1beta1.AnnotationForceStep,
			removals:      []clusterv1beta1.ServerRemoval{{Name: "demo-1", Phase: clusterv1beta1.RemovalRejoining}},
			inStatusPatch: true,
			clear: func(ctx context.Context, r *Reconciler, nc *clusterv1beta1.NatsCluster) error {
				_, err := r.rollout(ctx, nc, &Plan{}, Observed{Snapshot: &sysobs.Snapshot{}})
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stored := &clusterv1beta1.NatsCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "nats-system", Name: "demo", Annotations: map[string]string{tt.annotation: "demo-1"},
				},
				Status: clusterv1beta1.NatsClusterStatus{Removals: tt.removals},
			}
			key := client.ObjectKeyFromObject(stored)
			change := func(ctx context.Context, c client.Client) error {
				cur := &clusterv1beta1.NatsCluster{}
				if err := c.Get(ctx, key, cur); err != nil {
					return err
				}
				cur.Annotations[tt.annotation] = "demo-2"
				return c.Update(ctx, cur)
			}
			changed := false
			c := fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(stored).WithStatusSubresource(stored).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
						if tt.inStatusPatch && !changed {
							changed = true
							if err := change(ctx, c); err != nil {
								return err
							}
						}
						return c.SubResource(sub).Patch(ctx, obj, p, opts...)
					},
				}).Build()
			r := &Reconciler{Client: c, Now: func() time.Time { return time.Unix(0, 0) }}

			read := &clusterv1beta1.NatsCluster{}
			require.NoError(t, c.Get(t.Context(), key, read))
			if !tt.inStatusPatch {
				require.NoError(t, change(t.Context(), c))
			}

			err := tt.clear(t.Context(), r, read)
			require.True(t, apierrors.IsConflict(err), "got %v", err)
			got := &clusterv1beta1.NatsCluster{}
			require.NoError(t, c.Get(t.Context(), key, got))
			require.Equal(t, "demo-2", got.Annotations[tt.annotation])
			require.Equal(t, tt.wantRemovals, withoutSince(got.Status.Removals))

			require.NoError(t, tt.clear(t.Context(), r, got))
			require.NoError(t, c.Get(t.Context(), key, got))
			require.NotContains(t, got.Annotations, tt.annotation, "read fresh, it is cleared")
		})
	}
}
