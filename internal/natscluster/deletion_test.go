package natscluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// TestFinalize_ForceDeleteNotCleared pins that force-delete releases a
// deleted NatsCluster without observing it, and stays on it.
func TestFinalize_ForceDeleteNotCleared(t *testing.T) {
	nc := storyCluster(t)
	nc.Finalizers = []string{FinalizerJetStreamData, "example.com/hold"}
	nc.Annotations = map[string]string{clusterv1beta1.AnnotationForceDelete: ""}
	nc.DeletionTimestamp = &metav1.Time{Time: time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)}
	c := fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(nc).WithStatusSubresource(nc).Build()
	r := &Reconciler{Client: c}

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
	require.NoError(t, err)
	got := &clusterv1beta1.NatsCluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(nc), got))
	require.Equal(t, []string{"example.com/hold"}, got.Finalizers)
	require.Contains(t, got.Annotations, clusterv1beta1.AnnotationForceDelete)
}
