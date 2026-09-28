package natscluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// unobservable is an Observer that never reaches the NATS cluster.
type unobservable struct{}

func (unobservable) Observe(context.Context, *clusterv1beta1.NatsCluster) (*sysobs.Snapshot, error) {
	return nil, errors.New("no responders")
}

func (unobservable) ObserveLeafs(context.Context, *clusterv1beta1.NatsCluster) (map[string][]sysobs.Leaf, error) {
	return nil, errors.New("no responders")
}

// TestReconcile_ErrorWithoutRequeueAfter pins that a reconcile failing
// where it would otherwise wait returns the error alone: controller-runtime
// ignores a Result beside one.
func TestReconcile_ErrorWithoutRequeueAfter(t *testing.T) {
	deleted := func(t *testing.T) *clusterv1beta1.NatsCluster {
		nc := storyAuthCluster(t)
		nc.Finalizers = []string{FinalizerJetStreamData}
		nc.DeletionTimestamp = &metav1.Time{Time: time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)}
		return nc
	}
	tests := []struct {
		name string
		nc   func(*testing.T) *clusterv1beta1.NatsCluster
	}{
		{"held on missing trust", storyAuthCluster},
		{"deletion waiting on an unobserved NATS cluster", deleted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := tt.nc(t)
			c := fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(nc).WithStatusSubresource(nc).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
						return errors.New("injected status patch failure")
					},
				}).Build()
			r := &Reconciler{Client: c, Observer: unobservable{}}
			res, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
			require.ErrorContains(t, err, "injected status patch failure")
			require.Zero(t, res)
		})
	}
}
