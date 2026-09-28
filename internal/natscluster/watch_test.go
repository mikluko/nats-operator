package natscluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// TestSpecChanged pins which NatsCluster updates enqueue it: none that
// writes the status alone.
func TestSpecChanged(t *testing.T) {
	base := &clusterv1beta1.NatsCluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "nats", Generation: 3, ResourceVersion: "10"}}
	tests := []struct {
		name   string
		change func(*clusterv1beta1.NatsCluster)
		want   bool
	}{
		{"status only", func(nc *clusterv1beta1.NatsCluster) {
			nc.Status.Conditions = []metav1.Condition{{Type: ConditionProgressing, Status: metav1.ConditionTrue, Reason: ReasonGateBlocked}}
		}, false},
		{"spec", func(nc *clusterv1beta1.NatsCluster) { nc.Generation++ }, true},
		{"annotation", func(nc *clusterv1beta1.NatsCluster) {
			nc.Annotations = map[string]string{clusterv1beta1.AnnotationForceStep: "demo-2"}
		}, true},
		{"deletion", func(nc *clusterv1beta1.NatsCluster) {
			nc.DeletionTimestamp = &metav1.Time{Time: time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)}
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next := base.DeepCopy()
			next.ResourceVersion = "11"
			tt.change(next)
			require.Equal(t, tt.want, specChanged.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: next}))
		})
	}
}

// enqueued returns the "namespace/name" of each request h enqueues for a
// create event on o.
func enqueued(t *testing.T, h handler.EventHandler, o client.Object) []string {
	t.Helper()
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()
	h.Create(t.Context(), event.CreateEvent{Object: o}, q)
	var out []string
	for q.Len() > 0 {
		r, _ := q.Get()
		out = append(out, r.String())
		q.Done(r)
	}
	return out
}
