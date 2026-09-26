package natscluster

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// FinalizerJetStreamData holds a NatsCluster with JetStream while stream
// groups remain placed in its NATS cluster.
const FinalizerJetStreamData = "cluster.nats.mikluko.io/jetstream-data"

// ConditionDeleting is True while a deleted NatsCluster waits.
const ConditionDeleting = "Deleting"

// Deleting reasons.
const (
	ReasonJetStreamDataRemains = "JetStreamDataRemains"
	ReasonNoJetStreamData      = "NoJetStreamData"
)

// guardDeletion puts FinalizerJetStreamData on a NatsCluster with
// JetStream.
func (r *Reconciler) guardDeletion(ctx context.Context, nc *clusterv1beta1.NatsCluster) error {
	if nc.Spec.JetStream == nil || controllerutil.ContainsFinalizer(nc, FinalizerJetStreamData) {
		return nil
	}
	orig := nc.DeepCopy()
	controllerutil.AddFinalizer(nc, FinalizerJetStreamData)
	if err := r.Client.Patch(ctx, nc, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("add finalizer: %w", err)
	}
	return nil
}

// finalize releases a deleted NatsCluster once its NATS cluster holds no
// stream group, or at once when it runs no JetStream or carries the
// force-delete annotation. Until then it reports Deleting=True, also while
// the NATS cluster cannot be observed.
func (r *Reconciler) finalize(ctx context.Context, nc *clusterv1beta1.NatsCluster) (ctrl.Result, error) {
	key := client.ObjectKeyFromObject(nc)
	if controllerutil.ContainsFinalizer(nc, FinalizerJetStreamData) {
		if _, force := nc.Annotations[clusterv1beta1.AnnotationForceDelete]; !force && nc.Spec.JetStream != nil {
			snap, err := r.Observer.Observe(ctx, nc)
			if c := deletingCondition(nc.Name, snap, err); c.Status == metav1.ConditionTrue {
				orig := nc.DeepCopy()
				setCondition(&nc.Status, c, nc.Generation)
				return ctrl.Result{RequeueAfter: resyncUnsettled}, r.patchStatus(ctx, orig, nc)
			}
		}
		orig := nc.DeepCopy()
		controllerutil.RemoveFinalizer(nc, FinalizerJetStreamData)
		if err := r.Client.Patch(ctx, nc, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
		}
	}
	if r.Forget != nil {
		r.Forget(key)
	}
	return ctrl.Result{}, nil
}

// deletingCondition judges whether the NATS cluster named cluster still
// holds stream groups, from an observation snap or the error that stopped
// one.
func deletingCondition(cluster string, snap *sysobs.Snapshot, observeErr error) metav1.Condition {
	c := metav1.Condition{Type: ConditionDeleting, Status: metav1.ConditionTrue}
	if snap == nil {
		c.Reason = ReasonObservationFailed
		c.Message = "cannot tell whether JetStream data remains"
		if observeErr != nil {
			c.Message += ": " + observeErr.Error()
		}
		return c
	}
	var streams []string
	for _, g := range snap.Groups {
		if g.Kind == sysobs.KindStream {
			streams = append(streams, streamLabel(g))
		}
	}
	if len(streams) == 0 {
		c.Status, c.Reason = metav1.ConditionFalse, ReasonNoJetStreamData
		return c
	}
	c.Reason = ReasonJetStreamDataRemains
	c.Message = fmt.Sprintf("%d stream groups still placed in %s (%s)", len(streams), cluster, namedList(streams))
	return c
}
