package natsconn

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// Ready condition vocabulary on a NatsConnection.
const (
	ConditionReady = "Ready"

	ReasonConnected      = "Connected"
	ReasonDisconnected   = "Disconnected"
	ReasonSecretNotFound = "SecretNotFound"
	ReasonInvalidSecret  = "InvalidSecret"
	ReasonConnectFailed  = "ConnectFailed"
)

// SecretField is the field index SetupWithManager registers on
// NatsConnections: the names of the Secrets a connection reads.
const SecretField = "natsconn.nats.mikluko.io/secret"

// DefaultRetryAfter is how soon a NatsConnection that is not Ready is
// reconciled again when Reconciler.RetryAfter is zero.
const DefaultRetryAfter = 30 * time.Second

// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsconnections,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsconnections/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconciler reports Ready on NatsConnections from their pooled
// connections: True while connected, False with the reason otherwise. It
// closes a connection whose NatsConnection is deleted or no longer
// resolves, and reconciles again when a Secret it reads changes or its
// connection disconnects or reconnects.
type Reconciler struct {
	Client     client.Client
	Pool       *Pool
	RetryAfter time.Duration

	queue atomic.Pointer[workqueue.TypedRateLimitingInterface[reconcile.Request]]
}

// Reconcile implements reconcile.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	key := ConnectionKey(req.NamespacedName)
	var nc natsv1beta1.NatsConnection
	if err := r.Client.Get(ctx, req.NamespacedName, &nc); err != nil {
		if apierrors.IsNotFound(err) {
			r.Pool.Forget(key)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("get NatsConnection: %w", err)
	}
	if !nc.DeletionTimestamp.IsZero() {
		r.Pool.Forget(key)
		return reconcile.Result{}, nil
	}

	cond, err := r.observe(ctx, &nc)
	if err != nil {
		return reconcile.Result{}, err
	}
	if err := r.setReady(ctx, &nc, cond); err != nil {
		return reconcile.Result{}, err
	}
	if cond.Status != metav1.ConditionTrue {
		return reconcile.Result{RequeueAfter: r.retryAfter()}, nil
	}
	return reconcile.Result{}, nil
}

// observe dials nc through the pool and returns its Ready condition. The
// error is one the API server returned, never one about NATS.
func (r *Reconciler) observe(ctx context.Context, nc *natsv1beta1.NatsConnection) (metav1.Condition, error) {
	key := ConnectionKey(client.ObjectKeyFromObject(nc))
	ep, err := ReadEndpoint(ctx, r.Client, nc.Namespace, &nc.Spec)
	switch {
	case errors.Is(err, ErrSecretNotFound):
		r.Pool.Forget(key)
		return notReady(ReasonSecretNotFound, err), nil
	case errors.Is(err, ErrKeyNotFound):
		r.Pool.Forget(key)
		return notReady(ReasonInvalidSecret, err), nil
	case err != nil:
		return metav1.Condition{}, err
	}
	conn, err := r.Pool.Get(key, ep)
	switch {
	case errors.Is(err, ErrInvalidCA), errors.Is(err, ErrInvalidCredentials):
		r.Pool.Forget(key)
		return notReady(ReasonInvalidSecret, err), nil
	case err != nil:
		return notReady(ReasonConnectFailed, err), nil
	case !conn.IsConnected():
		return metav1.Condition{
			Type:    ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  ReasonDisconnected,
			Message: fmt.Sprintf("connection is %s", conn.Status()),
		}, nil
	}
	return metav1.Condition{
		Type:    ConditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  ReasonConnected,
		Message: fmt.Sprintf("connected to %s", conn.ConnectedUrlRedacted()),
	}, nil
}

func notReady(reason string, err error) metav1.Condition {
	return metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: err.Error()}
}

func (r *Reconciler) setReady(ctx context.Context, nc *natsv1beta1.NatsConnection, cond metav1.Condition) error {
	orig := nc.DeepCopy()
	cond.ObservedGeneration = nc.Generation
	changed := meta.SetStatusCondition(&nc.Status.Conditions, cond)
	if !changed && nc.Status.ObservedGeneration == nc.Generation {
		return nil
	}
	nc.Status.ObservedGeneration = nc.Generation
	if err := r.Client.Status().Patch(ctx, nc, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("patch NatsConnection status: %w", err)
	}
	return nil
}

func (r *Reconciler) retryAfter() time.Duration {
	if r.RetryAfter > 0 {
		return r.RetryAfter
	}
	return DefaultRetryAfter
}

// enqueue queues a NatsConnection whose pooled connection changed state.
// It never blocks, and drops keys of other kinds and keys arriving before
// the controller has started.
func (r *Reconciler) enqueue(key Key) {
	if key.Kind != Kind {
		return
	}
	if q := r.queue.Load(); q != nil {
		(*q).Add(reconcile.Request{NamespacedName: key.NamespacedName})
	}
}

// SetupWithManager registers SecretField on mgr's cache and builds the
// controller, watching NatsConnections, the Secrets they read and the
// pool's connection changes.
func (r *Reconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(ctx, &natsv1beta1.NatsConnection{}, SecretField, func(o client.Object) []string {
		return SecretNames(&o.(*natsv1beta1.NatsConnection).Spec)
	}); err != nil {
		return fmt.Errorf("index NatsConnection secrets: %w", err)
	}
	r.Pool.OnChange(r.enqueue)
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1beta1.NatsConnection{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.connectionsReading)).
		WatchesRawSource(source.Func(func(_ context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
			r.queue.Store(&q)
			return nil
		})).
		Complete(r)
}

// connectionsReading maps a Secret to the NatsConnections in its namespace
// that read it.
func (r *Reconciler) connectionsReading(ctx context.Context, s client.Object) []reconcile.Request {
	var list natsv1beta1.NatsConnectionList
	if err := r.Client.List(ctx, &list, client.InNamespace(s.GetNamespace()), client.MatchingFields{SecretField: s.GetName()}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "list NatsConnections reading secret", "secret", client.ObjectKeyFromObject(s))
		return nil
	}
	out := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
	}
	return out
}

// SecretNames returns the names of the Secrets spec reads, without
// duplicates.
func SecretNames(spec *natsv1beta1.NatsConnectionSpec) []string {
	var out []string
	if spec.TLS != nil && spec.TLS.CA != nil {
		out = append(out, spec.TLS.CA.SecretKeyRef.Name)
	}
	if spec.Credentials != nil && (len(out) == 0 || out[0] != spec.Credentials.SecretKeyRef.Name) {
		out = append(out, spec.Credentials.SecretKeyRef.Name)
	}
	return out
}
