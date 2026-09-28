package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/refindex"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// A Kind adapts one JetStream object resource kind, P, to the reconcile loop
// every such kind shares.
type Kind[P interface {
	client.Object
	DeepCopy() P
}] struct {
	// Name is the kind, as "NatsStream".
	Name string
	New  func() P
	List func() client.ObjectList
	// Fields points into obj.
	Fields func(obj P) Fields
	// Resolve returns obj's server object, asking with ready that every
	// resource it depends on be Ready, or nil with why recorded on obj's
	// status and gone set where nothing will reach it. Nil resolves through
	// Connection and Bind.
	Resolve func(ctx context.Context, c client.Client, d *natsconn.Dialer, obj P, ready bool) (o Object, gone bool, err error)
	// Connection is the NatsConnection reference obj makes and Bind binds
	// obj's server object through it, where Resolve is nil.
	Connection func(obj P) natsv1beta1.ObjectReference
	Bind       func(api *API, c client.Client, obj P) Object
	// Record writes info, the server object as Sync read it, into obj's
	// status.
	Record func(obj P, info *Info)
	// Observe, where set, runs after Record and records on obj what else the
	// server reports; its error fails the reconcile, with no requeue, after
	// status is written, and turns a True Ready False with ReasonObserveFailed.
	Observe func(ctx context.Context, o Object, obj P, info *Info) error
	// Conns is the NatsConnection references obj makes, nil being
	// Connection's, and Refs every reference a grant must admit, nil being
	// Conns.
	Conns, Refs func(obj P) []natsv1beta1.ObjectReference
	// Watches registers what the kind indexes and watches beside what every
	// kind does; nil registers nothing.
	Watches func(ctx context.Context, mgr ctrl.Manager, b *builder.Builder) (*builder.Builder, error)
}

// Fields are the parts of a resource the reconcile loop reads and writes.
type Fields struct {
	Policies jetstreamv1beta1.Policies
	Deletion jetstreamv1beta1.DeletionPolicy
	Sync     *jetstreamv1beta1.SyncStatus
	// Status is the resource's whole status by value, compared to decide
	// whether to write it.
	Status any
}

// Referrer is a resource of the kind in namespace, as a grant admits it.
func (k Kind[P]) Referrer(namespace string) grant.Referrer {
	return grant.Referrer{Group: jetstreamv1beta1.GroupVersion.Group, Kind: k.Name, Namespace: namespace}
}

// Reconcile brings the resource req names to its spec under s, or runs its
// deletion policy where it is being deleted.
func (k Kind[P]) Reconcile(ctx context.Context, c client.Client, d *natsconn.Dialer, s Syncer, req reconcile.Request) (reconcile.Result, error) {
	obj := k.New()
	if err := c.Get(ctx, req.NamespacedName, obj); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !obj.GetDeletionTimestamp().IsZero() {
		return k.finalize(ctx, c, d, obj)
	}
	if err := AddFinalizer(ctx, c, obj); err != nil {
		return reconcile.Result{}, err
	}
	base := obj.DeepCopy()
	res, syncErr := k.sync(ctx, c, d, s, obj)
	if err := PatchStatus(ctx, c, base, obj, k.Fields(base).Status, k.Fields(obj).Status); err != nil {
		return reconcile.Result{}, errors.Join(syncErr, err)
	}
	return res, syncErr
}

func (k Kind[P]) sync(ctx context.Context, c client.Client, d *natsconn.Dialer, s Syncer, obj P) (reconcile.Result, error) {
	o, _, err := k.resolve(ctx, c, d, obj, true)
	if err != nil {
		return reconcile.Result{}, err
	}
	if o == nil {
		return reconcile.Result{RequeueAfter: natsconn.DefaultRetryAfter}, nil
	}
	f := k.Fields(obj)
	res, info, err := s.Sync(ctx, Resource{
		Object:      obj,
		Policies:    f.Policies,
		Status:      f.Sync,
		OwnerExists: refindex.OwnerExists(c, k.List()),
	}, o)
	if info != nil {
		k.Record(obj, info)
		if k.Observe != nil {
			if obsErr := k.Observe(ctx, o, obj, info); obsErr != nil {
				observeFailed(f.Sync, obj.GetGeneration(), obsErr)
				return reconcile.Result{}, errors.Join(err, obsErr)
			}
		}
	}
	return res, err
}

// observeFailed turns a Ready condition that is True False, naming err.
func observeFailed(status *jetstreamv1beta1.SyncStatus, generation int64, err error) {
	if !meta.IsStatusConditionTrue(status.Conditions, ConditionReady) {
		return
	}
	conditions.Set(&status.Conditions, generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonObserveFailed, Message: err.Error()})
}

// finalize runs obj's deletion policy and drops its finalizer. A WaitError
// from the policy leaves nothing to delete.
func (k Kind[P]) finalize(ctx context.Context, c client.Client, d *natsconn.Dialer, obj P) (reconcile.Result, error) {
	if f := k.Fields(obj); f.Deletion == jetstreamv1beta1.DeletionDelete {
		base := obj.DeepCopy()
		o, gone, err := k.resolve(ctx, c, d, obj, false)
		if err != nil {
			return reconcile.Result{}, err
		}
		var wait *WaitError
		switch {
		case o == nil && !gone:
			if err := PatchStatus(ctx, c, base, obj, k.Fields(base).Status, k.Fields(obj).Status); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{RequeueAfter: natsconn.DefaultRetryAfter}, nil
		case o == nil:
		default:
			if err := Finalize(ctx, obj.GetUID(), f.Deletion, o); err != nil && !errors.As(err, &wait) {
				return reconcile.Result{}, err
			}
		}
	}
	return reconcile.Result{}, RemoveFinalizer(ctx, c, obj)
}

// SetupWithManager registers r and the field indexes it reads with mgr.
func (k Kind[P]) SetupWithManager(ctx context.Context, mgr ctrl.Manager, r reconcile.Reconciler) error {
	idx := mgr.GetFieldIndexer()
	typed := func(of func(P) []natsv1beta1.ObjectReference) func(client.Object) []natsv1beta1.ObjectReference {
		return func(o client.Object) []natsv1beta1.ObjectReference {
			p, ok := o.(P)
			if !ok {
				return nil
			}
			return of(p)
		}
	}
	conns := k.Conns
	if conns == nil {
		conns = func(obj P) []natsv1beta1.ObjectReference { return []natsv1beta1.ObjectReference{k.Connection(obj)} }
	}
	refs := k.Refs
	if refs == nil {
		refs = conns
	}
	if err := refindex.IndexConnections(ctx, idx, k.New(), typed(conns)); err != nil {
		return fmt.Errorf("index %s connections: %w", k.Name, err)
	}
	if err := refindex.IndexUID(ctx, idx, k.New()); err != nil {
		return fmt.Errorf("index %s UIDs: %w", k.Name, err)
	}
	if err := grant.IndexReferrers(ctx, idx, k.New(), refindex.Namespaces(typed(refs))); err != nil {
		return fmt.Errorf("index %s grant targets: %w", k.Name, err)
	}
	b := ctrl.NewControllerManagedBy(mgr).
		For(k.New(), builder.WithPredicates(SpecOrDeletion())).
		Watches(&natsv1beta1.NatsConnection{}, refindex.EnqueueByField(mgr.GetClient(), k.List(), refindex.ConnectionField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(mgr.GetClient(), jetstreamv1beta1.GroupVersion.WithKind(k.Name).GroupKind(), k.List()))
	if k.Watches != nil {
		var err error
		if b, err = k.Watches(ctx, mgr, b); err != nil {
			return err
		}
	}
	return b.Complete(telemetry.Traced(k.Name, r))
}

func (k Kind[P]) resolve(ctx context.Context, c client.Client, d *natsconn.Dialer, obj P, ready bool) (Object, bool, error) {
	if k.Resolve != nil {
		return k.Resolve(ctx, c, d, obj, ready)
	}
	api, why, err := Connect(ctx, d, k.Referrer(obj.GetNamespace()), k.Connection(obj), k.Fields(obj).Sync, obj.GetGeneration())
	if api == nil {
		return nil, why.Released(), err
	}
	return k.Bind(api, c, obj), false, nil
}

// WriteSpec writes server, a config read from the server, into the part of
// obj's spec config names and persists it: every field when replace is set,
// otherwise only the fields obj's spec omits.
func WriteSpec[P interface {
	client.Object
	DeepCopy() P
}, C any](ctx context.Context, c client.Client, obj P, config func(P) *C, server C, replace bool) error {
	want := obj.DeepCopy()
	dst := config(want)
	if replace {
		*dst = server
	} else if _, err := FillOmitted(dst, &server); err != nil {
		return err
	}
	return PatchSpec(ctx, c, obj, want, config(obj), *dst)
}
