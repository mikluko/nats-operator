package lifecycle

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// Ready reasons while a resource cannot reach its connection.
const (
	ReasonConnectionNotFound = "ConnectionNotFound"
	ReasonConnectionFailed   = "ConnectionFailed"
)

// Connect returns the API through the NatsConnection that from's ref names.
// Where the ref cannot be followed, because no grant admits it, the
// NatsConnection does not exist, or its Secrets or servers fail, it records
// why on status at generation and returns a nil API and no error. The error
// is one the Kubernetes API server returned.
func Connect(ctx context.Context, d *natsconn.Dialer, from grant.Referrer, ref natsv1beta1.ObjectReference, status *jetstreamv1beta1.SyncStatus, generation int64) (*API, error) {
	nc, denied, err := d.Reference(ctx, from, ref)
	switch {
	case denied != nil:
		denied.ObservedGeneration = generation
		meta.SetStatusCondition(&status.Conditions, *denied)
		NotReady(status, generation, grant.ReasonReferenceNotPermitted, denied.Message)
		return nil, nil
	case apierrors.IsNotFound(err):
		NotReady(status, generation, ReasonConnectionNotFound, err.Error())
		return nil, nil
	case isAPIStatus(err):
		return nil, err
	case err != nil:
		NotReady(status, generation, ReasonConnectionFailed, err.Error())
		return nil, nil
	}
	meta.RemoveStatusCondition(&status.Conditions, grant.ConditionReferencesResolved)
	return &API{Conn: nc}, nil
}

func isAPIStatus(err error) bool {
	var s apierrors.APIStatus
	return errors.As(err, &s)
}

// Field indexes the controllers register.
const (
	// ConnectionField holds "namespace/name" of each NatsConnection a
	// resource names.
	ConnectionField = "jetstream.nats.mikluko.io/connection"
	// UIDField holds a resource's UID.
	UIDField = "jetstream.nats.mikluko.io/uid"
)

// IndexConnections registers ConnectionField for obj's kind; refs returns
// the connection references of an object, a reference without a namespace
// naming the object's own.
func IndexConnections(ctx context.Context, indexer client.FieldIndexer, obj client.Object, refs func(client.Object) []natsv1beta1.ObjectReference) error {
	return indexer.IndexField(ctx, obj, ConnectionField, func(o client.Object) []string {
		var out []string
		for _, r := range refs(o) {
			ns := r.Namespace
			if ns == "" {
				ns = o.GetNamespace()
			}
			out = append(out, types.NamespacedName{Namespace: ns, Name: r.Name}.String())
		}
		return out
	})
}

// EnqueueByField maps an object to the objects, listed through list's type,
// whose field index holds its "namespace/name", as ConnectionField does
// for a NatsConnection; r carries the index.
func EnqueueByField(r client.Reader, list client.ObjectList, field string) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
		return listRequests(ctx, r, list, client.MatchingFields{field: client.ObjectKeyFromObject(o).String()})
	})
}

func listRequests(ctx context.Context, r client.Reader, list client.ObjectList, opts ...client.ListOption) []reconcile.Request {
	l, ok := list.DeepCopyObject().(client.ObjectList)
	if !ok {
		return nil
	}
	if err := r.List(ctx, l, opts...); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "list referrers")
		return nil
	}
	items, err := meta.ExtractList(l)
	if err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "extract referrers")
		return nil
	}
	out := make([]reconcile.Request, 0, len(items))
	for _, it := range items {
		if o, ok := it.(client.Object); ok {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(o)})
		}
	}
	return out
}

// IndexUID registers UIDField for obj's kind.
func IndexUID(ctx context.Context, indexer client.FieldIndexer, obj client.Object) error {
	return indexer.IndexField(ctx, obj, UIDField, func(o client.Object) []string {
		return []string{string(o.GetUID())}
	})
}

// OwnerExists returns Resource.OwnerExists for the kind list lists; r
// carries UIDField.
func OwnerExists(r client.Reader, list client.ObjectList) func(context.Context, types.UID) (bool, error) {
	return func(ctx context.Context, uid types.UID) (bool, error) {
		l, ok := list.DeepCopyObject().(client.ObjectList)
		if !ok {
			return false, fmt.Errorf("%T is not a list", list)
		}
		if err := r.List(ctx, l, client.MatchingFields{UIDField: string(uid)}); err != nil {
			return false, fmt.Errorf("list owners by UID: %w", err)
		}
		return meta.LenList(l) > 0, nil
	}
}
