// Package refindex carries the field indexes the JetStream controller's
// reconcilers register over the references their resources make, and the
// event handlers that enqueue a referrer when what it references changes.
package refindex

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

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

// OwnerExists returns a lifecycle.Resource OwnerExists for the kind list
// lists; r carries UIDField.
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

// Namespaces adapts refs to grant.IndexReferrers: the namespace each
// reference names, "" where it names none.
func Namespaces(refs func(client.Object) []natsv1beta1.ObjectReference) func(client.Object) []string {
	return func(o client.Object) []string {
		var out []string
		for _, r := range refs(o) {
			out = append(out, r.Namespace)
		}
		return out
	}
}
