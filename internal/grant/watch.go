package grant

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/refindex"
)

// TargetNamespaceField is the field index IndexReferrers registers: the
// namespaces other than its own that a referrer's references name.
const TargetNamespaceField = "grant.nats.mikluko.io/target-namespace"

// IndexReferrers registers TargetNamespaceField on indexer for obj's kind.
// targets returns the namespaces an object's references name; empty strings
// and the object's own namespace are dropped.
func IndexReferrers(ctx context.Context, indexer client.FieldIndexer, obj client.Object, targets func(client.Object) []string) error {
	return indexer.IndexField(ctx, obj, TargetNamespaceField, func(o client.Object) []string {
		return foreignNamespaces(o.GetNamespace(), targets(o))
	})
}

// foreignNamespaces returns namespaces less duplicates, empty strings and
// own, in first-seen order.
func foreignNamespaces(own string, namespaces []string) []string {
	var out []string
	seen := map[string]bool{own: true, "": true}
	for _, ns := range namespaces {
		if !seen[ns] {
			seen[ns] = true
			out = append(out, ns)
		}
	}
	return out
}

// EnqueueReferrers returns the handler that enqueues, for a
// NatsReferenceGrant event, every object of kind whose references the grant,
// before or after an update, may admit. r must carry the index
// IndexReferrers registers for kind.
func EnqueueReferrers(r client.Reader, kind schema.GroupKind, list client.ObjectList) handler.EventHandler {
	e := &enqueuer{r: r, kind: kind, list: list}
	return handler.Funcs{
		CreateFunc: func(ctx context.Context, ev event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			e.enqueue(ctx, q, ev.Object)
		},
		UpdateFunc: func(ctx context.Context, ev event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			e.enqueue(ctx, q, ev.ObjectOld, ev.ObjectNew)
		},
		DeleteFunc: func(ctx context.Context, ev event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			e.enqueue(ctx, q, ev.Object)
		},
		GenericFunc: func(ctx context.Context, ev event.GenericEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			e.enqueue(ctx, q, ev.Object)
		},
	}
}

type enqueuer struct {
	r    client.Reader
	kind schema.GroupKind
	list client.ObjectList
}

func (e *enqueuer) enqueue(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request], objs ...client.Object) {
	for _, req := range e.requests(ctx, objs...) {
		q.Add(req)
	}
}

// requests lists, for each grant among objs, the referrers its from entries
// for e.kind name that point into the grant's namespace.
func (e *enqueuer) requests(ctx context.Context, objs ...client.Object) []reconcile.Request {
	var out []reconcile.Request
	seen := map[types.NamespacedName]bool{}
	for _, obj := range objs {
		g, ok := obj.(*natsv1beta1.NatsReferenceGrant)
		if !ok {
			continue
		}
		for _, f := range g.Spec.From {
			if f.Group != e.kind.Group || f.Kind != e.kind.Kind {
				continue
			}
			for _, req := range refindex.Requests(ctx, e.r, e.list, client.InNamespace(f.Namespace), client.MatchingFields{TargetNamespaceField: g.Namespace}) {
				if !seen[req.NamespacedName] {
					seen[req.NamespacedName] = true
					out = append(out, req)
				}
			}
		}
	}
	return out
}
