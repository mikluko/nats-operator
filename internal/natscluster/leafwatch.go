package natscluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// Field indexes on NatsClusters: the namespace/name of every NatsConnection
// and NatsAccountTrust their leafRemotes name.
const (
	LeafConnectionField   = "cluster.nats.mikluko.io/leaf-connection"
	LeafAccountTrustField = "cluster.nats.mikluko.io/leaf-account-trust"
)

func leafConnectionKeys(nc *clusterv1beta1.NatsCluster) []string {
	var out []string
	for _, r := range nc.Spec.LeafRemotes {
		out = append(out, refKey(nc, r.ConnectionRef))
	}
	return out
}

func leafAccountTrustKeys(nc *clusterv1beta1.NatsCluster) []string {
	var out []string
	for _, r := range nc.Spec.LeafRemotes {
		if r.LocalAccountTrustRef != nil {
			out = append(out, refKey(nc, *r.LocalAccountTrustRef))
		}
	}
	return out
}

// refKey is the namespace/name ref resolves to from nc.
func refKey(nc *clusterv1beta1.NatsCluster, ref natsv1beta1.ObjectReference) string {
	ns := ref.Namespace
	if ns == "" {
		ns = nc.Namespace
	}
	return ns + "/" + ref.Name
}

// ConnectionSecretField is the field index on NatsConnections of the
// namespace/name of every Secret they read.
const ConnectionSecretField = "cluster.nats.mikluko.io/connection-secret"

func connectionSecretKeys(conn *natsv1beta1.NatsConnection) []string {
	var out []string
	for _, s := range natsconn.SecretNames(&conn.Spec) {
		out = append(out, conn.Namespace+"/"+s)
	}
	return out
}

// indexLeafRefs registers LeafConnectionField, LeafAccountTrustField and
// ConnectionSecretField.
func indexLeafRefs(ctx context.Context, idx client.FieldIndexer) error {
	for field, keys := range map[string]func(*clusterv1beta1.NatsCluster) []string{
		LeafConnectionField:   leafConnectionKeys,
		LeafAccountTrustField: leafAccountTrustKeys,
	} {
		if err := idx.IndexField(ctx, &clusterv1beta1.NatsCluster{}, field, func(o client.Object) []string {
			return keys(o.(*clusterv1beta1.NatsCluster))
		}); err != nil {
			return fmt.Errorf("index NatsCluster %s: %w", field, err)
		}
	}
	if err := idx.IndexField(ctx, &natsv1beta1.NatsConnection{}, ConnectionSecretField, func(o client.Object) []string {
		return connectionSecretKeys(o.(*natsv1beta1.NatsConnection))
	}); err != nil {
		return fmt.Errorf("index NatsConnection %s: %w", ConnectionSecretField, err)
	}
	return nil
}

// watchLeafRefs enqueues a NatsCluster when a NatsConnection or
// NatsAccountTrust its leafRemotes name changes, or a Secret such a
// NatsConnection reads.
func (r *Reconciler) watchLeafRefs(b *builder.Builder) *builder.Builder {
	return b.
		Watches(&natsv1beta1.NatsConnection{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.clustersByField(ctx, LeafConnectionField, o.GetNamespace()+"/"+o.GetName())
		})).
		Watches(&natsv1beta1.NatsAccountTrust{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.clustersByField(ctx, LeafAccountTrustField, o.GetNamespace()+"/"+o.GetName())
		})).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.clustersReadingSecret),
			builder.WithPredicates(predicate.NewPredicateFuncs(r.readByConnection)))
}

// clustersByField maps key to the NatsClusters whose field index field
// holds it.
func (r *Reconciler) clustersByField(ctx context.Context, field, key string) []reconcile.Request {
	var list clusterv1beta1.NatsClusterList
	if err := r.Client.List(ctx, &list, client.MatchingFields{field: key}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "list NatsClusters by field index", "field", field, "key", key)
		return nil
	}
	out := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
	}
	return out
}

// readByConnection reports whether a NatsConnection reads Secret s.
func (r *Reconciler) readByConnection(s client.Object) bool {
	return len(r.connectionsReading(context.Background(), s)) > 0
}

// connectionsReading are the NatsConnections that read Secret s.
func (r *Reconciler) connectionsReading(ctx context.Context, s client.Object) []natsv1beta1.NatsConnection {
	var conns natsv1beta1.NatsConnectionList
	key := s.GetNamespace() + "/" + s.GetName()
	if err := r.Client.List(ctx, &conns, client.MatchingFields{ConnectionSecretField: key}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "list NatsConnections reading secret", "secret", key)
		return nil
	}
	return conns.Items
}

// clustersReadingSecret maps a Secret to the NatsClusters whose leafRemotes
// name a NatsConnection reading it.
func (r *Reconciler) clustersReadingSecret(ctx context.Context, s client.Object) []reconcile.Request {
	var out []reconcile.Request
	for _, conn := range r.connectionsReading(ctx, s) {
		out = append(out, r.clustersByField(ctx, LeafConnectionField, conn.Namespace+"/"+conn.Name)...)
	}
	return out
}
