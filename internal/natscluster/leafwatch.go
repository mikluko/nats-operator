package natscluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/refindex"
)

// Field indexes on NatsClusters: the namespace/name of every NatsConnection
// their leafRemotes name, and of every NatsAccountTrust their leafRemotes or
// auth.accountTrustRefs name.
const (
	LeafConnectionField = "cluster.nats-operator.io/leaf-connection"
	AccountTrustField   = "cluster.nats-operator.io/account-trust"
)

func leafConnectionKeys(nc *clusterv1beta1.NatsCluster) []string {
	var out []string
	for _, r := range nc.Spec.LeafRemotes {
		out = append(out, r.ConnectionRef.ObjectKey(nc.Namespace).String())
	}
	return out
}

func accountTrustKeys(nc *clusterv1beta1.NatsCluster) []string {
	var out []string
	for _, r := range nc.Spec.LeafRemotes {
		if r.LocalAccountTrustRef != nil {
			out = append(out, r.LocalAccountTrustRef.ObjectKey(nc.Namespace).String())
		}
	}
	if a := nc.Spec.Auth; a != nil {
		for _, ref := range a.AccountTrustRefs {
			out = append(out, ref.ObjectKey(nc.Namespace).String())
		}
	}
	return out
}

// ConnectionSecretField is the field index on NatsConnections of the
// namespace/name of every Secret they read.
const ConnectionSecretField = "cluster.nats-operator.io/connection-secret"

func connectionSecretKeys(conn *natsv1beta1.NatsConnection) []string {
	var out []string
	for _, s := range natsconn.SecretNames(&conn.Spec) {
		out = append(out, conn.Namespace+"/"+s)
	}
	return out
}

// indexLeafRefs registers LeafConnectionField, AccountTrustField and
// ConnectionSecretField.
func indexLeafRefs(ctx context.Context, idx client.FieldIndexer) error {
	for field, keys := range map[string]func(*clusterv1beta1.NatsCluster) []string{
		LeafConnectionField: leafConnectionKeys,
		AccountTrustField:   accountTrustKeys,
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
// NatsAccountTrust it names changes, or a Secret such a NatsConnection
// reads.
func (r *Reconciler) watchLeafRefs(b *builder.Builder) *builder.Builder {
	clusters := &clusterv1beta1.NatsClusterList{}
	return b.
		Watches(&natsv1beta1.NatsConnection{}, refindex.EnqueueByField(r.Client, clusters, LeafConnectionField)).
		Watches(&natsv1beta1.NatsAccountTrust{}, refindex.EnqueueByField(r.Client, clusters, AccountTrustField)).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.clustersReadingSecret))
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
		out = append(out, refindex.Requests(ctx, r.Client, &clusterv1beta1.NatsClusterList{},
			client.MatchingFields{LeafConnectionField: client.ObjectKeyFromObject(&conn).String()})...)
	}
	return out
}
