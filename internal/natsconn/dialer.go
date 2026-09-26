package natsconn

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
)

// Kind is the kind of a NatsConnection, as a Key and a grant target name it.
const Kind = "NatsConnection"

// ConnectionKey is the pool Key of the NatsConnection named by key.
func ConnectionKey(key client.ObjectKey) Key {
	return Key{Kind: Kind, NamespacedName: key}
}

// Dialer reaches NATS through NatsConnections, sharing one pooled
// connection per NatsConnection among every resource that names it.
type Dialer struct {
	Reader client.Reader
	Pool   *Pool
}

// Connection returns the pooled connection for nc, resolving its Secrets in
// nc's namespace.
func (d *Dialer) Connection(ctx context.Context, nc *natsv1beta1.NatsConnection) (*nats.Conn, error) {
	ep, err := ReadEndpoint(ctx, d.Reader, nc.Namespace, &nc.Spec)
	if err != nil {
		return nil, err
	}
	return d.Pool.Get(ConnectionKey(client.ObjectKeyFromObject(nc)), ep)
}

// Reference returns the connection for the NatsConnection that from's ref
// names. A ref into another namespace is dialed only where a
// NatsReferenceGrant there admits from; otherwise Reference returns the
// grant package's ReferencesResolved=False condition, no connection and no
// error. The Secrets are those of the NatsConnection's namespace, so the
// grant is what admits from to them.
func (d *Dialer) Reference(ctx context.Context, from grant.Referrer, ref natsv1beta1.ObjectReference) (*nats.Conn, *metav1.Condition, error) {
	ns := ref.Namespace
	if ns == "" {
		ns = from.Namespace
	}
	denied, err := grant.Admit(ctx, d.Reader, from, grant.Target{
		Group:     natsv1beta1.GroupVersion.Group,
		Kind:      Kind,
		Namespace: ns,
		Name:      ref.Name,
	})
	if err != nil || denied != nil {
		return nil, denied, err
	}
	var nc natsv1beta1.NatsConnection
	if err := d.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: ref.Name}, &nc); err != nil {
		return nil, nil, fmt.Errorf("get NatsConnection %s/%s: %w", ns, ref.Name, err)
	}
	conn, err := d.Connection(ctx, &nc)
	return conn, nil, err
}
