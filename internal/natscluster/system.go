package natscluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// PoolKind is the natsconn.Key kind of a NatsCluster's system connection.
const PoolKind = "NatsCluster"

// ErrNoSystemUser is returned for a NatsCluster that names no system user
// to connect as: it has no auth plane, or no auth.systemCredentials.
var ErrNoSystemUser = errors.New("no system user: auth.systemCredentials is not set")

// SystemConnections reaches each NATS cluster a NatsCluster deployed as the
// system user its auth.systemCredentials names, over one pooled connection
// per NatsCluster. It observes a NatsCluster without that user through
// Fallback.
type SystemConnections struct {
	Client   client.Reader
	Pool     *natsconn.Pool
	Fallback Observer
	// Servers returns the URLs nc's servers are dialed at; nil dials nc's
	// client Service.
	Servers func(nc *clusterv1beta1.NatsCluster) []string
	// Wait is the sysobs.Observer wait; zero keeps its default.
	Wait time.Duration
}

// Observe observes nc over $SYS when it names a system user, and through
// Fallback otherwise.
func (s *SystemConnections) Observe(ctx context.Context, nc *clusterv1beta1.NatsCluster) (*sysobs.Snapshot, error) {
	if !hasSystemUser(nc) {
		return s.Fallback.Observe(ctx, nc)
	}
	o, err := s.observer(ctx, nc)
	if err != nil {
		return nil, err
	}
	return o.Observe(ctx)
}

// Reloader is a ReloaderFunc: it reloads nc's servers over $SYS, and
// returns ErrNoSystemUser when nc names no system user.
func (s *SystemConnections) Reloader(ctx context.Context, nc *clusterv1beta1.NatsCluster) (ServerReloader, error) {
	if !hasSystemUser(nc) {
		return nil, ErrNoSystemUser
	}
	return s.observer(ctx, nc)
}

// Admin is an AdminFunc: it evacuates and removes nc's servers over $SYS,
// and returns ErrNoSystemUser when nc names no system user.
func (s *SystemConnections) Admin(ctx context.Context, nc *clusterv1beta1.NatsCluster) (ServerAdmin, error) {
	if !hasSystemUser(nc) {
		return nil, ErrNoSystemUser
	}
	return s.observer(ctx, nc)
}

// Forget closes the connection of the NatsCluster named key.
func (s *SystemConnections) Forget(key types.NamespacedName) {
	s.Pool.Forget(natsconn.Key{Kind: PoolKind, NamespacedName: key})
}

func (s *SystemConnections) observer(ctx context.Context, nc *clusterv1beta1.NatsCluster) (*sysobs.Observer, error) {
	creds, err := natsconn.ReadCredentials(ctx, s.Client, nc.Namespace, nc.Spec.Auth.SystemCredentials)
	if err != nil {
		return nil, fmt.Errorf("read auth.systemCredentials: %w", err)
	}
	servers := []string{fmt.Sprintf("nats://%s.%s.svc:%d", clientServiceName(nc), nc.Namespace, PortClient)}
	if s.Servers != nil {
		servers = s.Servers(nc)
	}
	conn, err := s.Pool.Get(ctx, natsconn.Key{Kind: PoolKind, NamespacedName: client.ObjectKeyFromObject(nc)}, natsconn.Endpoint{Servers: servers, Creds: creds})
	if err != nil {
		return nil, err
	}
	opts := []sysobs.Option{sysobs.WithGateways()}
	if s.Wait > 0 {
		opts = append(opts, sysobs.WithWait(s.Wait))
	}
	return sysobs.New(conn, nc.Name, opts...), nil
}

func hasSystemUser(nc *clusterv1beta1.NatsCluster) bool {
	return nc.Spec.Auth != nil && nc.Spec.Auth.SystemCredentials != nil
}
