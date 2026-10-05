package natscluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
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

// SystemConnections reaches a NatsCluster's servers as the system user its
// auth.systemCredentials names, and one without that user, or whose
// servers do not all answer it, through Fallback.
type SystemConnections struct {
	Client   client.Reader
	Pool     *natsconn.Pool
	Fallback Observer
	// Servers returns the URLs nc's servers are dialed at; nil dials nc's
	// client Service.
	Servers func(nc *clusterv1beta1.NatsCluster) []string
	// Wait is the sysobs.SystemClient wait; zero keeps its default.
	Wait time.Duration
}

var (
	_ Observer     = (*SystemConnections)(nil)
	_ ReloaderFunc = (*SystemConnections)(nil).Reloader
	_ AdminFunc    = (*SystemConnections)(nil).Admin
)

// Observe observes nc over $SYS when it names a system user and every
// server spec.replicas names answers there, and through Fallback otherwise,
// as while servers still run a config without that user. A $SYS
// observation that fails is masked whenever Fallback succeeds.
func (s *SystemConnections) Observe(ctx context.Context, nc *clusterv1beta1.NatsCluster) (*sysobs.Snapshot, error) {
	if !hasSystemUser(nc) {
		return s.Fallback.Observe(ctx, nc)
	}
	snap, err := s.observeSystem(ctx, nc)
	if err == nil && observesAll(snap, serverNames(nc)) {
		return snap, nil
	}
	fb, fbErr := s.Fallback.Observe(ctx, nc)
	switch {
	case fbErr == nil:
		return fb, nil
	case err == nil:
		return snap, nil
	}
	return nil, fmt.Errorf("%w; through the monitoring port: %w", err, fbErr)
}

func (s *SystemConnections) observeSystem(ctx context.Context, nc *clusterv1beta1.NatsCluster) (*sysobs.Snapshot, error) {
	o, err := s.client(ctx, nc)
	if err != nil {
		return nil, err
	}
	return o.Observe(ctx)
}

// observesAll reports whether every server in servers is in snap's roster
// and not Silent.
func observesAll(snap *sysobs.Snapshot, servers []string) bool {
	for _, name := range servers {
		if slices.Contains(snap.Silent, name) || !slices.ContainsFunc(snap.Servers, func(s sysobs.Server) bool { return s.Name == name }) {
			return false
		}
	}
	return true
}

// Reloader reloads nc's servers over $SYS, and returns ErrNoSystemUser
// when nc names no system user.
func (s *SystemConnections) Reloader(ctx context.Context, nc *clusterv1beta1.NatsCluster) (ServerReloader, error) {
	if !hasSystemUser(nc) {
		return nil, ErrNoSystemUser
	}
	return s.client(ctx, nc)
}

// Admin evacuates and removes nc's servers over $SYS, and returns
// ErrNoSystemUser when nc names no system user.
func (s *SystemConnections) Admin(ctx context.Context, nc *clusterv1beta1.NatsCluster) (ServerAdmin, error) {
	if !hasSystemUser(nc) {
		return nil, ErrNoSystemUser
	}
	return s.client(ctx, nc)
}

// Forget closes the connection of the NatsCluster named key.
func (s *SystemConnections) Forget(key types.NamespacedName) {
	s.Pool.Forget(natsconn.Key{Kind: PoolKind, NamespacedName: key})
}

func (s *SystemConnections) client(ctx context.Context, nc *clusterv1beta1.NatsCluster) (*sysobs.SystemClient, error) {
	creds, err := natsconn.ReadCredentials(ctx, s.Client, nc.Namespace, nc.Spec.Auth.SystemCredentials)
	if err != nil {
		return nil, fmt.Errorf("read auth.systemCredentials: %w", err)
	}
	ep := natsconn.Endpoint{Servers: []string{clientURL(nc)}, Creds: creds}
	if s.Servers != nil {
		ep.Servers = s.Servers(nc)
	}
	if ep.CA, err = s.clientCA(ctx, nc); err != nil {
		return nil, err
	}
	conn, err := s.Pool.Get(ctx, natsconn.Key{Kind: PoolKind, NamespacedName: client.ObjectKeyFromObject(nc)}, ep)
	if err != nil {
		return nil, err
	}
	opts := []sysobs.Option{sysobs.WithGateways()}
	if s.Wait > 0 {
		opts = append(opts, sysobs.WithWait(s.Wait))
	}
	return sysobs.New(conn, nc.Name, opts...), nil
}

// clientCA is the ca.crt of nc's client certificate Secret, nil when nc
// has no client TLS or the Secret holds none.
func (s *SystemConnections) clientCA(ctx context.Context, nc *clusterv1beta1.NatsCluster) ([]byte, error) {
	name := clientCertSecret(nc)
	if name == "" {
		return nil, nil
	}
	secret := &corev1.Secret{}
	if err := s.Client.Get(ctx, client.ObjectKey{Namespace: nc.Namespace, Name: name}, secret); err != nil {
		return nil, fmt.Errorf("read client certificate Secret %s: %w", name, err)
	}
	return secret.Data[caKey], nil
}

func hasSystemUser(nc *clusterv1beta1.NatsCluster) bool {
	return nc.Spec.Auth != nil && nc.Spec.Auth.SystemCredentials != nil
}
