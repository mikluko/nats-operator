package balancectl

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// TestNewPool_JetStreamControllerUser pins that a NatsConnection dialed
// from NewPool takes replies as a jetstream-controller preset user of the
// system account.
func TestNewPool_JetStreamControllerUser(t *testing.T) {
	p := newPlane(t)
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true}
	p.configure(t, opts)
	srv, err := server.NewServer(opts)
	require.NoError(t, err)
	go srv.Start()
	t.Cleanup(srv.Shutdown)
	require.True(t, srv.ReadyForConnections(10*time.Second))

	objs := connection("sys", srv.ClientURL(), p.sysCreds)
	pool := NewPool()
	t.Cleanup(pool.Close)
	d := &natsconn.Dialer{Reader: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).Build(), Pool: pool}
	nc, err := d.Connection(t.Context(), objs[1].(*natsv1beta1.NatsConnection))
	require.NoError(t, err)
	_, err = nc.Request("$SYS.REQ.SERVER.PING.STATSZ", nil, 2*time.Second)
	require.NoError(t, err)
}
