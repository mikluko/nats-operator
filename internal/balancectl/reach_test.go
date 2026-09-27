package balancectl

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestStepdownReachProbesUnderCallersContext(t *testing.T) {
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	require.NoError(t, err)
	srv.Start()
	t.Cleanup(srv.Shutdown)
	require.True(t, srv.ReadyForConnections(5*time.Second))
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	r := newStepdownReach(nc)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, ok := r.prefix(cancelled, "A")
	require.False(t, ok)
	require.ErrorIs(t, r.err, context.Canceled)

	_, ok = r.prefix(t.Context(), "B")
	require.False(t, ok, "no responder answers B's stepdown API")
	require.ErrorIs(t, r.err, context.Canceled, "the first failed probe stays the error")

	fresh := newStepdownReach(nc)
	_, ok = fresh.prefix(t.Context(), "B")
	require.False(t, ok)
	require.NoError(t, fresh.err)
}
