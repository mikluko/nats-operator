package natstest

import (
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
)

// TestStartSupercluster pins that a supercluster forms on ports its servers
// bind, and that a server restarted from its rewritten config binds the same
// ports.
func TestStartSupercluster(t *testing.T) {
	c1 := &Cluster{Name: "C1", Size: 3}
	c2 := &Cluster{Name: "C2", Size: 1, Prefix: "x", Configure: func(_ int, o *server.Options) { o.Tags = []string{"az:b"} }}
	StartSupercluster(t, c1, c2)
	require.Len(t, c1.Servers, 3)
	require.Same(t, c2.Servers[0], c2.Server("x0"))
	require.Equal(t, "C1-2", c1.Servers[2].Name())

	x := c1.Servers[0]
	before := x.Bound(t)
	require.NotZero(t, before.Route)
	require.NotZero(t, before.Gateway)
	x.Shutdown()
	x.WaitForShutdown()
	x = x.Restart(t)
	require.Equal(t, before, x.Bound(t))

	y := c2.Servers[0]
	y.Shutdown()
	y.WaitForShutdown()
	y = y.Restart(t)
	v, err := y.Varz(nil)
	require.NoError(t, err)
	require.Equal(t, []string{"az:b"}, []string(v.Tags), "Configure applies on restart")
}
