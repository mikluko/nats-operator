package sysobs

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// testCluster is a NATS cluster of size servers; a port is zero until its
// server has started and bound it.
type testCluster struct {
	name       string
	size       int
	clientPort []int
	routePort  []int
	gwPort     []int
	storeDir   []string
}

type testServer struct {
	*server.Server
	conf string
}

func newTestCluster(t *testing.T, name string, size int) *testCluster {
	c := &testCluster{name: name, size: size, clientPort: make([]int, size), routePort: make([]int, size), gwPort: make([]int, size)}
	for range size {
		c.storeDir = append(c.storeDir, filepath.Join(t.TempDir(), "js"))
	}
	return c
}

func (c *testCluster) serverName(i int) string { return fmt.Sprintf("%s-%d", c.name, i) }

// peered waits for server i of c to bind its route port, and its gateway
// port where gatewayed, and records them.
func (c *testCluster) peered(t *testing.T, i int, s *testServer, gatewayed bool) {
	t.Helper()
	require.Eventually(t, func() bool { return s.ClusterAddr() != nil && (!gatewayed || s.GatewayAddr() != nil) },
		15*time.Second, 10*time.Millisecond, "%s is not listening for peers", c.serverName(i))
	c.routePort[i] = s.ClusterAddr().Port
	if gatewayed {
		c.gwPort[i] = s.GatewayAddr().Port
	}
}

// ready waits for server i of c to accept connections and records its
// client port.
func (c *testCluster) ready(t *testing.T, i int, s *testServer) {
	t.Helper()
	require.True(t, s.ReadyForConnections(15*time.Second), "%s not ready", c.serverName(i))
	c.clientPort[i] = s.Addr().(*net.TCPAddr).Port
}

// urls lists the NATS URLs of the ports that are bound.
func urls(ports []int) string {
	var us []string
	for _, p := range ports {
		if p != 0 {
			us = append(us, fmt.Sprintf("%q", fmt.Sprintf("nats://127.0.0.1:%d", p)))
		}
	}
	return strings.Join(us, ", ")
}

// unroutable is the route a server names while no peer has bound its
// route port: nats-server refuses to start clustered JetStream without a
// configured route, and nothing listens on port 1.
const unroutable = `"nats://127.0.0.1:1"`

// routes lists the NATS URLs of the route ports that are bound, or
// unroutable where none is.
func routes(ports []int) string {
	if u := urls(ports); u != "" {
		return u
	}
	return unroutable
}

// listen returns the address to listen on port, one the server picks where
// port is zero.
func listen(port int) string {
	if port == 0 {
		port = server.RANDOM_PORT
	}
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// conf renders server i of c, routed and gatewayed to the servers already
// bound; extra is appended verbatim.
func (c *testCluster) conf(i int, peers []*testCluster, extra string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "server_name: %s\nlisten: %q\n", c.serverName(i), listen(c.clientPort[i]))
	fmt.Fprintf(&b, "jetstream { store_dir: %q }\n", c.storeDir[i])
	fmt.Fprintf(&b, "cluster { name: %s, listen: %q, routes: [%s] }\n", c.name, listen(c.routePort[i]), routes(c.routePort))
	if len(peers) > 0 {
		var gws []string
		for _, p := range peers {
			if u := urls(p.gwPort); u != "" {
				gws = append(gws, fmt.Sprintf("{name: %s, urls: [%s]}", p.name, u))
			}
		}
		fmt.Fprintf(&b, "gateway { name: %s, listen: %q, gateways: [%s] }\n", c.name, listen(c.gwPort[i]), strings.Join(gws, ", "))
	}
	b.WriteString(`accounts {
  A: { jetstream: enabled, users: [{user: a, password: a}] }
  SYS: { users: [{user: sys, password: sys}] }
}
system_account: SYS
`)
	b.WriteString(extra)
	return b.String()
}

// startSupercluster starts every server of every NATS cluster, each
// gatewayed to all the others when there is more than one, on ports the
// servers pick, and returns once each is routed to its NATS cluster's other
// servers and gatewayed to every other NATS cluster. It rewrites each
// server's configuration file with every port bound, so a server restarted
// from it binds its own ports again.
func startSupercluster(t *testing.T, clusters ...*testCluster) map[string]*testServer {
	t.Helper()
	var peers []*testCluster
	if len(clusters) > 1 {
		peers = clusters
	}
	out := map[string]*testServer{}
	for _, c := range clusters {
		for i := range c.size {
			f := filepath.Join(t.TempDir(), "s.conf")
			require.NoError(t, os.WriteFile(f, []byte(c.conf(i, peers, "")), 0o600))
			s := startServer(t, f)
			c.peered(t, i, s, peers != nil)
			out[c.serverName(i)] = s
		}
	}
	for _, c := range clusters {
		for i := range c.size {
			s := out[c.serverName(i)]
			c.ready(t, i, s)
			require.Eventually(t, func() bool {
				return s.NumRemotes() == c.size-1 && s.NumOutboundGateways() == max(len(peers)-1, 0)
			}, 30*time.Second, 50*time.Millisecond, "%s has not joined the supercluster", c.serverName(i))
			require.NoError(t, os.WriteFile(s.conf, []byte(c.conf(i, peers, "")), 0o600))
		}
	}
	return out
}

func startServer(t *testing.T, conf string) *testServer {
	t.Helper()
	o, err := server.ProcessConfigFile(conf)
	require.NoError(t, err)
	o.NoLog, o.NoSigs = true, true
	s, err := server.NewServer(o)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	return &testServer{Server: s, conf: conf}
}

func connect(t *testing.T, port int, user string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%s@127.0.0.1:%d", user, user, port))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}
