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

type testCluster struct {
	name       string
	size       int
	clientPort []int
	routePort  []int
	gwPort     []int
}

type testServer struct {
	*server.Server
	conf string
	port int
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, l.Close()) }()
	return l.Addr().(*net.TCPAddr).Port
}

func newTestCluster(t *testing.T, name string, size int) *testCluster {
	c := &testCluster{name: name, size: size}
	for range size {
		c.clientPort = append(c.clientPort, freePort(t))
		c.routePort = append(c.routePort, freePort(t))
		c.gwPort = append(c.gwPort, freePort(t))
	}
	return c
}

func (c *testCluster) serverName(i int) string { return fmt.Sprintf("%s-%d", c.name, i) }

func urls(ports []int) string {
	var us []string
	for _, p := range ports {
		us = append(us, fmt.Sprintf("%q", fmt.Sprintf("nats://127.0.0.1:%d", p)))
	}
	return strings.Join(us, ", ")
}

// conf renders server i of c; extra is appended verbatim.
func (c *testCluster) conf(t *testing.T, i int, peers []*testCluster, extra string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "server_name: %s\nlisten: 127.0.0.1:%d\n", c.serverName(i), c.clientPort[i])
	fmt.Fprintf(&b, "jetstream { store_dir: %q }\n", filepath.Join(t.TempDir(), "js"))
	fmt.Fprintf(&b, "cluster { name: %s, listen: 127.0.0.1:%d, routes: [%s] }\n", c.name, c.routePort[i], urls(c.routePort))
	if len(peers) > 0 {
		var gws []string
		for _, p := range peers {
			gws = append(gws, fmt.Sprintf("{name: %s, urls: [%s]}", p.name, urls(p.gwPort)))
		}
		fmt.Fprintf(&b, "gateway { name: %s, listen: 127.0.0.1:%d, gateways: [%s] }\n", c.name, c.gwPort[i], strings.Join(gws, ", "))
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
// gatewayed to all the others when there is more than one.
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
			require.NoError(t, os.WriteFile(f, []byte(c.conf(t, i, peers, "")), 0o600))
			out[c.serverName(i)] = startServer(t, f, c.clientPort[i])
		}
	}
	for name, s := range out {
		require.True(t, s.ReadyForConnections(15*time.Second), "%s not ready", name)
	}
	return out
}

func startServer(t *testing.T, conf string, port int) *testServer {
	t.Helper()
	o, err := server.ProcessConfigFile(conf)
	require.NoError(t, err)
	o.NoLog, o.NoSigs = true, true
	s, err := server.NewServer(o)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	return &testServer{Server: s, conf: conf, port: port}
}

func connect(t *testing.T, port int, user string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%s@127.0.0.1:%d", user, user, port))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}
