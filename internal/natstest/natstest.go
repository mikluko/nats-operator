// Package natstest starts in-process nats-server clusters and superclusters
// for tests, on loopback ports each server binds itself.
package natstest

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
)

// Unroutable is the route or gateway URL a server names while no peer has
// bound its port: nats-server refuses to start clustered JetStream without
// a configured route, and nothing listens on port 1.
const Unroutable = "nats://127.0.0.1:1"

// Listen is the loopback address to listen on port, one the server picks
// where port is zero.
func Listen(port int) string {
	if port == 0 {
		port = server.RANDOM_PORT
	}
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// URL is the NATS URL of the loopback port.
func URL(port int) string { return fmt.Sprintf("nats://127.0.0.1:%d", port) }

// Ports are the ports a server has bound; a listener it does not have is
// zero.
type Ports struct {
	Client, Route, Gateway, Leafnode, Monitor int
}

// Server is a nats-server started from a config file.
type Server struct {
	*server.Server
	// Conf is the config file the server started from.
	Conf      string
	configure []func(*server.Options)
}

// Start starts a server from the config file conf, with each of configure
// applied to the parsed options, and returns once it has bound every
// listener conf names. The server shuts down when t ends.
func Start(t testing.TB, conf string, configure ...func(*server.Options)) *Server {
	t.Helper()
	o, err := server.ProcessConfigFile(conf)
	require.NoError(t, err)
	o.NoLog, o.NoSigs = true, true
	for _, f := range configure {
		f(o)
	}
	monitored := o.HTTPPort != 0
	s, err := server.NewServer(o)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	require.True(t, s.ReadyForConnections(15*time.Second), "%s did not start", conf)
	if monitored {
		require.Eventually(t, func() bool { return s.MonitorAddr() != nil }, 15*time.Second, 10*time.Millisecond, "%s is not monitored", conf)
	}
	return &Server{Server: s, Conf: conf, configure: configure}
}

// Restart starts a new server from s's config file with s's option
// adjustments; s must have shut down.
func (s *Server) Restart(t testing.TB) *Server {
	t.Helper()
	return Start(t, s.Conf, s.configure...)
}

// Bound reads back the ports s has bound.
func (s *Server) Bound(t testing.TB) Ports {
	t.Helper()
	var p Ports
	if a, ok := s.Addr().(*net.TCPAddr); ok {
		p.Client = a.Port
	}
	if a := s.ClusterAddr(); a != nil {
		p.Route = a.Port
	}
	if a := s.GatewayAddr(); a != nil {
		p.Gateway = a.Port
	}
	if a := s.MonitorAddr(); a != nil {
		p.Monitor = a.Port
	}
	v, err := s.Varz(nil)
	require.NoError(t, err)
	p.Leafnode = max(v.LeafNode.Port, 0)
	return p
}

// A Cluster is a NATS cluster of Size JetStream servers for
// [StartSupercluster].
type Cluster struct {
	Name string
	Size int
	// Prefix names server i Prefix followed by i; Name + "-" where empty.
	Prefix string
	// Config, where set, is appended verbatim to server i's config.
	Config func(i int) string
	// Configure, where set, adjusts server i's parsed options on every start.
	Configure func(i int, o *server.Options)

	// Servers are the running servers, filled by [StartSupercluster].
	Servers []*Server

	ports    []Ports
	storeDir []string
}

// ServerName is the name of server i of c.
func (c *Cluster) ServerName(i int) string {
	if c.Prefix == "" {
		return fmt.Sprintf("%s-%d", c.Name, i)
	}
	return fmt.Sprintf("%s%d", c.Prefix, i)
}

// Server is the running server named name, or nil.
func (c *Cluster) Server(name string) *Server {
	for i, s := range c.Servers {
		if c.ServerName(i) == name {
			return s
		}
	}
	return nil
}

// urls lists the quoted NATS URLs of ports that are bound.
func urls(ports []int) []string {
	var out []string
	for _, p := range ports {
		if p != 0 {
			out = append(out, fmt.Sprintf("%q", URL(p)))
		}
	}
	return out
}

func (c *Cluster) routePorts() []int {
	var out []int
	for _, p := range c.ports {
		out = append(out, p.Route)
	}
	return out
}

func (c *Cluster) gatewayPorts() []int {
	var out []int
	for _, p := range c.ports {
		out = append(out, p.Gateway)
	}
	return out
}

// conf renders server i of c, routed to the servers of c already bound and,
// where peers is not empty, gatewayed to those of peers already bound.
func (c *Cluster) conf(i int, peers []*Cluster) string {
	var b strings.Builder
	p := c.ports[i]
	fmt.Fprintf(&b, "server_name: %s\nlisten: %q\n", c.ServerName(i), Listen(p.Client))
	fmt.Fprintf(&b, "jetstream { store_dir: %q }\n", c.storeDir[i])
	routes := urls(c.routePorts())
	if len(routes) == 0 {
		routes = []string{fmt.Sprintf("%q", Unroutable)}
	}
	fmt.Fprintf(&b, "cluster { name: %s, listen: %q, routes: [%s] }\n", c.Name, Listen(p.Route), strings.Join(routes, ", "))
	if len(peers) > 0 {
		var gws []string
		for _, peer := range peers {
			if u := urls(peer.gatewayPorts()); len(u) > 0 {
				gws = append(gws, fmt.Sprintf("{name: %s, urls: [%s]}", peer.Name, strings.Join(u, ", ")))
			}
		}
		fmt.Fprintf(&b, "gateway { name: %s, listen: %q, gateways: [%s] }\n", c.Name, Listen(p.Gateway), strings.Join(gws, ", "))
	}
	if c.Config != nil {
		b.WriteString(c.Config(i))
	}
	return b.String()
}

// StartSupercluster starts every server of every cluster, each gatewayed to
// all the others when there is more than one, on ports the servers bind
// themselves. It returns once each server is routed to its cluster's other
// servers and gatewayed to every other cluster and, where there is more
// than one server, a JetStream meta leader counts every server as a peer.
// Each server's config file is rewritten with every port bound.
func StartSupercluster(t testing.TB, clusters ...*Cluster) {
	t.Helper()
	var peers []*Cluster
	if len(clusters) > 1 {
		peers = clusters
	}
	var all []*Server
	for _, c := range clusters {
		c.ports, c.storeDir, c.Servers = make([]Ports, c.Size), nil, nil
		for range c.Size {
			c.storeDir = append(c.storeDir, filepath.Join(t.TempDir(), "js"))
		}
	}
	for _, c := range clusters {
		for i := range c.Size {
			f := filepath.Join(t.TempDir(), "nats.conf")
			require.NoError(t, os.WriteFile(f, []byte(c.conf(i, peers)), 0o600))
			var configure []func(*server.Options)
			if c.Configure != nil {
				configure = append(configure, func(o *server.Options) { c.Configure(i, o) })
			}
			s := Start(t, f, configure...)
			c.ports[i] = s.Bound(t)
			c.Servers = append(c.Servers, s)
			all = append(all, s)
		}
	}
	for _, c := range clusters {
		for i, s := range c.Servers {
			require.Eventually(t, func() bool {
				return s.NumRemotes() == c.Size-1 && s.NumOutboundGateways() == max(len(peers)-1, 0)
			}, 30*time.Second, 50*time.Millisecond, "%s has not joined the supercluster", c.ServerName(i))
			require.NoError(t, os.WriteFile(s.Conf, []byte(c.conf(i, peers)), 0o600))
		}
	}
	if len(all) == 1 {
		return
	}
	require.Eventually(t, func() bool {
		for _, s := range all {
			if s.JetStreamIsLeader() {
				return len(s.JetStreamClusterPeers()) == len(all)
			}
		}
		return false
	}, time.Minute, 100*time.Millisecond, "no JetStream meta leader counts all %d servers", len(all))
}
