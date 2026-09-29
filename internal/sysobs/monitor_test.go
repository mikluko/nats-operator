package sysobs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// startNoAuthCluster starts a NATS cluster with no accounts, no client auth
// and system_account unset, each server with its monitoring port and
// server_metadata rev: <name>.
func startNoAuthCluster(t *testing.T, c *testCluster) (map[string]*testServer, []Endpoint) {
	t.Helper()
	srvs := map[string]*testServer{}
	var eps []Endpoint
	for i := range c.size {
		httpPort := freePort(t)
		conf := fmt.Sprintf(`server_name: %s
listen: 127.0.0.1:%d
http: 127.0.0.1:%d
server_metadata { rev: %s }
jetstream { store_dir: %q }
cluster { name: %s, listen: 127.0.0.1:%d, routes: [%s] }
`, c.serverName(i), c.clientPort[i], httpPort, c.serverName(i), filepath.Join(t.TempDir(), "js"), c.name, c.routePort[i], urls(c.routePort))
		f := filepath.Join(t.TempDir(), "s.conf")
		require.NoError(t, os.WriteFile(f, []byte(conf), 0o600))
		srvs[c.serverName(i)] = startServer(t, f, c.clientPort[i])
		eps = append(eps, Endpoint{Name: c.serverName(i), URL: fmt.Sprintf("http://127.0.0.1:%d", httpPort)})
	}
	for name, s := range srvs {
		require.True(t, s.ReadyForConnections(15*time.Second), "%s not ready", name)
	}
	return srvs, eps
}

// TestNoAuthClusterHidesSystemAccount pins why a NATS cluster without an
// auth plane is observed over HTTP: a client lands in the global account and
// the system API has no responder there.
func TestNoAuthClusterHidesSystemAccount(t *testing.T) {
	c := newTestCluster(t, "N", 1)
	_, _ = startNoAuthCluster(t, c)
	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", c.clientPort[0]))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	_, err = nc.Request(subjPingJsz, nil, time.Second)
	require.True(t, errors.Is(err, nats.ErrNoResponders), "got %v", err)
}

func TestMonitorObserve(t *testing.T) {
	c := newTestCluster(t, "N", 3)
	srvs, eps := startNoAuthCluster(t, c)

	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", c.clientPort[0]))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := js.AddStream(&nats.StreamConfig{Name: "R3", Subjects: []string{"r3"}, Replicas: 3, Placement: &nats.Placement{Cluster: "N"}})
		return err == nil
	}, 30*time.Second, 200*time.Millisecond)
	_, err = js.AddConsumer("R3", &nats.ConsumerConfig{Durable: "D", AckPolicy: nats.AckExplicitPolicy})
	require.NoError(t, err)

	o := NewMonitor(http.DefaultClient, 0)
	var snap *Snapshot
	require.Eventually(t, func() bool {
		s, err := o.Observe(context.Background(), eps)
		if err != nil || len(s.Groups) != 3 || !s.Verdict().Settled() {
			return false
		}
		snap = s
		return true
	}, 30*time.Second, 200*time.Millisecond)
	require.Equal(t, []string{"meta://", "stream:$G/R3/", "consumer:$G/R3/D"}, groupNames(snap))
	require.Equal(t, &Placement{Cluster: "N"}, findGroup(t, snap, KindStream, "R3", "").Placement)
	require.Equal(t, &Placement{Cluster: "N"}, findGroup(t, snap, KindConsumer, "R3", "D").Placement)
	for _, s := range snap.Servers {
		require.Equal(t, srvs[s.Name].ID(), s.ID)
		require.Equal(t, map[string]string{"rev": s.Name}, s.Metadata)
		require.True(t, s.JetStream)
		require.NotEmpty(t, s.Version)
		require.Equal(t, &Gateways{Inbound: map[string]int{}}, s.Gateways)
	}

	t.Run("wrong server_name is silent", func(t *testing.T) {
		swapped := slices.Clone(eps)
		swapped[0].Name = "impostor"
		s, err := o.Observe(context.Background(), swapped)
		require.NoError(t, err)
		require.Equal(t, []string{"impostor"}, s.Silent)
		require.False(t, s.Verdict().Settled())
	})

	down := "N-1"
	if findGroup(t, snap, KindStream, "R3", "").Leader == down {
		down = "N-2"
	}
	srvs[down].Shutdown()
	require.Eventually(t, func() bool {
		s, err := o.Observe(context.Background(), eps)
		if err != nil {
			return false
		}
		v := s.Verdict()
		i := slices.IndexFunc(v.Unsettled, func(u Unsettled) bool {
			return u.Group.Kind == KindStream && u.Group.Stream == "R3"
		})
		return slices.Equal(v.Silent, []string{down}) && i >= 0 && v.Unsettled[i].Reason == ReasonMemberOffline
	}, 30*time.Second, 200*time.Millisecond)

	t.Run("no server answers", func(t *testing.T) {
		_, err := o.Observe(context.Background(), []Endpoint{{Name: down, URL: eps[slices.IndexFunc(eps, func(e Endpoint) bool { return e.Name == down })].URL}})
		require.ErrorIs(t, err, ErrNoServers)
	})
}
