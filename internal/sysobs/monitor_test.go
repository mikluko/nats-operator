package sysobs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/natstest"
)

// startNoAuthCluster starts a NATS cluster with no accounts, no client auth
// and system_account unset, each server with its monitoring port and
// server_metadata rev: <name>.
func startNoAuthCluster(t *testing.T, name string, size int) (*natstest.Cluster, []Endpoint) {
	t.Helper()
	c := &natstest.Cluster{Name: name, Size: size}
	c.Config = func(i int) string {
		return fmt.Sprintf("http: %q\nserver_metadata { rev: %s }\n", natstest.Listen(0), c.ServerName(i))
	}
	natstest.StartSupercluster(t, c)
	var eps []Endpoint
	for i, s := range c.Servers {
		eps = append(eps, Endpoint{Name: c.ServerName(i), URL: "http://" + s.MonitorAddr().String()})
	}
	return c, eps
}

// TestNoAuthClusterHidesSystemAccount pins why a NATS cluster without an
// auth plane is observed over HTTP: a client lands in the global account and
// the system API has no responder there.
func TestNoAuthClusterHidesSystemAccount(t *testing.T) {
	c, _ := startNoAuthCluster(t, "N", 1)
	nc, err := nats.Connect(c.Servers[0].ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	_, err = nc.Request(subjPingJsz, nil, time.Second)
	require.True(t, errors.Is(err, nats.ErrNoResponders), "got %v", err)
}

func TestMonitorObserve(t *testing.T) {
	c, eps := startNoAuthCluster(t, "N", 3)

	nc, err := nats.Connect(c.Servers[0].ClientURL())
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
		require.Equal(t, c.Server(s.Name).ID(), s.ID)
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
	c.Server(down).Shutdown()
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
