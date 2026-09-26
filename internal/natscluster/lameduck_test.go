package natscluster

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// TestLameDuckFitsTerminationGrace pins that the preStop signals lame-duck
// mode to the rendered pid file, and that the lame-duck mode the rendered
// config sets ends inside the pod's termination grace period, with room
// for the second it waits after stepping leaders down and for JetStream to
// shut down.
func TestLameDuckFitsTerminationGrace(t *testing.T) {
	nc := storyCluster(t)
	nc.Spec.Routes = &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{Enabled: ptr.To(false)}}
	plan, err := Render(nc, nil)
	require.NoError(t, err)
	s := plan.Servers[0]
	pod := s.StatefulSet.Spec.Template.Spec
	require.Equal(t, []string{"/nats-server", "--signal", "ldm=" + pidDir + "/nats.pid"},
		pod.Containers[0].Lifecycle.PreStop.Exec.Command)

	f := filepath.Join(t.TempDir(), configFile)
	require.NoError(t, os.WriteFile(f, []byte(s.ConfigMap.Data[configFile]), 0o600))
	o, err := server.ProcessConfigFile(f)
	require.NoError(t, err)
	require.Equal(t, pidDir+"/nats.pid", o.PidFile)
	require.Equal(t, lameDuckDuration, o.LameDuckDuration)
	require.Equal(t, lameDuckGracePeriod, o.LameDuckGracePeriod)

	const slack = 30 * time.Second
	grace := time.Duration(*pod.TerminationGracePeriodSeconds) * time.Second
	require.Less(t, o.LameDuckDuration+time.Second+slack, grace)
}

// TestLameDuckHandsOffLeaders pins that a server entering lame-duck mode
// hands off the meta, stream and consumer leadership it holds to another
// server before its clients are told and before it exits, which is why a
// rollout moves no leader before a restart.
func TestLameDuckHandsOffLeaders(t *testing.T) {
	nc := storyCluster(t)
	_, srvs, url, _ := startRendered(t, nc, nil, "r1", func(o *server.Options) {
		o.LameDuckDuration, o.LameDuckGracePeriod = 4*time.Second, 2*time.Second
	})
	const account, stream, consumer = "$G", "ORDERS", "C"
	roles := map[string]func(*server.Server) bool{
		"meta":     func(s *server.Server) bool { return s.JetStreamIsLeader() },
		"stream":   func(s *server.Server) bool { return s.JetStreamIsStreamLeader(account, stream) },
		"consumer": func(s *server.Server) bool { return s.JetStreamIsConsumerLeader(account, stream, consumer) },
	}
	leaderOf := func(is func(*server.Server) bool) *server.Server {
		for _, s := range srvs {
			if s.Running() && is(s) {
				return s
			}
		}
		return nil
	}

	conn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	js, err := conn.JetStream()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{"orders.>"}, Replicas: 3})
		return err == nil
	}, 30*time.Second, 200*time.Millisecond)
	_, err = js.AddConsumer(stream, &nats.ConsumerConfig{Durable: consumer, AckPolicy: nats.AckExplicitPolicy, Replicas: 3})
	require.NoError(t, err)

	var x *server.Server
	require.Eventually(t, func() bool { x = leaderOf(roles["meta"]); return x != nil }, 30*time.Second, 50*time.Millisecond)
	preferred := fmt.Appendf(nil, `{"placement":{"preferred":%q}}`, x.Name())
	for _, move := range []struct{ role, subject string }{
		{"stream", "$JS.API.STREAM.LEADER.STEPDOWN." + stream},
		{"consumer", "$JS.API.CONSUMER.LEADER.STEPDOWN." + stream + "." + consumer},
	} {
		require.Eventually(t, func() bool {
			if roles[move.role](x) {
				return true
			}
			_, _ = conn.Request(move.subject, preferred, 2*time.Second)
			return false
		}, 30*time.Second, 500*time.Millisecond, "%s leader not moved onto %s", move.role, x.Name())
	}
	for role, is := range roles {
		require.True(t, is(x), "%s leader left %s", role, x.Name())
	}

	var mu sync.Mutex
	var told time.Time
	onX, err := nats.Connect(x.ClientURL(), nats.NoReconnect(), nats.LameDuckModeHandler(func(*nats.Conn) {
		mu.Lock()
		defer mu.Unlock()
		told = time.Now()
	}))
	require.NoError(t, err)
	t.Cleanup(onX.Close)
	clientTold := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return !told.IsZero()
	}

	go x.LameDuckShutdown()
	handedOff, late := map[string]bool{}, map[string]bool{}
	for deadline := time.Now().Add(5 * time.Second); len(handedOff) < len(roles) && time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		for role, is := range roles {
			if l := leaderOf(is); l != nil && l != x && !handedOff[role] {
				handedOff[role] = true
				late[role] = clientTold() || !x.Running()
			}
		}
	}
	require.Len(t, handedOff, len(roles), "leaders not handed off")
	for role := range roles {
		require.False(t, late[role], "%s handed off after clients were told or %s exited", role, x.Name())
	}
	require.Eventually(t, clientTold, 5*time.Second, 10*time.Millisecond, "clients never told of lame-duck mode")
	require.Eventually(t, func() bool { return !x.Running() }, 10*time.Second, 50*time.Millisecond)
}
