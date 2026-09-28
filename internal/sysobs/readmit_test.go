package sysobs

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestObserve_RemoteMetaLeaderListsItsPeers pins that with the meta leader
// in another NATS cluster of the supercluster, the meta group lists the
// servers the leader counts as peers: a server removed from the meta group
// and restarted under its own name on an empty store answers and names the
// leader, yet is absent until its tombstone lapses.
func TestObserve_RemoteMetaLeaderListsItsPeers(t *testing.T) {
	c1 := newTestCluster(t, "C1", 3)
	c2 := newTestCluster(t, "C2", 3)
	srvs := startSupercluster(t, c1, c2)
	ctx := context.Background()
	o := New(connect(t, c1.clientPort[0], "sys"), "C1")

	require.Eventually(t, func() bool {
		for name, s := range srvs {
			if s.JetStreamIsLeader() {
				if strings.HasPrefix(name, "C2-") {
					return true
				}
				_ = o.StepDownMeta(ctx)
			}
		}
		return false
	}, 60*time.Second, 500*time.Millisecond, "the meta leader never moved to C2")
	meta := func(t *testing.T) Group {
		t.Helper()
		snap, err := o.Observe(ctx)
		require.NoError(t, err)
		require.Equal(t, KindMeta, snap.Groups[0].Kind)
		return snap.Groups[0]
	}
	require.Eventually(t, func() bool {
		g := meta(t)
		return strings.HasPrefix(g.Leader, "C2-") && !g.FromFollowers && len(g.Members) == 3
	}, 30*time.Second, 200*time.Millisecond, "C1's meta group not read from the leader in C2")

	x := srvs["C1-2"]
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assert.NoError(ct, o.RemovePeer(ctx, "C1-2"))
	}, 30*time.Second, 500*time.Millisecond, "C1-2 not removed from the meta group")
	x.Shutdown()
	x.WaitForShutdown()
	opts, err := server.ProcessConfigFile(x.conf)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(opts.StoreDir))
	x = startServer(t, x.conf, x.port)
	require.True(t, x.ReadyForConnections(15*time.Second))
	require.Eventually(t, func() bool {
		jsz, err := x.Jsz(&server.JSzOptions{})
		return err == nil && jsz.Meta != nil && strings.HasPrefix(jsz.Meta.Leader, "C2-")
	}, 30*time.Second, 200*time.Millisecond, "the restarted server does not name the leader")

	g := meta(t)
	require.False(t, g.FromFollowers)
	require.Equal(t, []string{"C1-0", "C1-1"}, memberNames(g))
}
