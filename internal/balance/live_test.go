package balance

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// globalAccount is the account every client of a server with no auth plane is in.
const globalAccount = "$G"

func accountObserver(js jetstream.JetStream, account string, expect ...string) AccountObserver {
	return AccountObserver{JS: js, Account: account, Cluster: testCluster, Expect: expect}
}

func TestStepdown_AccountConnectionMovesAStreamAndAConsumer(t *testing.T) {
	t.Parallel()
	servers := startCluster(t, nil)
	nc := connect(t, servers[0], nil)
	js := jetStream(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "ticks", Subjects: []string{"ticks.>"}, Replicas: 3})
	require.NoError(t, err)
	_, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "d", AckPolicy: jetstream.AckExplicitPolicy})
	require.NoError(t, err)
	obs := accountObserver(js, globalAccount, "ticks")
	mover := Stepdown{Conn: nc}
	require.True(t, mover.CanMove(t.Context(), globalAccount))

	for _, name := range []string{"$G/ticks", "$G/ticks > d"} {
		o := settled(t, ctx, obs, func(o Observation) bool { _, ok := find(o, name); return ok }, "the NATS cluster did not settle")
		g, _ := find(o, name)
		require.Len(t, g.Members, 2)
		to := g.Members[0].Name
		require.NoError(t, mover.MoveLeader(ctx, LeaderMove{Group: g, To: to}))
		settled(t, ctx, obs, ledBy(name, to), fmt.Sprintf("%s did not move to %s", name, to))
	}

	g, _ := find(settled(t, ctx, obs, func(Observation) bool { return true }, "settle"), "$G/ticks")
	require.Error(t, mover.MoveLeader(ctx, LeaderMove{Group: g, To: g.Leader}), "a leader asked to step down for itself")
	require.Error(t, mover.MoveLeader(ctx, LeaderMove{Group: g, To: "n9"}), "a server that is no member")
}

func TestStreamMove_AccountConnectionMovesItsOwnStreamWithItsData(t *testing.T) {
	t.Parallel()
	servers := startCluster(t, nil)
	nc := connect(t, servers[0], nil)
	js := jetStream(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "one", Subjects: []string{"one.>"}, Replicas: 1})
	require.NoError(t, err)
	fill(t, ctx, js, "one", 50, 10)
	obs := accountObserver(js, globalAccount, "one")

	g, _ := find(settled(t, ctx, obs, func(o Observation) bool { _, ok := find(o, "$G/one"); return ok }, "settle"), "$G/one")
	from := g.Leader
	require.NoError(t, StreamMove{Conn: nc}.MovePlacement(ctx, PlacementMove{Group: g, From: from, Cluster: testCluster}))
	settled(t, ctx, obs, func(o Observation) bool {
		g, ok := find(o, "$G/one")
		return ok && len(g.Holders()) == 1 && g.Leader != from
	}, "the stream did not leave "+from)
	intact(t, ctx, js, "one", 50, 10)

	err = StreamMove{Conn: nc}.MovePlacement(ctx, PlacementMove{Group: g, From: from, Cluster: testCluster})
	require.Error(t, err, "a server that no longer holds the stream is refused")
}

func TestSystemMovers_ReachAnotherAccountThroughItsExport(t *testing.T) {
	t.Parallel()
	p := newPlane(t)
	servers := startCluster(t, p)
	sys := connect(t, servers[0], p.sysUser)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	jsA := jetStream(t, connect(t, servers[1], p.aUser))
	jsB := jetStream(t, connect(t, servers[2], p.bUser))
	for _, js := range []jetstream.JetStream{jsA, jsB} {
		require.Eventually(t, func() bool {
			_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "ticks", Subjects: []string{"ticks.>"}, Replicas: 3})
			return err == nil
		}, 30*time.Second, 200*time.Millisecond, "the account got no JetStream")
	}
	_, err := jsA.CreateStream(ctx, jetstream.StreamConfig{Name: "one", Subjects: []string{"one.>"}, Replicas: 1})
	require.NoError(t, err)
	fill(t, ctx, jsA, "one", 50, 10)
	fill(t, ctx, jsA, "ticks", 5, 5)

	leaders := Stepdown{Conn: sys, Prefix: p.prefix}
	require.True(t, leaders.CanMove(t.Context(), p.a.pub))
	require.False(t, leaders.CanMove(t.Context(), p.b.pub), "B carries no stepdown export")

	obsA := accountObserver(jsA, p.a.pub, "ticks", "one")
	ticks, consumer := StreamID{p.a.pub, "ticks"}.String(), StreamID{p.a.pub, "ticks"}.String()+" > d"
	for _, name := range []string{ticks, consumer} {
		o := settled(t, ctx, obsA, func(o Observation) bool { _, ok := find(o, name); return ok }, "the NATS cluster did not settle")
		g, _ := find(o, name)
		to := g.Members[0].Name
		require.NoError(t, leaders.MoveLeader(ctx, LeaderMove{Group: g, To: to}))
		settled(t, ctx, obsA, ledBy(name, to), fmt.Sprintf("%s did not move to %s", name, to))
	}

	obsB := accountObserver(jsB, p.b.pub, "ticks")
	gB, _ := find(settled(t, ctx, obsB, func(o Observation) bool { return len(o.Groups) == 1 }, "B did not settle"), StreamID{p.b.pub, "ticks"}.String())
	require.Error(t, leaders.MoveLeader(ctx, LeaderMove{Group: gB, To: gB.Members[0].Name}))
	require.Error(t, Stepdown{Conn: sys}.MoveLeader(ctx, LeaderMove{Group: gB, To: gB.Members[0].Name}),
		"the system account reaches no account's stepdown API unexported")

	one := StreamID{p.a.pub, "one"}.String()
	g, _ := find(settled(t, ctx, obsA, func(o Observation) bool { _, ok := find(o, one); return ok }, "settle"), one)
	from := g.Leader
	require.NoError(t, StreamMove{Conn: sys}.MovePlacement(ctx, PlacementMove{Group: g, From: from, Cluster: testCluster}))
	settled(t, ctx, obsA, func(o Observation) bool {
		g, ok := find(o, one)
		return ok && len(g.Holders()) == 1 && g.Leader != from
	}, "the stream did not leave "+from)
	intact(t, ctx, jsA, "one", 50, 10)
}

func TestBalancer_SystemBalancerEvensOutAnAccountItReaches(t *testing.T) {
	t.Parallel()
	p := newPlane(t)
	servers := startCluster(t, p)
	sys := connect(t, servers[0], p.sysUser)
	ncA := connect(t, servers[1], p.aUser)
	jsA := jetStream(t, ncA)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var names []string
	for i := range 6 {
		name := fmt.Sprintf("req_%d", i)
		names = append(names, name)
		require.Eventually(t, func() bool {
			_, err := jsA.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}, Replicas: 3})
			return err == nil
		}, 30*time.Second, 200*time.Millisecond)
	}
	obs := accountObserver(jsA, p.a.pub, names...)
	steer := Stepdown{Conn: ncA}
	for _, g := range settled(t, ctx, obs, func(Observation) bool { return true }, "settle").Groups {
		if g.Leader != "n0" {
			require.NoError(t, steer.MoveLeader(ctx, LeaderMove{Group: g, To: "n0"}))
			settled(t, ctx, obs, ledBy(g.String(), "n0"), g.String()+" did not move to n0")
		}
	}

	k := &Balancer{Observer: obs, Leaders: Stepdown{Conn: sys, Prefix: p.prefix}, Placement: StreamMove{Conn: sys}}
	var got Passed
	require.Eventually(t, func() bool {
		var err error
		got, err = k.Pass(ctx)
		require.NoError(t, err)
		return got.Held == "" && got.Moved == nil && got.Placed == nil
	}, 2*time.Minute, 300*time.Millisecond, "six leaders on one of three servers did not come to rest")
	require.Equal(t, []PoolReport{{Name: DefaultPool, Streams: 6}}, got.Pools)
	for _, l := range got.Servers {
		require.Equal(t, 2, l.Leaders, l.Server)
	}
	require.Empty(t, got.Unreachable)
}

func TestBalancer_HoldsWhileAServerIsDown(t *testing.T) {
	t.Parallel()
	servers := startCluster(t, nil)
	js := jetStream(t, connect(t, servers[0], nil))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "ticks", Subjects: []string{"ticks.>"}, Replicas: 3})
	require.NoError(t, err)
	k := &Balancer{Observer: accountObserver(js, globalAccount, "ticks"), DryRun: true}
	require.Eventually(t, func() bool {
		got, err := k.Pass(ctx)
		return err == nil && got.Held == ""
	}, 30*time.Second, 200*time.Millisecond, "the NATS cluster did not settle")

	servers[2].Shutdown()
	require.Eventually(t, func() bool {
		got, err := k.Pass(ctx)
		return err != nil || got.Held != ""
	}, 30*time.Second, 200*time.Millisecond, "a server down left the balancer free to move")
	require.Never(t, func() bool {
		got, err := k.Pass(ctx)
		return err == nil && got.Held == ""
	}, 5*time.Second, 200*time.Millisecond, "two of three servers read as Settled")
}

func TestBalancer_HoldsWhileAMemberIsBehind(t *testing.T) {
	t.Parallel()
	servers := startCluster(t, nil)
	js := jetStream(t, connect(t, servers[0], nil))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "ticks", Subjects: []string{"ticks.>"}, Replicas: 3})
	require.NoError(t, err)
	o := settled(t, ctx, accountObserver(js, globalAccount, "ticks"), func(o Observation) bool { _, ok := find(o, "$G/ticks"); return ok }, "settle")
	ticks, _ := find(o, "$G/ticks")
	behind := ticks.Members[0].Name
	i := slices.IndexFunc(servers, func(s *server.Server) bool { return s.Name() == behind })

	require.NoError(t, servers[i].DisableJetStream(), "the server stays up, its replica stops taking entries")
	for range 20 {
		_, err := js.Publish(ctx, "ticks.x", []byte("x"))
		require.NoError(t, err)
	}
	k := &Balancer{Observer: accountObserver(jetStream(t, connect(t, servers[(i+1)%len(servers)], nil)), globalAccount, "ticks"), DryRun: true}
	want := fmt.Sprintf("%s is 20 behind for $G/ticks", behind)
	require.Eventually(t, func() bool {
		got, err := k.Pass(ctx)
		return err == nil && got.Held == want
	}, 30*time.Second, 200*time.Millisecond, "a replica 20 entries behind left the balancer free to move")
	require.Never(t, func() bool {
		got, err := k.Pass(ctx)
		return err == nil && got.Held == ""
	}, 5*time.Second, 200*time.Millisecond, "a replica behind read as Settled")
}

func TestAccountObserver_HoldsRatherThanFailsOnAnOfflineStream(t *testing.T) {
	t.Parallel()
	servers := startCluster(t, nil)
	js := jetStream(t, connect(t, servers[0], nil))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "lone", Subjects: []string{"lone.>"}, Replicas: 1})
	require.NoError(t, err)
	o := settled(t, ctx, accountObserver(js, globalAccount, "lone"), func(o Observation) bool { _, ok := find(o, "$G/lone"); return ok }, "settle")
	lone, _ := find(o, "$G/lone")

	gone := slices.IndexFunc(servers, func(s *server.Server) bool { return s.Name() == lone.Leader })
	stays := (gone + 1) % len(servers)
	obs := accountObserver(jetStream(t, connect(t, servers[stays], nil)), globalAccount, "lone")
	servers[gone].Shutdown()

	require.Never(t, func() bool {
		_, err := obs.Observe(ctx)
		return err != nil
	}, 10*time.Second, 500*time.Millisecond, "an offline stream failed the read")
	o, err = obs.Observe(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, o.Unsettled)
}
