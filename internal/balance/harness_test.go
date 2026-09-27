package balance

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
)

// testCluster is the NATS cluster name every harness server joins.
const testCluster = "test"

// startCluster runs three JetStream servers routed into one NATS cluster,
// n0 to n2, under p's auth plane where p is not nil. A clustered JetStream
// server refuses to start with no route configured, so every route port is
// settled before any server starts.
func startCluster(t *testing.T, p *plane) []*server.Server {
	t.Helper()
	const n = 3
	var routes []*url.URL
	ports := make([]int, n)
	for i := range ports {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		ports[i] = l.Addr().(*net.TCPAddr).Port
		require.NoError(t, l.Close())
		routes = append(routes, &url.URL{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", ports[i])})
	}
	var servers []*server.Server
	for i := range n {
		opts := &server.Options{
			ServerName: fmt.Sprintf("n%d", i),
			Host:       "127.0.0.1",
			Port:       -1,
			NoLog:      true,
			NoSigs:     true,
			JetStream:  true,
			StoreDir:   t.TempDir(),
			Cluster:    server.ClusterOpts{Name: testCluster, Host: "127.0.0.1", Port: ports[i]},
			Routes:     routes,
		}
		if p != nil {
			p.configure(t, opts)
		}
		srv, err := server.NewServer(opts)
		require.NoError(t, err)
		go srv.Start()
		t.Cleanup(srv.Shutdown)
		require.True(t, srv.ReadyForConnections(10*time.Second), "n%d did not start", i)
		servers = append(servers, srv)
	}
	require.Eventually(t, func() bool {
		for _, srv := range servers {
			if srv.JetStreamIsLeader() {
				return len(srv.JetStreamClusterPeers()) == n
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "the NATS cluster elected no meta leader seeing all %d servers", n)
	return servers
}

// connect is a client of srv, as user where it is not nil.
func connect(t *testing.T, srv *server.Server, user *identity) *nats.Conn {
	t.Helper()
	var opts []nats.Option
	if user != nil {
		opts = append(opts, nats.UserJWTAndSeed(user.jwt, string(user.seed)))
	}
	nc, err := nats.Connect(srv.ClientURL(), opts...)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

func jetStream(t *testing.T, nc *nats.Conn) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return js
}

// An identity is a key pair and the JWT that names it.
type identity struct {
	pub  string
	seed []byte
	kp   nkeys.KeyPair
	jwt  string
}

func newIdentity(t *testing.T, create func() (nkeys.KeyPair, error)) *identity {
	t.Helper()
	kp, err := create()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)
	return &identity{pub: pub, seed: seed, kp: kp}
}

// A plane is a NATS operator with a system account and two accounts: A, which
// exports its stepdown API to the system account under [plane.prefix], and B,
// which exports nothing.
type plane struct {
	operator     *jwt.OperatorClaims
	sys, a, b    *identity
	sysUser      *identity
	aUser, bUser *identity
}

// stepdownSubjects is what the jetstream-stepdown export preset exports.
var stepdownSubjects = []string{"$JS.API.STREAM.LEADER.STEPDOWN.*", "$JS.API.CONSUMER.LEADER.STEPDOWN.*.*"}

func newPlane(t *testing.T) *plane {
	t.Helper()
	op := newIdentity(t, nkeys.CreateOperator)
	p := &plane{
		sys: newIdentity(t, nkeys.CreateAccount),
		a:   newIdentity(t, nkeys.CreateAccount),
		b:   newIdentity(t, nkeys.CreateAccount),
	}
	p.operator = jwt.NewOperatorClaims(op.pub)
	p.operator.SystemAccount = p.sys.pub

	sys := jwt.NewAccountClaims(p.sys.pub)
	sys.Name = "SYS"
	limits := jwt.JetStreamLimits{MemoryStorage: -1, DiskStorage: -1, Streams: -1, Consumer: -1}
	a := jwt.NewAccountClaims(p.a.pub)
	a.Name, a.Limits.JetStreamLimits = "A", limits
	b := jwt.NewAccountClaims(p.b.pub)
	b.Name, b.Limits.JetStreamLimits = "B", limits
	for _, subject := range stepdownSubjects {
		a.Exports.Add(&jwt.Export{Subject: jwt.Subject(subject), Type: jwt.Service})
		sys.Imports.Add(&jwt.Import{
			Account:      p.a.pub,
			Subject:      jwt.Subject(subject),
			LocalSubject: jwt.RenamingSubject(p.prefixOf(p.a.pub) + subject),
			Type:         jwt.Service,
		})
	}
	var err error
	for _, c := range []struct {
		id     *identity
		claims *jwt.AccountClaims
	}{{p.sys, sys}, {p.a, a}, {p.b, b}} {
		c.id.jwt, err = c.claims.Encode(op.kp)
		require.NoError(t, err)
	}
	p.sysUser, p.aUser, p.bUser = p.user(t, p.sys), p.user(t, p.a), p.user(t, p.b)
	return p
}

func (p *plane) user(t *testing.T, account *identity) *identity {
	t.Helper()
	u := newIdentity(t, nkeys.CreateUser)
	var err error
	u.jwt, err = jwt.NewUserClaims(u.pub).Encode(account.kp)
	require.NoError(t, err)
	return u
}

// prefixOf is the prefix the system account imports account's stepdown API
// under.
func (p *plane) prefixOf(account string) string { return "acc." + account + "." }

// prefix is [Stepdown.Prefix] for a system connection under p.
func (p *plane) prefix(_ context.Context, account string) (string, bool) {
	if account != p.a.pub {
		return "", false
	}
	return p.prefixOf(account), true
}

func (p *plane) configure(t *testing.T, opts *server.Options) {
	t.Helper()
	resolver := &server.MemAccResolver{}
	for _, id := range []*identity{p.sys, p.a, p.b} {
		require.NoError(t, resolver.Store(id.pub, id.jwt))
	}
	opts.TrustedOperators = []*jwt.OperatorClaims{p.operator}
	opts.SystemAccount = p.sys.pub
	opts.AccountResolver = resolver
}

// settled waits for o to read Settled and satisfy want, and returns what it
// read.
func settled(t *testing.T, ctx context.Context, o Observer, want func(Observation) bool, msg string) Observation {
	t.Helper()
	var got Observation
	require.Eventually(t, func() bool {
		var err error
		got, err = o.Observe(ctx)
		return err == nil && got.Unsettled == "" && want(got)
	}, time.Minute, 200*time.Millisecond, msg)
	return got
}

// find is the group of obs named name.
func find(obs Observation, name string) (Group, bool) {
	for _, g := range obs.Groups {
		if g.String() == name {
			return g, true
		}
	}
	return Group{}, false
}

func ledBy(name, server string) func(Observation) bool {
	return func(o Observation) bool { g, ok := find(o, name); return ok && g.Leader == server }
}

// fill publishes n messages to stream's subjects and acknowledges the first
// acked through a durable, so a move has data and consumer state to lose.
func fill(t *testing.T, ctx context.Context, js jetstream.JetStream, stream string, n, acked int) {
	t.Helper()
	for i := range n {
		_, err := js.Publish(ctx, fmt.Sprintf("%s.%d", stream, i), []byte("x"))
		require.NoError(t, err)
	}
	s, err := js.Stream(ctx, stream)
	require.NoError(t, err)
	c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "d", AckPolicy: jetstream.AckExplicitPolicy})
	require.NoError(t, err)
	batch, err := c.Fetch(acked)
	require.NoError(t, err)
	for msg := range batch.Messages() {
		require.NoError(t, msg.DoubleAck(ctx))
	}
	require.NoError(t, batch.Error())
}

// intact requires that stream kept n messages and its durable's acknowledged
// floor at acked.
func intact(t *testing.T, ctx context.Context, js jetstream.JetStream, stream string, n, acked int) {
	t.Helper()
	s, err := js.Stream(ctx, stream)
	require.NoError(t, err)
	info, err := s.Info(ctx)
	require.NoError(t, err)
	require.EqualValues(t, n, info.State.Msgs, "the messages moved with the stream")
	c, err := s.Consumer(ctx, "d")
	require.NoError(t, err)
	cinfo, err := c.Info(ctx)
	require.NoError(t, err)
	require.EqualValues(t, acked, cinfo.AckFloor.Stream, "and so did what the durable acknowledged")
}
