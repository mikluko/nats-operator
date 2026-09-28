package balancectl

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/events"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// A plane is an auth plane signed by jwtplane: account A carries the
// jetstream-stepdown export preset and the system account imports it; B
// carries nothing. sysCreds is a jetstream-controller preset user of the
// system account.
type plane struct {
	operator *jwt.OperatorClaims
	sysPub   string
	sys      jwtplane.Keys
	a, b     jwtplane.Keys
	aPub     string
	bPub     string
	jwts     map[string]string
	sysCreds []byte
}

func newPlane(t *testing.T) *plane {
	t.Helper()
	now := time.Now()
	op := newKeys(t, nkeys.PrefixByteOperator)
	sys := newKeys(t, nkeys.PrefixByteAccount)
	p := &plane{sys: sys, a: newKeys(t, nkeys.PrefixByteAccount), b: newKeys(t, nkeys.PrefixByteAccount), jwts: map[string]string{}}
	p.sysPub, p.aPub, p.bPub = pubOf(t, sys), pubOf(t, p.a), pubOf(t, p.b)

	opJWT, err := jwtplane.SignOperator(jwtplane.Operator{Name: "op", Keys: op, SystemAccount: p.sysPub})
	require.NoError(t, err)
	p.operator, err = jwt.DecodeOperatorClaims(opJWT)
	require.NoError(t, err)
	p.jwts[p.sysPub], err = jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "SYS", Keys: sys, StepdownAccounts: []string{p.aPub}}, op, now)
	require.NoError(t, err)
	stepdown, err := jwtplane.ExportPreset(jwtplane.ExportPresetJetStreamStepdown)
	require.NoError(t, err)
	limits := jwtplane.Limits{JetStream: &jwtplane.JetStreamLimits{}}
	p.jwts[p.aPub], err = jwtplane.SignAccount(jwtplane.Account{Name: "A", Keys: p.a, Limits: limits, Exports: stepdown}, op, now)
	require.NoError(t, err)
	p.jwts[p.bPub], err = jwtplane.SignAccount(jwtplane.Account{Name: "B", Keys: p.b, Limits: limits}, op, now)
	require.NoError(t, err)
	p.sysCreds = creds(t, jwtplane.User{Preset: jwtplane.PresetJetStreamController, SystemAccount: true}, sys)
	return p
}

func newKeys(t *testing.T, kind nkeys.PrefixByte) jwtplane.Keys {
	t.Helper()
	pair := func() nkeys.KeyPair {
		seed, err := jwtplane.GenerateSeed(kind)
		require.NoError(t, err)
		kp, err := jwtplane.ParseSeed(seed, kind)
		require.NoError(t, err)
		return kp
	}
	return jwtplane.Keys{Identity: pair(), Signing: []jwtplane.SigningKey{{Name: "s", Pair: pair()}}}
}

func pubOf(t *testing.T, k jwtplane.Keys) string {
	t.Helper()
	pub, err := k.Identity.PublicKey()
	require.NoError(t, err)
	return pub
}

// creds is a creds file for a new user u of account.
func creds(t *testing.T, u jwtplane.User, account jwtplane.Keys) []byte {
	t.Helper()
	token, seed := newUser(t, u, account)
	out, err := jwt.FormatUserConfig(token, seed)
	require.NoError(t, err)
	return out
}

// newUser signs a new user u of account and returns its JWT and seed.
func newUser(t *testing.T, u jwtplane.User, account jwtplane.Keys) (string, []byte) {
	t.Helper()
	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	u.Name = "u"
	u.PublicKey, err = kp.PublicKey()
	require.NoError(t, err)
	token, err := jwtplane.SignUser(u, account)
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)
	return token, seed
}

func (p *plane) configure(t *testing.T, opts *server.Options) {
	t.Helper()
	resolver := &server.MemAccResolver{}
	for pub, token := range p.jwts {
		require.NoError(t, resolver.Store(pub, token))
	}
	opts.TrustedOperators = []*jwt.OperatorClaims{p.operator}
	opts.SystemAccount = p.sysPub
	opts.AccountResolver = resolver
}

// clusterSize is how many servers each NATS cluster of the supercluster has.
const clusterSize = 3

// startSupercluster runs one NATS cluster of clusterSize JetStream servers per
// name, <name>-0 onwards, each gatewayed to every other, under p. A
// clustered JetStream server refuses to start with no route configured, so
// every port is settled before any server starts.
func startSupercluster(t *testing.T, p *plane, names ...string) map[string][]*server.Server {
	t.Helper()
	return startTaggedSupercluster(t, p, nil, names...)
}

// startTaggedSupercluster is startSupercluster with every server of a NATS
// cluster carrying tags[name].
func startTaggedSupercluster(t *testing.T, p *plane, tags map[string][]string, names ...string) map[string][]*server.Server {
	t.Helper()
	listen := func() int {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() { require.NoError(t, l.Close()) }()
		return l.Addr().(*net.TCPAddr).Port
	}
	routes, gateways := map[string][]*url.URL{}, map[string][]*url.URL{}
	routePorts, gatewayPorts := map[string][]int{}, map[string][]int{}
	for _, name := range names {
		for range clusterSize {
			rp, gp := listen(), listen()
			routePorts[name], gatewayPorts[name] = append(routePorts[name], rp), append(gatewayPorts[name], gp)
			routes[name] = append(routes[name], &url.URL{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", rp)})
			gateways[name] = append(gateways[name], &url.URL{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", gp)})
		}
	}
	var remotes []*server.RemoteGatewayOpts
	for _, name := range names {
		remotes = append(remotes, &server.RemoteGatewayOpts{Name: name, URLs: gateways[name]})
	}
	out := map[string][]*server.Server{}
	var all []*server.Server
	for _, name := range names {
		for i := range clusterSize {
			opts := &server.Options{
				ServerName:         fmt.Sprintf("%s-%d", name, i),
				Host:               "127.0.0.1",
				Port:               -1,
				NoLog:              true,
				NoSigs:             true,
				JetStream:          true,
				JetStreamMaxStore:  256 << 20,
				JetStreamMaxMemory: 64 << 20,
				StoreDir:           t.TempDir(),
				Tags:               jwt.TagList(tags[name]),
				Cluster:            server.ClusterOpts{Name: name, Host: "127.0.0.1", Port: routePorts[name][i]},
				Routes:             routes[name],
				Gateway:            server.GatewayOpts{Name: name, Host: "127.0.0.1", Port: gatewayPorts[name][i], Gateways: remotes},
			}
			p.configure(t, opts)
			srv, err := server.NewServer(opts)
			require.NoError(t, err)
			go srv.Start()
			t.Cleanup(srv.Shutdown)
			out[name] = append(out[name], srv)
			all = append(all, srv)
		}
	}
	for _, srv := range all {
		require.True(t, srv.ReadyForConnections(15*time.Second), "%s did not start", srv.Name())
	}
	require.Eventually(t, func() bool {
		for _, srv := range all {
			if srv.JetStreamIsLeader() {
				return len(srv.JetStreamClusterPeers()) == len(all)
			}
		}
		return false
	}, time.Minute, 100*time.Millisecond, "the supercluster elected no meta leader seeing all %d servers", len(all))
	return out
}

// accountJS is a JetStream context of a new user of account, connected to srv.
func accountJS(t *testing.T, srv *server.Server, account jwtplane.Keys) jetstream.JetStream {
	t.Helper()
	token, seed := newUser(t, jwtplane.User{}, account)
	nc, err := nats.Connect(srv.ClientURL(), nats.UserJWTAndSeed(token, string(seed)))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	j, err := jetstream.New(nc)
	require.NoError(t, err)
	return j
}

// until calls try every interval until it returns nil, failing t with its
// last error once ctx ends or t's deadline is ten seconds away.
func until(t *testing.T, ctx context.Context, interval time.Duration, try func() error, format string, args ...any) {
	t.Helper()
	if dl, ok := t.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, dl.Add(-10*time.Second))
		defer cancel()
	}
	for {
		err := try()
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			require.NoError(t, err, append([]any{format}, args...)...)
		case <-time.After(interval):
		}
	}
}

// createStream creates cfg through j, retrying while the meta group cannot
// place it, and returns once the stream's Raft group has a leader and every
// replica is current.
func createStream(t *testing.T, ctx context.Context, j jetstream.JetStream, cfg jetstream.StreamConfig) {
	t.Helper()
	until(t, ctx, 200*time.Millisecond, func() error { _, err := j.CreateStream(ctx, cfg); return err }, "create %s", cfg.Name)
	until(t, ctx, 200*time.Millisecond, func() error { _, err := settledStream(ctx, j, cfg.Name); return err }, "%s did not settle", cfg.Name)
}

// settledStream returns the leader of stream name, or an error while its Raft
// group has no leader or a replica that is offline or not current.
func settledStream(ctx context.Context, j jetstream.JetStream, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	s, err := j.Stream(ctx, name)
	if err != nil {
		return "", err
	}
	c := s.CachedInfo().Cluster
	if c == nil || c.Leader == "" {
		return "", fmt.Errorf("stream %s has no leader", name)
	}
	for _, r := range c.Replicas {
		if !r.Current || r.Offline {
			return "", fmt.Errorf("stream %s replica %s is not current", name, r.Name)
		}
	}
	return c.Leader, nil
}

// skewedStreams creates n R3 streams named <prefix>_<i> placed in C1 through
// j, then moves every leader to C1-0 through j's own stepdown API, and
// returns their names.
func skewedStreams(t *testing.T, ctx context.Context, j jetstream.JetStream, prefix string, n int) []string {
	t.Helper()
	const cluster, leader = "C1", "C1-0"
	var names []string
	for i := range n {
		name := fmt.Sprintf("%s_%d", prefix, i)
		names = append(names, name)
		createStream(t, ctx, j, jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}, Replicas: 3, Placement: &jetstream.Placement{Cluster: cluster}})
	}
	for _, name := range names {
		until(t, ctx, 300*time.Millisecond, func() error {
			got, err := settledStream(ctx, j, name)
			if err != nil || got == leader {
				return err
			}
			_, _ = j.Conn().Request("$JS.API.STREAM.LEADER.STEPDOWN."+name, fmt.Appendf(nil, `{"placement":{"preferred":%q}}`, leader), 5*time.Second)
			return fmt.Errorf("stream %s led by %s", name, got)
		}, "%s did not move to %s", name, leader)
	}
	return names
}

// recorded drains the events rec holds.
func recorded(rec *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// notes are the notes of the events of typ and reason among evs.
func notes(evs []string, typ, reason string) []string {
	var out []string
	for _, e := range evs {
		if note, ok := strings.CutPrefix(e, typ+" "+reason+" "); ok {
			out = append(out, note)
		}
	}
	return out
}
