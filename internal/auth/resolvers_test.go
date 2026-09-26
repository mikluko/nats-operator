package auth_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/auth"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// fullCluster is a routed cluster of servers trusting a plane's operator,
// each with a full resolver on a directory of its own that deletes are
// allowed on and that outlives a restart. The resolvers sync once an hour,
// so only pushes and deletes move JWTs between them.
type fullCluster struct {
	t    *testing.T
	oc   *jwt.OperatorClaims
	p    plane
	dirs []string
	srvs []*server.Server
}

func startFullCluster(t *testing.T, p plane, n int) *fullCluster {
	t.Helper()
	oc, err := jwt.DecodeOperatorClaims(p.opJWT)
	require.NoError(t, err)
	c := &fullCluster{t: t, oc: oc, p: p, srvs: make([]*server.Server, n)}
	for range n {
		c.dirs = append(c.dirs, t.TempDir())
	}
	for i := range n {
		c.start(i)
	}
	return c
}

// start starts server i on its directory, routed to server 0, and waits
// for every running server to route to every other.
func (c *fullCluster) start(i int) {
	t := c.t
	t.Helper()
	res, err := server.NewDirAccResolver(c.dirs[i], 0, time.Hour, server.HardDelete)
	require.NoError(t, err)
	require.NoError(t, res.Store(c.p.sysPub, c.p.sysJWT))
	o := &server.Options{
		ServerName:       fmt.Sprintf("s%d", i),
		Host:             "127.0.0.1",
		Port:             -1,
		TrustedOperators: []*jwt.OperatorClaims{c.oc},
		SystemAccount:    c.p.sysPub,
		AccountResolver:  res,
		NoLog:            true,
		NoSigs:           true,
		Cluster:          server.ClusterOpts{Name: "c", Host: "127.0.0.1", Port: -1},
	}
	if i > 0 {
		o.Routes = server.RoutesFromStr("nats://" + c.srvs[0].ClusterAddr().String())
	}
	s, err := server.NewServer(o)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	require.True(t, s.ReadyForConnections(10*time.Second))
	c.srvs[i] = s
	require.Eventually(t, func() bool {
		up := c.running()
		for _, s := range up {
			if s.NumRoutes() < len(up)-1 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)
}

func (c *fullCluster) stop(i int) {
	c.srvs[i].Shutdown()
	c.srvs[i].WaitForShutdown()
	c.srvs[i] = nil
}

func (c *fullCluster) running() []*server.Server {
	var out []*server.Server
	for _, s := range c.srvs {
		if s != nil {
			out = append(out, s)
		}
	}
	return out
}

// held returns the JWT server i's resolver directory holds for account, or
// "" for none; it reads the directory, so it works on a stopped server.
func (c *fullCluster) held(i int, account string) string {
	raw, err := os.ReadFile(filepath.Join(c.dirs[i], account+".jwt"))
	if os.IsNotExist(err) {
		return ""
	}
	require.NoError(c.t, err)
	return string(raw)
}

// newAccount returns a new account's keys and public key.
func newAccount(t *testing.T) (jwtplane.Keys, string) {
	t.Helper()
	id, err := nkeys.CreateAccount()
	require.NoError(t, err)
	sk, err := nkeys.CreateAccount()
	require.NoError(t, err)
	pub, err := id.PublicKey()
	require.NoError(t, err)
	return jwtplane.Keys{Identity: id, Signing: []jwtplane.SigningKey{{Name: "s", Pair: sk}}}, pub
}

// signAccount signs the account with keys, named name, with p's operator.
func signAccount(t *testing.T, p plane, keys jwtplane.Keys, name string) string {
	t.Helper()
	token, err := jwtplane.SignAccount(jwtplane.Account{Name: name, Keys: keys}, p.op, time.Now())
	require.NoError(t, err)
	return token
}

// signTwice signs the account twice, a second apart, so the second JWT is
// issued after the first.
func signTwice(t *testing.T, p plane, keys jwtplane.Keys) (older, newer string) {
	t.Helper()
	older = signAccount(t, p, keys, "v1")
	time.Sleep(1100 * time.Millisecond)
	return older, signAccount(t, p, keys, "v2")
}

// resolversOn returns Resolvers reaching c through a system user holding
// the auth-controller preset, connected to server 0.
func resolversOn(t *testing.T, c *fullCluster, operator types.NamespacedName) *auth.Resolvers {
	t.Helper()
	nc, _ := dial(t, c.srvs[0].ClientURL(), jwtplane.User{Name: "auth-controller", SystemAccount: true, Preset: jwtplane.PresetAuthController}, c.p.sys)
	return &auth.Resolvers{
		Conn: func(_ context.Context, got types.NamespacedName) (*nats.Conn, error) {
			require.Equal(t, operator, got)
			return nc, nil
		},
		Wait:     500 * time.Millisecond,
		Interval: 200 * time.Millisecond,
	}
}

var testOperator = types.NamespacedName{Namespace: "ns", Name: "op"}

// TestResolvers_PushCrossesRoutes pins that one push into one server lands
// in the resolver of every routed server, and that a user of the account
// can then connect to any of them.
func TestResolvers_PushCrossesRoutes(t *testing.T) {
	p := newPlane(t)
	c := startFullCluster(t, p, 3)
	r := resolversOn(t, c, testOperator)
	keys, pub := newAccount(t)
	token := signAccount(t, p, keys, "orders")

	for i := range c.srvs {
		require.Empty(t, c.held(i, pub))
	}
	require.NoError(t, r.Push(t.Context(), testOperator, token))
	for i, s := range c.srvs {
		require.Equal(t, token, c.held(i, pub), "server %d", i)
		dial(t, s.ClientURL(), jwtplane.User{Name: "u"}, keys)
	}
}

// TestResolvers_Current pins the count behind status.distribution: the
// servers answering STATSZ, and those among them whose CLAIMS.LOOKUP reply
// is the JWT, one holding an older JWT or none not counted, until a push
// brings it current.
func TestResolvers_Current(t *testing.T) {
	p := newPlane(t)
	c := startFullCluster(t, p, 3)
	r := resolversOn(t, c, testOperator)
	keys, pub := newAccount(t)
	v1, v2 := signTwice(t, p, keys)

	d, err := r.Current(t.Context(), testOperator, v1)
	require.NoError(t, err)
	require.Equal(t, authv1beta1.Distribution{Servers: 3}, d, "no server holds the account, and none was pushed it")

	require.NoError(t, r.Push(t.Context(), testOperator, v1))
	d, err = r.Current(t.Context(), testOperator, v1)
	require.NoError(t, err)
	require.Equal(t, [2]int32{3, 3}, [2]int32{d.Servers, d.Current})
	require.NotNil(t, d.LastPushTime)

	c.stop(2)
	require.NoError(t, r.Push(t.Context(), testOperator, v2))
	c.start(2)
	require.Equal(t, v1, c.held(2, pub), "server 2 was down for the push")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = r.Start(ctx) }()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		d, err := r.Current(t.Context(), testOperator, v2)
		assert.NoError(ct, err)
		assert.Equal(ct, [2]int32{3, 2}, [2]int32{d.Servers, d.Current}, "server 2 holds the older JWT")
	}, 10*time.Second, 100*time.Millisecond)

	require.NoError(t, r.Push(t.Context(), testOperator, v2), "the current JWT is pushed again")
	d, err = r.Current(t.Context(), testOperator, v2)
	require.NoError(t, err)
	require.Equal(t, [2]int32{3, 3}, [2]int32{d.Servers, d.Current})
}

// TestResolvers_Lookup pins the answer revocations are recovered from:
// "" only when every server says it holds no JWT, the newest JWT where
// servers disagree, and ErrUnreachable where no server can be asked.
func TestResolvers_Lookup(t *testing.T) {
	p := newPlane(t)
	c := startFullCluster(t, p, 3)
	r := resolversOn(t, c, testOperator)
	keys, pub := newAccount(t)
	v1, v2 := signTwice(t, p, keys)

	got, err := r.Lookup(t.Context(), testOperator, pub)
	require.NoError(t, err)
	require.Empty(t, got, "no server holds the account")

	require.NoError(t, r.Push(t.Context(), testOperator, v1))
	c.stop(2)
	require.NoError(t, r.Push(t.Context(), testOperator, v2))
	c.start(2)
	require.Equal(t, v1, c.held(2, pub))
	got, err = r.Lookup(t.Context(), testOperator, pub)
	require.NoError(t, err)
	require.Equal(t, v2, got, "the newest of the JWTs the servers hold")

	down := &auth.Resolvers{
		Conn: func(context.Context, types.NamespacedName) (*nats.Conn, error) {
			return nil, errors.New("dial: refused")
		},
		Wait: 200 * time.Millisecond,
	}
	_, err = down.Lookup(t.Context(), testOperator, pub)
	require.ErrorIs(t, err, auth.ErrUnreachable)
}

// TestResolvers_NeverPushesOlder pins Q2181's rule: a server keeps whatever
// it is pushed last, so a JWT issued before one already pushed, or before
// one a server holds when nothing was pushed since a restart, is refused
// and never sent.
func TestResolvers_NeverPushesOlder(t *testing.T) {
	p := newPlane(t)
	c := startFullCluster(t, p, 2)
	keys, pub := newAccount(t)
	v1, v2 := signTwice(t, p, keys)

	r := resolversOn(t, c, testOperator)
	require.NoError(t, r.Push(t.Context(), testOperator, v2))
	require.ErrorIs(t, r.Push(t.Context(), testOperator, v1), auth.ErrStaleJWT)
	require.NoError(t, r.Push(t.Context(), testOperator, v2), "the same JWT again is not older")

	restarted := resolversOn(t, c, testOperator)
	require.ErrorIs(t, restarted.Push(t.Context(), testOperator, v1), auth.ErrStaleJWT, "a server holds a newer one")
	for i := range c.srvs {
		require.Equal(t, v2, c.held(i, pub), "server %d", i)
	}
}

// TestResolvers_DeleteResentOnRejoin pins Q2068's gap and its remedy: a
// server down across a delete comes back still serving the account, since
// the resolver's sync never carries a delete, and the delete is sent again
// once it answers STATSZ, under a new server ID.
func TestResolvers_DeleteResentOnRejoin(t *testing.T) {
	p := newPlane(t)
	c := startFullCluster(t, p, 3)
	r := resolversOn(t, c, testOperator)
	keys, pub := newAccount(t)
	token := signAccount(t, p, keys, "orders")
	require.NoError(t, r.Push(t.Context(), testOperator, token))

	c.stop(2)
	req, err := jwtplane.SignDelete(p.op, []string{pub})
	require.NoError(t, err)
	require.NoError(t, r.Delete(t.Context(), testOperator, req))
	for i := range 2 {
		require.Empty(t, c.held(i, pub), "server %d deleted it", i)
		_, err := nats.Connect(c.srvs[i].ClientURL(), userCreds(t, keys), nats.NoReconnect())
		require.Error(t, err, "server %d refuses the account's users", i)
	}
	require.Equal(t, token, c.held(2, pub), "server 2 was down for the delete")

	c.start(2)
	nc, err := nats.Connect(c.srvs[2].ClientURL(), userCreds(t, keys), nats.NoReconnect())
	require.NoError(t, err, "back up, server 2 still serves the deleted account")
	nc.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = r.Start(ctx) }()
	require.Eventually(t, func() bool { return c.held(2, pub) == "" }, 10*time.Second, 50*time.Millisecond,
		"the delete is sent again once server 2 is in the roster")
	_, err = nats.Connect(c.srvs[2].ClientURL(), userCreds(t, keys), nats.NoReconnect())
	require.Error(t, err)
}

// userCreds signs a new user of the account with keys.
func userCreds(t *testing.T, keys jwtplane.Keys) nats.Option {
	t.Helper()
	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)
	token, err := jwtplane.SignUser(jwtplane.User{Name: "u", PublicKey: pub}, keys)
	require.NoError(t, err)
	return nats.UserJWTAndSeed(token, string(seed))
}
