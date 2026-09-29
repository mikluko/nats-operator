package authctl_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	"github.com/mikluko/nats-operator/internal/authctl"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// plane is a NATS operator, its system account and one ordinary account, with
// the keys to sign users of either.
type plane struct {
	opJWT, sysJWT, accJWT string
	op, sys, acc          jwtplane.Keys
	sysPub, accPub        string
}

func newPlane(t *testing.T) plane {
	t.Helper()
	keys := func(prefix nkeys.PrefixByte) jwtplane.Keys {
		id, err := nkeys.CreatePair(prefix)
		require.NoError(t, err)
		sk, err := nkeys.CreatePair(prefix)
		require.NoError(t, err)
		return jwtplane.Keys{Identity: id, Signing: []jwtplane.SigningKey{{Name: "s", Pair: sk}}}
	}
	op, sys, acc := keys(nkeys.PrefixByteOperator), keys(nkeys.PrefixByteAccount), keys(nkeys.PrefixByteAccount)
	p := plane{op: op, sys: sys, acc: acc}
	var err error
	p.sysPub, err = sys.Identity.PublicKey()
	require.NoError(t, err)
	p.accPub, err = acc.Identity.PublicKey()
	require.NoError(t, err)
	p.opJWT, err = jwtplane.SignOperator(jwtplane.Operator{Name: "op", Keys: op, SystemAccount: p.sysPub})
	require.NoError(t, err)
	p.sysJWT, err = jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "sys", Keys: sys}, op, time.Now())
	require.NoError(t, err)
	p.accJWT, err = jwtplane.SignAccount(jwtplane.Account{Name: "acc", Keys: acc, Limits: jwtplane.Limits{}}, op, time.Now())
	require.NoError(t, err)
	return p
}

// startServers starts n routed servers trusting p's NATS operator.
func startServers(t *testing.T, p plane, n int) []*server.Server {
	t.Helper()
	oc, err := jwt.DecodeOperatorClaims(p.opJWT)
	require.NoError(t, err)
	var out []*server.Server
	for i := range n {
		res := &server.MemAccResolver{}
		require.NoError(t, res.Store(p.sysPub, p.sysJWT))
		require.NoError(t, res.Store(p.accPub, p.accJWT))
		o := &server.Options{
			ServerName:       fmt.Sprintf("s%d", i),
			Host:             "127.0.0.1",
			Port:             -1,
			TrustedOperators: []*jwt.OperatorClaims{oc},
			SystemAccount:    p.sysPub,
			AccountResolver:  res,
			NoLog:            true,
			NoSigs:           true,
			Cluster:          server.ClusterOpts{Name: "c", Host: "127.0.0.1", Port: -1},
		}
		if i > 0 {
			o.Routes = server.RoutesFromStr("nats://" + out[0].ClusterAddr().String())
		}
		s, err := server.NewServer(o)
		require.NoError(t, err)
		go s.Start()
		t.Cleanup(s.Shutdown)
		require.True(t, s.ReadyForConnections(10*time.Second))
		out = append(out, s)
	}
	require.Eventually(t, func() bool {
		for _, s := range out {
			if s.NumRoutes() < n-1 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)
	return out
}

// dial connects a new user of the account with keys to url, returning the
// connection and the user's public key.
func dial(t *testing.T, url string, u jwtplane.User, keys jwtplane.Keys) (*nats.Conn, string) {
	t.Helper()
	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)
	u.PublicKey = pub
	token, err := jwtplane.SignUser(u, keys)
	require.NoError(t, err)
	opts := []nats.Option{nats.UserJWTAndSeed(token, string(seed)), nats.NoReconnect()}
	if u.Preset != "" {
		opts = append(opts, nats.CustomInboxPrefix(jwtplane.InboxPrefix(u.Preset)))
	}
	nc, err := nats.Connect(url, opts...)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc, pub
}

// authInbox is the inbox prefix the auth-controller preset grants.
var authInbox = nats.CustomInboxPrefix(jwtplane.InboxPrefix(jwtplane.PresetAuthController))

func TestConnSessions_Kick(t *testing.T) {
	p := newPlane(t)
	srvs := startServers(t, p, 2)
	sysNC, _ := dial(t, srvs[0].ClientURL(), jwtplane.User{Name: "ctl", SystemAccount: true, Preset: jwtplane.PresetAuthController}, p.sys)

	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)
	token, err := jwtplane.SignUser(jwtplane.User{Name: "victim", PublicKey: pub}, p.acc)
	require.NoError(t, err)
	var victims []*nats.Conn
	for _, s := range srvs {
		nc, err := nats.Connect(s.ClientURL(), nats.UserJWTAndSeed(token, string(seed)), nats.NoReconnect())
		require.NoError(t, err)
		t.Cleanup(nc.Close)
		victims = append(victims, nc)
	}
	bystander, _ := dial(t, srvs[1].ClientURL(), jwtplane.User{Name: "bystander"}, p.acc)

	operator := types.NamespacedName{Namespace: "ns", Name: "op"}
	s := authctl.ConnSessions{Resolvers: &authctl.Resolvers{
		Conn: func(_ context.Context, got types.NamespacedName) (*nats.Conn, error) {
			if !assert.Equal(t, operator, got) {
				return nil, fmt.Errorf("no connection for %s", got)
			}
			return sysNC, nil
		},
		Wait: 500 * time.Millisecond,
	}}
	n, err := s.Kick(t.Context(), operator, p.sysPub, pub)
	require.NoError(t, err)
	require.Zero(t, n, "the account is filtered on as well as the user")

	n, err = s.Kick(t.Context(), operator, p.accPub, pub)
	require.NoError(t, err)
	require.Equal(t, 2, n, "one connection on each server")
	for _, nc := range victims {
		require.Eventually(t, nc.IsClosed, 5*time.Second, 20*time.Millisecond)
	}
	require.True(t, bystander.IsConnected(), "another user of the account keeps its connection")

	n, err = s.Kick(t.Context(), operator, p.accPub, pub)
	require.NoError(t, err)
	require.Zero(t, n, "every server answered with none")
}

// TestConnSessions_KickNoRoster pins that a kick pass no server answers,
// as over a system connection that lost its server and buffers its
// publishes, is ErrUnreachable rather than none found.
func TestConnSessions_KickNoRoster(t *testing.T) {
	p := newPlane(t)
	srv := startServers(t, p, 1)[0]
	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)
	token, err := jwtplane.SignUser(jwtplane.User{Name: "ctl", PublicKey: pub, SystemAccount: true, Preset: jwtplane.PresetAuthController}, p.sys)
	require.NoError(t, err)
	sysNC, err := nats.Connect(srv.ClientURL(), nats.UserJWTAndSeed(token, string(seed)), authInbox,
		nats.MaxReconnects(-1), nats.ReconnectWait(time.Hour))
	require.NoError(t, err)
	t.Cleanup(sysNC.Close)
	srv.Shutdown()
	require.Eventually(t, sysNC.IsReconnecting, 5*time.Second, 20*time.Millisecond)

	s := authctl.ConnSessions{Resolvers: &authctl.Resolvers{
		Conn: func(context.Context, types.NamespacedName) (*nats.Conn, error) { return sysNC, nil },
		Wait: 200 * time.Millisecond,
	}}
	_, err = s.Kick(t.Context(), types.NamespacedName{Namespace: "ns", Name: "op"}, p.accPub, pub)
	require.ErrorIs(t, err, authctl.ErrUnreachable)
}
