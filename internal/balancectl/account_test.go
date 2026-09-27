package balancectl

import (
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// TestAccountOf_UserJWT pins that the account of a connection with a user
// JWT is read from the JWT, so a user allowed to publish on $JS.API.> alone
// is not left waiting on $SYS.REQ.USER.INFO.
func TestAccountOf_UserJWT(t *testing.T) {
	p := newPlane(t)
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true}
	p.configure(t, opts)
	srv, err := server.NewServer(opts)
	require.NoError(t, err)
	go srv.Start()
	t.Cleanup(srv.Shutdown)
	require.True(t, srv.ReadyForConnections(10*time.Second))

	token, seed := newUser(t, jwtplane.User{Permissions: &jwtplane.Permissions{
		Publish:   jwtplane.SubjectPermissions{Allow: []string{"$JS.API.>"}},
		Subscribe: jwtplane.SubjectPermissions{Allow: []string{"_INBOX.>"}},
	}}, p.a)
	nc, err := nats.Connect(srv.ClientURL(), nats.UserJWTAndSeed(token, string(seed)), nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	uc, err := jwt.DecodeUserClaims(token)
	require.NoError(t, err)
	require.Equal(t, p.aPub, uc.IssuerAccount, "signed by A's signing key")

	start := time.Now()
	account, err := accountOf(t.Context(), nc)
	require.NoError(t, err)
	require.Equal(t, p.aPub, account)
	require.Less(t, time.Since(start), probeTimeout/2)
}
