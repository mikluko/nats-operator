package authctl

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/natstest"
)

// TestAccountLimits_TiersServed signs an account from tiered limits in spec
// and has a three-server NATS cluster admit a stream of each replica count
// against its own tier.
func TestAccountLimits_TiersServed(t *testing.T) {
	n := func(v int64) *int64 { return &v }
	q := func(s string) *resource.Quantity { v := resource.MustParse(s); return &v }
	limits := accountLimits(&authv1beta1.AccountLimits{JetStream: &authv1beta1.AccountJetStreamLimits{
		Tiers: []authv1beta1.AccountJetStreamTier{
			{Name: "R1", DiskStorage: q("64Mi"), Streams: n(1)},
			{Name: "R3", DiskStorage: q("256Mi"), Streams: n(2)},
		},
	}})

	op := testKeys(t, nkeys.PrefixByteOperator, false)
	sys := testKeys(t, nkeys.PrefixByteAccount, false)
	acc := testKeys(t, nkeys.PrefixByteAccount, false)
	sysPub, accPub := testPub(t, sys.Identity), testPub(t, acc.Identity)

	opJWT, err := jwtplane.SignOperator(jwtplane.Operator{Name: "op", Keys: op, SystemAccount: sysPub})
	require.NoError(t, err)
	opClaims, err := jwt.DecodeOperatorClaims(opJWT)
	require.NoError(t, err)
	sysJWT, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "SYS", Keys: sys}, op)
	require.NoError(t, err)
	accJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: "orders", Keys: acc, Limits: limits}, op, time.Now())
	require.NoError(t, err)

	user, err := nkeys.CreateUser()
	require.NoError(t, err)
	userSeed, err := user.Seed()
	require.NoError(t, err)
	userJWT, err := jwtplane.SignUser(jwtplane.User{Name: "app", PublicKey: testPub(t, user)}, acc)
	require.NoError(t, err)

	c := &natstest.Cluster{Name: "tiers", Size: 3, Configure: func(_ int, o *server.Options) {
		resolver := &server.MemAccResolver{}
		require.NoError(t, resolver.Store(sysPub, sysJWT))
		require.NoError(t, resolver.Store(accPub, accJWT))
		o.TrustedOperators = []*jwt.OperatorClaims{opClaims}
		o.SystemAccount = sysPub
		o.AccountResolver = resolver
	}}
	natstest.StartSupercluster(t, c)
	require.Eventually(t, func() bool {
		for _, s := range c.Servers {
			a, err := s.LookupAccount(accPub)
			if err != nil || !a.JetStreamEnabled() {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond, "a server has not enabled JetStream for the account")

	nc, err := nats.Connect(c.Servers[0].ClientURL(), nats.UserJWTAndSeed(userJWT, string(userSeed)))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	create := func(name string, replicas int) error {
		_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}, Replicas: replicas})
		return err
	}
	refused := func(t *testing.T, err error, code server.ErrorIdentifier) {
		t.Helper()
		var api *jetstream.APIError
		require.ErrorAs(t, err, &api)
		require.Equal(t, jetstream.ErrorCode(code), api.ErrorCode, api.Description)
	}

	info, err := js.AccountInfo(ctx)
	require.NoError(t, err)
	require.Len(t, info.Tiers, 2)
	require.Equal(t, int64(64<<20), info.Tiers["R1"].Limits.MaxStore)
	require.Equal(t, int64(256<<20), info.Tiers["R3"].Limits.MaxStore)

	require.NoError(t, create("one", 1))
	require.NoError(t, create("three", 3))
	refused(t, create("one-more", 1), server.JSMaximumStreamsLimitErr)
	require.NoError(t, create("three-more", 3), "R3 admits a second stream while R1 is full")
	refused(t, create("two", 2), server.JSNoLimitsErr)
}
