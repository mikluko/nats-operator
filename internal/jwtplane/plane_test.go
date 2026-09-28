package jwtplane_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// plane is an auth plane signed by this package and served by one
// in-process nats-server trusting its NATS operator.
type plane struct {
	srv   *server.Server
	users map[string]planeUser
	a     string
}

type planeUser struct {
	jwt  string
	pair nkeys.KeyPair
}

// startPlane builds the plane: account A, whose identity is offline, exports
// the stepdown preset and a private service that B imports; the system
// account imports A's stepdown exports.
func startPlane(t *testing.T) plane {
	t.Helper()
	now := time.Now()
	op := newKeys(t, nkeys.PrefixByteOperator, "op")
	sys := newKeys(t, nkeys.PrefixByteAccount, "sys")
	a := offline(t, newKeys(t, nkeys.PrefixByteAccount, "a"))
	b := newKeys(t, nkeys.PrefixByteAccount, "b")
	sysPub, aPub, bPub := pub(t, sys.Identity), a.PublicKey, pub(t, b.Identity)

	opJWT, err := jwtplane.SignOperator(jwtplane.Operator{Name: "op", Keys: op, SystemAccount: sysPub})
	require.NoError(t, err)
	sysJWT, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "SYS", Keys: sys, StepdownAccounts: []string{aPub}}, op, now)
	require.NoError(t, err)

	execute := jwtplane.Export{Name: "execute", Type: jwt.Service, Subject: "a.execute", Private: true, Importers: []string{bPub}}
	stepdown, err := jwtplane.ExportPreset(jwtplane.ExportPresetJetStreamStepdown)
	require.NoError(t, err)
	token, err := jwtplane.SignActivation(a, execute, bPub)
	require.NoError(t, err)

	p := plane{users: map[string]planeUser{}, a: aPub}
	sign := func(name string, u jwtplane.User, acc jwtplane.Keys) {
		kp := newPair(t, nkeys.PrefixByteUser)
		u.Name, u.PublicKey = name, pub(t, kp)
		tok, err := jwtplane.SignUser(u, acc)
		require.NoError(t, err)
		p.users[name] = planeUser{tok, kp}
	}
	sign("a", jwtplane.User{}, a)
	sign("a-revoked", jwtplane.User{}, a)
	sign("a-readonly", jwtplane.User{Preset: jwtplane.PresetReadonly}, a)
	sign("a-leaf", jwtplane.User{Preset: jwtplane.PresetLeafnode}, a)
	sign("b", jwtplane.User{}, b)
	for _, preset := range controllerPresets {
		sign("sys-"+string(preset), jwtplane.User{Preset: preset, SystemAccount: true}, sys)
	}

	aJWT, err := jwtplane.SignAccount(jwtplane.Account{
		Name:        "A",
		Keys:        a,
		Limits:      jwtplane.Limits{JetStream: &jwtplane.JetStreamLimits{}},
		Exports:     append([]jwtplane.Export{execute}, stepdown...),
		Revocations: []jwtplane.Revocation{{PublicKey: pub(t, p.users["a-revoked"].pair), At: time.Now()}},
	}, op, now)
	require.NoError(t, err)
	bJWT, err := jwtplane.SignAccount(jwtplane.Account{
		Name:    "B",
		Keys:    b,
		Imports: []jwtplane.Import{{Account: aPub, Export: execute, Token: token}},
	}, op, now)
	require.NoError(t, err)

	opc, err := jwt.DecodeOperatorClaims(opJWT)
	require.NoError(t, err)
	res := &server.MemAccResolver{}
	for pk, tok := range map[string]string{sysPub: sysJWT, aPub: aJWT, bPub: bJWT} {
		require.NoError(t, res.Store(pk, tok))
	}
	srv, err := server.NewServer(&server.Options{
		Host:             "127.0.0.1",
		Port:             -1,
		JetStream:        true,
		StoreDir:         t.TempDir(),
		TrustedOperators: []*jwt.OperatorClaims{opc},
		SystemAccount:    sysPub,
		AccountResolver:  res,
		NoLog:            true,
		NoSigs:           true,
	})
	require.NoError(t, err)
	go srv.Start()
	t.Cleanup(srv.Shutdown)
	require.True(t, srv.ReadyForConnections(10*time.Second))
	p.srv = srv
	return p
}

func (p plane) connect(t *testing.T, name string, opts ...nats.Option) (*nats.Conn, error) {
	t.Helper()
	u := p.users[name]
	seed, err := u.pair.Seed()
	require.NoError(t, err)
	nc, err := nats.Connect(p.srv.ClientURL(), append(opts, nats.UserJWTAndSeed(u.jwt, string(seed)), nats.NoReconnect())...)
	if err == nil {
		t.Cleanup(nc.Close)
	}
	return nc, err
}

func TestPlaneServed(t *testing.T) {
	p := startPlane(t)

	t.Run("user signed by an offline account's signing key connects", func(t *testing.T) {
		_, err := p.connect(t, "a")
		require.NoError(t, err)
	})

	t.Run("revoked user is refused", func(t *testing.T) {
		_, err := p.connect(t, "a-revoked")
		require.Error(t, err)
	})

	t.Run("leafnode preset user is refused as a client", func(t *testing.T) {
		_, err := p.connect(t, "a-leaf")
		require.Error(t, err)
	})

	t.Run("private service import through an activation token", func(t *testing.T) {
		nca, err := p.connect(t, "a")
		require.NoError(t, err)
		_, err = nca.Subscribe("a.execute", func(m *nats.Msg) { _ = m.Respond([]byte("done")) })
		require.NoError(t, err)
		require.NoError(t, nca.Flush())

		ncb, err := p.connect(t, "b")
		require.NoError(t, err)
		reply, err := ncb.Request("a.execute", nil, 2*time.Second)
		require.NoError(t, err)
		require.Equal(t, "done", string(reply.Data))
	})

	t.Run("readonly preset reads JetStream under its own inbox and nothing else", func(t *testing.T) {
		violations := make(chan error, 1)
		nc, err := p.connect(t, "a-readonly",
			nats.CustomInboxPrefix(jwtplane.InboxPrefix(jwtplane.PresetReadonly)),
			nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { violations <- err }))
		require.NoError(t, err)
		_, err = nc.Request("$JS.API.INFO", nil, 2*time.Second)
		require.NoError(t, err)

		refused := func(what string, do func() error) {
			t.Helper()
			require.NoError(t, do())
			select {
			case err := <-violations:
				require.ErrorIs(t, err, nats.ErrPermissionViolation, what)
			case <-time.After(2 * time.Second):
				t.Fatalf("%s was not refused", what)
			}
		}
		refused("publish outside the preset", func() error { return nc.Publish("a.execute", nil) })
		for _, subject := range []string{">", "_INBOX.>", "a.execute", "$JS.API.>"} {
			refused("subscribe to "+subject, func() error {
				sub, err := nc.SubscribeSync(subject)
				if err != nil {
					return err
				}
				t.Cleanup(func() { _ = sub.Unsubscribe() })
				return nil
			})
		}
	})

	t.Run("jetstream-stepdown preset routes the system account's request to the account's JetStream API", func(t *testing.T) {
		nca, err := p.connect(t, "a")
		require.NoError(t, err)
		js, err := nca.JetStream()
		require.NoError(t, err)
		_, err = js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"s.>"}, Storage: nats.MemoryStorage})
		require.NoError(t, err)

		nc, err := p.connect(t, "sys-jetstream-controller", nats.CustomInboxPrefix(jwtplane.InboxPrefix(jwtplane.PresetJetStreamController)))
		require.NoError(t, err)
		reply, err := nc.Request(jwtplane.StreamStepdownSubject(p.a, "S"), nil, 2*time.Second)
		require.NoError(t, err)
		var resp struct {
			Type string `json:"type"`
		}
		require.NoError(t, json.Unmarshal(reply.Data, &resp))
		require.Equal(t, "io.nats.jetstream.api.v1.stream_leader_stepdown_response", resp.Type)

		_, err = nc.Request(jwtplane.StreamStepdownSubject(pub(t, newPair(t, nkeys.PrefixByteAccount)), "S"), nil, time.Second)
		require.True(t, errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrTimeout), "an account without the preset is unreachable: %v", err)
	})
}

var controllerPresets = []jwtplane.UserPreset{jwtplane.PresetClusterController, jwtplane.PresetJetStreamController, jwtplane.PresetAuthController}

// TestControllerInboxes pins that a system user holding a controller preset
// takes replies under that preset's inbox prefix and may subscribe to no
// other inbox, another controller preset's among them.
func TestControllerInboxes(t *testing.T) {
	p := startPlane(t)
	for _, own := range controllerPresets {
		t.Run(string(own), func(t *testing.T) {
			violations := make(chan error, 1)
			nc, err := p.connect(t, "sys-"+string(own),
				nats.CustomInboxPrefix(jwtplane.InboxPrefix(own)),
				nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { violations <- err }))
			require.NoError(t, err)
			_, err = nc.Request("$SYS.REQ.SERVER.PING.STATSZ", nil, 2*time.Second)
			require.NoError(t, err)

			refused := []string{"_INBOX.>"}
			for _, other := range controllerPresets {
				if other != own {
					refused = append(refused, jwtplane.InboxPrefix(other)+".>")
				}
			}
			for _, subject := range refused {
				sub, err := nc.SubscribeSync(subject)
				require.NoError(t, err)
				select {
				case err := <-violations:
					require.ErrorIs(t, err, nats.ErrPermissionViolation, subject)
				case <-time.After(2 * time.Second):
					t.Fatalf("subscription to %s was not refused", subject)
				}
				require.NoError(t, sub.Unsubscribe())
			}
		})
	}
}
