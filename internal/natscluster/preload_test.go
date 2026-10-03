package natscluster

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/natstest"
)

// preloadingCluster is edge-operator.yaml's edge-site-2 with no leaf
// remotes, preloading the NatsAccountTrusts refs name.
func preloadingCluster(t *testing.T, refs ...natsv1beta1.ObjectReference) *clusterv1beta1.NatsCluster {
	t.Helper()
	nc := storyLeafCluster(t, "edge-operator.yaml", "edge-site-2")
	nc.Spec.LeafRemotes = nil
	nc.Spec.Auth.AccountTrustRefs = refs
	return nc
}

// renderedWith renders server 0 of nc in its pod from in and remotes as a
// decoded config.
func renderedWith(t *testing.T, nc *clusterv1beta1.NatsCluster, in Inputs, remotes ...LeafRemote) map[string]any {
	t.Helper()
	b, err := serverConfig(nc, in, serverName(nc, 0), podLayout(nc), "r1", remotes...).Render()
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	return m
}

// literalTrust is a NatsAccountTrust in nats-system named name, holding a
// new account of p's NATS operator and its JWT.
func literalTrust(t *testing.T, p testPlane, name string) *natsv1beta1.NatsAccountTrust {
	t.Helper()
	keys := newTestKeys(t, nkeys.PrefixByteAccount)
	accJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: name, Keys: keys}, p.op, time.Now())
	require.NoError(t, err)
	return &natsv1beta1.NatsAccountTrust{
		ObjectMeta: metav1.ObjectMeta{Namespace: "nats-system", Name: name},
		Spec:       natsv1beta1.NatsAccountTrustSpec{PublicKey: publicKey(t, keys.Identity), JWT: accJWT},
	}
}

func updateTrust(t *testing.T, c client.Client, name string, mutate func(*natsv1beta1.NatsAccountTrust)) {
	t.Helper()
	at := &natsv1beta1.NatsAccountTrust{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "nats-system", Name: name}, at))
	mutate(at)
	require.NoError(t, c.Update(context.Background(), at))
}

// TestReadAccountPreloads pins what auth.accountTrustRefs render: every
// server preloads each account's JWT beside the system account's, on a
// NatsCluster that is not a leaf, from a literal trust or from the status
// the auth controller writes.
func TestReadAccountPreloads(t *testing.T) {
	f := newLeafFixture(t)
	ctx := context.Background()
	tel := publicKey(t, f.telemetry.Identity)
	ref := natsv1beta1.ObjectReference{Name: "telemetry"}

	t.Run("a literal trust", func(t *testing.T) {
		nc := preloadingCluster(t, ref)
		accounts, cond, err := readAccountPreloads(ctx, f.client(t), nc, f.p.trust)
		require.NoError(t, err)
		require.Nil(t, cond)
		require.Equal(t, []AccountPreload{{PublicKey: tel, JWT: f.telJWT}}, accounts)
		require.Nil(t, checkPreloads(nc, f.p.trust, nil, accounts))
		m := renderedWith(t, nc, Inputs{Trust: f.p.trust, Accounts: accounts})
		require.Equal(t, map[string]any{f.p.trust.SystemAccount: f.p.trust.SystemAccountJWT, tel: f.telJWT}, m["resolver_preload"])
		require.Equal(t, "full", m["resolver"].(map[string]any)["type"])
	})

	t.Run("a reference-form trust reads its status", func(t *testing.T) {
		c := f.client(t)
		updateTrust(t, c, "telemetry", func(at *natsv1beta1.NatsAccountTrust) {
			at.Spec = natsv1beta1.NatsAccountTrustSpec{AccountRef: &natsv1beta1.ObjectReference{Name: "telemetry"}}
			at.Status.PublicKey, at.Status.JWT = tel, f.telJWT
		})
		accounts, cond, err := readAccountPreloads(ctx, c, preloadingCluster(t, ref), f.p.trust)
		require.NoError(t, err)
		require.Nil(t, cond)
		require.Equal(t, []AccountPreload{{PublicKey: tel, JWT: f.telJWT}}, accounts)
	})

	t.Run("no accountTrustRefs preloads nothing", func(t *testing.T) {
		accounts, cond, err := readAccountPreloads(ctx, f.client(t), preloadingCluster(t), f.p.trust)
		require.NoError(t, err)
		require.Nil(t, cond)
		require.Nil(t, accounts)
	})
}

// TestReadAccountPreloads_Refusals pins the Progressing condition for each
// auth.accountTrustRefs entry that cannot be preloaded.
func TestReadAccountPreloads_Refusals(t *testing.T) {
	f := newLeafFixture(t)
	ctx := context.Background()
	other := mintPlane(t)
	foreignJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: "telemetry", Keys: f.telemetry}, other.op, time.Now())
	require.NoError(t, err)
	tel := publicKey(t, f.telemetry.Identity)
	ref := natsv1beta1.ObjectReference{Name: "telemetry"}
	orders := literalTrust(t, f.p, "orders")

	for _, tt := range []struct {
		name   string
		ref    natsv1beta1.ObjectReference
		mutate func(*natsv1beta1.NatsAccountTrust)
		reason string
		msg    string
	}{
		{"trust missing", natsv1beta1.ObjectReference{Name: "nowhere"}, nil,
			ReasonAccountTrustNotFound, "auth.accountTrustRefs[1]: NatsAccountTrust nats-system/nowhere does not exist"},
		{"no grant admits another namespace", natsv1beta1.ObjectReference{Name: "telemetry", Namespace: "elsewhere"}, nil,
			grant.ReasonNoGrant, "auth.accountTrustRefs[1]: "},
		{"reference form without a public key yet", ref, func(at *natsv1beta1.NatsAccountTrust) {
			at.Spec = natsv1beta1.NatsAccountTrustSpec{AccountRef: &natsv1beta1.ObjectReference{Name: "telemetry"}}
		}, ReasonAccountTrustNotReady, "auth.accountTrustRefs[1]: NatsAccountTrust nats-system/telemetry has no public key in its status yet"},
		{"reference form without a JWT yet", ref, func(at *natsv1beta1.NatsAccountTrust) {
			at.Spec = natsv1beta1.NatsAccountTrustSpec{AccountRef: &natsv1beta1.ObjectReference{Name: "telemetry"}}
			at.Status.PublicKey = tel
		}, ReasonAccountTrustNotReady, "auth.accountTrustRefs[1]: NatsAccountTrust nats-system/telemetry has no JWT in its status yet"},
		{"literal trust without a JWT", ref, func(at *natsv1beta1.NatsAccountTrust) { at.Spec.JWT = "" },
			ReasonAccountTrustInvalid, "auth.accountTrustRefs[1]: NatsAccountTrust nats-system/telemetry carries no jwt to preload"},
		{"JWT signed by another operator", ref, func(at *natsv1beta1.NatsAccountTrust) { at.Spec.JWT = foreignJWT },
			ReasonAccountTrustInvalid, "not by operator"},
		{"JWT of another account", ref, func(at *natsv1beta1.NatsAccountTrust) {
			at.Spec.PublicKey = publicKey(t, newTestPair(t, nkeys.PrefixByteAccount))
		}, ReasonAccountTrustInvalid, "jwt is account"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := f.client(t, orders)
			if tt.mutate != nil {
				updateTrust(t, c, "telemetry", tt.mutate)
			}
			nc := preloadingCluster(t, natsv1beta1.ObjectReference{Name: "orders"}, tt.ref)
			accounts, cond, err := readAccountPreloads(ctx, c, nc, f.p.trust)
			require.NoError(t, err)
			require.Nil(t, accounts)
			require.NotNil(t, cond)
			require.Equal(t, ConditionProgressing, cond.Type)
			require.Equal(t, metav1.ConditionFalse, cond.Status)
			require.Equal(t, tt.reason, cond.Reason, cond.Message)
			require.Contains(t, cond.Message, tt.msg)
		})
	}
}

// startPreloading starts server 0 of nc from its config rendered under trust
// with accounts, keeping its resolver in dir, and returns it.
func startPreloading(t *testing.T, conf string, nc *clusterv1beta1.NatsCluster, trust *Trust, accounts []AccountPreload, dir string) *natstest.Server {
	t.Helper()
	run := t.TempDir()
	l := Layout{
		ClientListen:  natstest.Listen(0),
		RouteListen:   natstest.Listen(0),
		MonitorListen: natstest.Listen(0),
		PidFile:       filepath.Join(run, "nats.pid"),
		StoreDir:      filepath.Join(run, "jetstream"),
		ResolverDir:   dir,
		Routes:        []string{natstest.Unroutable},
		TLSDir:        writeRouteCert(t, nc),
	}
	b, err := serverConfig(nc, Inputs{Trust: trust, Accounts: accounts}, serverName(nc, 0), l, "r1").Render()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(conf, b, 0o600))
	return natstest.Start(t, conf)
}

// TestPreload_RemovedReference pins what a server serves of an account whose
// reference leaves auth.accountTrustRefs: a Full resolver stores a preload
// in its directory, so a server restarted on that directory keeps serving
// the account, and one restarted on a fresh directory does not.
func TestPreload_RemovedReference(t *testing.T) {
	p := mintPlane(t)
	keys := newTestKeys(t, nkeys.PrefixByteAccount)
	accJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: "orders", Keys: keys}, p.op, time.Now())
	require.NoError(t, err)
	pub := publicKey(t, keys.Identity)
	nc := preloadingCluster(t)
	nc.Spec.JetStream = nil
	conf := filepath.Join(t.TempDir(), "nats.conf")
	dial := func(s *natstest.Server) error {
		conn, err := natsconn.Dial(natsconn.Endpoint{Servers: []string{s.ClientURL()}, Creds: p.creds(t, keys, jwtplane.User{Name: "orders"})}, nats.NoReconnect())
		if err == nil {
			conn.Close()
		}
		return err
	}
	restart := func(s *natstest.Server, dir string) *natstest.Server {
		s.Shutdown()
		s.WaitForShutdown()
		return startPreloading(t, conf, nc, p.trust, nil, dir)
	}

	dir := t.TempDir()
	s := startPreloading(t, conf, nc, p.trust, []AccountPreload{{PublicKey: pub, JWT: accJWT}}, dir)
	require.NoError(t, dial(s))
	require.FileExists(t, filepath.Join(dir, pub+".jwt"))

	s = restart(s, dir)
	require.NoError(t, dial(s), "a persistent directory stops serving the account")

	s = restart(s, t.TempDir())
	require.ErrorIs(t, dial(s), nats.ErrAuthorization)
}

// TestPreloads_OncePerAccount pins that one account named twice by
// auth.accountTrustRefs, or by it and a leaf remote, is preloaded once, and
// that two JWTs for one account are refused.
func TestPreloads_OncePerAccount(t *testing.T) {
	f := newLeafFixture(t)
	ctx := context.Background()
	tel := publicKey(t, f.telemetry.Identity)
	resigned, err := jwtplane.SignAccount(jwtplane.Account{Name: "telemetry", Keys: f.telemetry}, f.p.op, time.Now().Add(time.Second))
	require.NoError(t, err)
	twin := &natsv1beta1.NatsAccountTrust{
		ObjectMeta: metav1.ObjectMeta{Namespace: "nats-system", Name: "twin"},
		Spec:       natsv1beta1.NatsAccountTrustSpec{PublicKey: tel, JWT: resigned},
	}
	telemetry := natsv1beta1.ObjectReference{Name: "telemetry"}

	t.Run("one trust named twice", func(t *testing.T) {
		nc := preloadingCluster(t, telemetry, telemetry)
		accounts, cond, err := readAccountPreloads(ctx, f.client(t), nc, f.p.trust)
		require.NoError(t, err)
		require.Nil(t, cond)
		require.Nil(t, checkPreloads(nc, f.p.trust, nil, accounts))
		m := renderedWith(t, nc, Inputs{Trust: f.p.trust, Accounts: accounts})
		require.Equal(t, map[string]any{f.p.trust.SystemAccount: f.p.trust.SystemAccountJWT, tel: f.telJWT}, m["resolver_preload"])
	})

	t.Run("a leaf remote and the list", func(t *testing.T) {
		nc := storyLeafCluster(t, "edge-operator.yaml", "edge-site-2")
		nc.Spec.Auth.AccountTrustRefs = []natsv1beta1.ObjectReference{telemetry}
		c := f.client(t)
		remotes, cond, err := readLeafRemotes(ctx, c, nc, f.p.trust)
		require.NoError(t, err)
		require.Nil(t, cond)
		accounts, cond, err := readAccountPreloads(ctx, c, nc, f.p.trust)
		require.NoError(t, err)
		require.Nil(t, cond)
		require.Nil(t, checkPreloads(nc, f.p.trust, remotes, accounts))
		m := renderedWith(t, nc, Inputs{Trust: f.p.trust, Accounts: accounts}, remotes...)
		require.Equal(t, map[string]any{f.p.trust.SystemAccount: f.p.trust.SystemAccountJWT, tel: f.telJWT}, m["resolver_preload"])
	})

	for _, tt := range []struct {
		name string
		nc   func(*testing.T) *clusterv1beta1.NatsCluster
		msg  string
	}{
		{"two trusts, two JWTs", func(t *testing.T) *clusterv1beta1.NatsCluster {
			return preloadingCluster(t, telemetry, natsv1beta1.ObjectReference{Name: "twin"})
		}, "auth.accountTrustRefs[0] and auth.accountTrustRefs[1] give account " + tel + " different JWTs"},
		{"a leaf remote's JWT and another", func(t *testing.T) *clusterv1beta1.NatsCluster {
			nc := storyLeafCluster(t, "edge-operator.yaml", "edge-site-2")
			nc.Spec.Auth.AccountTrustRefs = []natsv1beta1.ObjectReference{{Name: "twin"}}
			return nc
		}, "leafRemotes[1] and auth.accountTrustRefs[0] give account " + tel + " different JWTs"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			nc := tt.nc(t)
			c := f.client(t, twin.DeepCopy())
			remotes, cond, err := readLeafRemotes(ctx, c, nc, f.p.trust)
			require.NoError(t, err)
			require.Nil(t, cond)
			accounts, cond, err := readAccountPreloads(ctx, c, nc, f.p.trust)
			require.NoError(t, err)
			require.Nil(t, cond)
			cond = checkPreloads(nc, f.p.trust, remotes, accounts)
			require.NotNil(t, cond)
			require.Equal(t, ReasonAccountTrustInvalid, cond.Reason)
			require.Equal(t, tt.msg, cond.Message)
		})
	}

	t.Run("the system account with another JWT", func(t *testing.T) {
		resignedSys := f.p.sign(t, f.p.op, jwtplane.SystemAccount{Name: "SYS", Keys: f.p.sys, Revocations: []jwtplane.Revocation{{PublicKey: publicKey(t, newTestPair(t, nkeys.PrefixByteUser)), At: time.Now()}}})
		cond := checkPreloads(preloadingCluster(t), f.p.trust, nil, []AccountPreload{{PublicKey: f.p.trust.SystemAccount, JWT: resignedSys.SystemAccountJWT}})
		require.NotNil(t, cond)
		require.Equal(t, "auth.trustRef and auth.accountTrustRefs[0] give account "+f.p.trust.SystemAccount+" different JWTs", cond.Message)
	})
}

// TestCheckPreloads_Volume pins the volume rule: a leaf preloading an
// account, through a remote or auth.accountTrustRefs, into a Full resolver
// is refused without jetstream.volumeClaimTemplate; a NatsCluster that is
// not a leaf preloads on whatever directory it has.
func TestCheckPreloads_Volume(t *testing.T) {
	f := newLeafFixture(t)
	ctx := context.Background()
	accounts := []AccountPreload{{PublicKey: publicKey(t, f.telemetry.Identity), JWT: f.telJWT}}
	leaf := func(t *testing.T) (*clusterv1beta1.NatsCluster, []LeafRemote) {
		nc := storyLeafCluster(t, "edge-operator.yaml", "edge-site-2")
		nc.Spec.JetStream.VolumeClaimTemplate = nil
		remotes, cond, err := readLeafRemotes(ctx, f.client(t), nc, f.p.trust)
		require.NoError(t, err)
		require.Nil(t, cond)
		return nc, remotes
	}
	refused := func(t *testing.T, cond *metav1.Condition) {
		require.NotNil(t, cond)
		require.Equal(t, ReasonUnsupportedSpec, cond.Reason)
		require.Contains(t, cond.Message, "auth.resolver: Cache")
	}

	t.Run("a leaf preloading through a remote", func(t *testing.T) {
		nc, remotes := leaf(t)
		refused(t, checkPreloads(nc, f.p.trust, remotes, nil))
	})
	t.Run("a leaf preloading through accountTrustRefs", func(t *testing.T) {
		nc, remotes := leaf(t)
		remotes[1].PreloadJWT = ""
		refused(t, checkPreloads(nc, f.p.trust, remotes, accounts))
	})
	t.Run("Cache on a preloading leaf needs no volume", func(t *testing.T) {
		nc, remotes := leaf(t)
		nc.Spec.Auth.Resolver = clusterv1beta1.ResolverCache
		require.Nil(t, checkPreloads(nc, f.p.trust, remotes, accounts))
	})
	t.Run("not a leaf, without JetStream", func(t *testing.T) {
		nc := preloadingCluster(t)
		nc.Spec.JetStream = nil
		require.Nil(t, checkPreloads(nc, f.p.trust, nil, accounts))
	})
	t.Run("not a leaf, JetStream without a volume", func(t *testing.T) {
		nc := preloadingCluster(t)
		nc.Spec.JetStream.VolumeClaimTemplate = nil
		require.Nil(t, checkPreloads(nc, f.p.trust, nil, accounts))
	})
}

// TestRestartReason_AccountPreload pins that adding, re-signing or removing
// an account auth.accountTrustRefs preload restarts the servers.
func TestRestartReason_AccountPreload(t *testing.T) {
	p := mintPlane(t)
	nc := preloadingCluster(t)
	keys := newTestKeys(t, nkeys.PrefixByteAccount)
	pub := publicKey(t, keys.Identity)
	sign := func(at time.Time) []AccountPreload {
		accJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: "orders", Keys: keys}, p.op, at)
		require.NoError(t, err)
		return []AccountPreload{{PublicKey: pub, JWT: accJWT}}
	}
	first, resigned := sign(time.Now()), sign(time.Now().Add(time.Second))
	render := func(accounts []AccountPreload) []byte {
		return encode(t, renderedWith(t, nc, Inputs{Trust: p.trust, Accounts: accounts}))
	}
	want := "resolver_preload." + pub + " is restart-only"
	require.Equal(t, want, restartReason("2.15.0", render(nil), render(first)))
	require.Equal(t, want, restartReason("2.15.0", render(first), render(resigned)))
	require.Equal(t, want, restartReason("2.15.0", render(first), render(nil)))
}
