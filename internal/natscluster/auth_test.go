package natscluster

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/refindex"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// testPlane is a hand-minted auth plane: a NATS operator with one signing
// key, and its system account.
type testPlane struct {
	op, sys jwtplane.Keys
	trust   *Trust
}

func newTestKeys(t *testing.T, kind nkeys.PrefixByte) jwtplane.Keys {
	t.Helper()
	return jwtplane.Keys{Identity: newTestPair(t, kind), Signing: []jwtplane.SigningKey{{Name: "main", Pair: newTestPair(t, kind)}}}
}

func newTestPair(t *testing.T, kind nkeys.PrefixByte) nkeys.KeyPair {
	t.Helper()
	seed, err := jwtplane.GenerateSeed(kind)
	require.NoError(t, err)
	kp, err := jwtplane.ParseSeed(seed, kind)
	require.NoError(t, err)
	return kp
}

func publicKey(t *testing.T, kp nkeys.KeyPair) string {
	t.Helper()
	p, err := kp.PublicKey()
	require.NoError(t, err)
	return p
}

func mintPlane(t *testing.T) testPlane {
	t.Helper()
	p := testPlane{op: newTestKeys(t, nkeys.PrefixByteOperator), sys: newTestKeys(t, nkeys.PrefixByteAccount)}
	p.trust = p.sign(t, p.op, jwtplane.SystemAccount{Name: "SYS", Keys: p.sys})
	return p
}

// sign signs the NATS operator with keys op and the system account sys under it.
func (p testPlane) sign(t *testing.T, op jwtplane.Keys, sys jwtplane.SystemAccount) *Trust {
	t.Helper()
	opJWT, err := jwtplane.SignOperator(jwtplane.Operator{Name: "demo", Keys: op, SystemAccount: publicKey(t, p.sys.Identity)})
	require.NoError(t, err)
	sysJWT, err := jwtplane.SignSystemAccount(sys, op, time.Now())
	require.NoError(t, err)
	trust, err := ParseTrust(opJWT, sysJWT)
	require.NoError(t, err)
	return trust
}

// creds returns a creds file for a new user of account.
func (p testPlane) creds(t *testing.T, account jwtplane.Keys, u jwtplane.User) []byte {
	t.Helper()
	kp := newTestPair(t, nkeys.PrefixByteUser)
	u.PublicKey = publicKey(t, kp)
	tok, err := jwtplane.SignUser(u, account)
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)
	b, err := jwt.FormatUserConfig(tok, seed)
	require.NoError(t, err)
	return b
}

func (p testPlane) systemCreds(t *testing.T, preset jwtplane.UserPreset) []byte {
	t.Helper()
	return p.creds(t, p.sys, jwtplane.User{Name: string(preset), SystemAccount: true, Preset: preset})
}

// storyAuthCluster reads story 2's NatsCluster manifest, with store limits
// an in-process server accepts.
func storyAuthCluster(t *testing.T) *clusterv1beta1.NatsCluster {
	t.Helper()
	b, err := os.ReadFile("../../docs/content/docs/stories/02-auth-plane/01-natscluster.yaml")
	require.NoError(t, err)
	nc := &clusterv1beta1.NatsCluster{}
	require.NoError(t, yaml.UnmarshalStrict(b, nc))
	nc.Spec.JetStream.Limits = &clusterv1beta1.JetStreamLimits{MaxMemoryStore: quantity("256Mi"), MaxFileStore: quantity("1Gi")}
	return nc
}

func TestParseTrust(t *testing.T) {
	p := mintPlane(t)
	other := mintPlane(t)
	signedByIdentity, err := func() (string, error) {
		c := jwt.NewAccountClaims(publicKey(t, p.sys.Identity))
		return c.Encode(p.op.Identity)
	}()
	require.NoError(t, err)
	strangerSys := jwtplane.Keys{Identity: newTestPair(t, nkeys.PrefixByteAccount), Signing: p.sys.Signing}
	strangerJWT, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{Name: "SYS", Keys: strangerSys}, p.op, time.Now())
	require.NoError(t, err)

	for _, tt := range []struct {
		name     string
		op, sys  string
		wantErr  string
		wantAcct string
	}{
		{"signed by an operator signing key", p.trust.OperatorJWT, p.trust.SystemAccountJWT, "", publicKey(t, p.sys.Identity)},
		{"signed by the operator identity", p.trust.OperatorJWT, signedByIdentity, "", publicKey(t, p.sys.Identity)},
		{"signed by another operator", p.trust.OperatorJWT, other.trust.SystemAccountJWT, "is signed by", ""},
		{"not the operator's system account", p.trust.OperatorJWT, strangerJWT, "names system account", ""},
		{"operator JWT is an account JWT", p.trust.SystemAccountJWT, p.trust.SystemAccountJWT, "operator JWT", ""},
		{"system account JWT is garbage", p.trust.OperatorJWT, "eyJnot", "system account JWT", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTrust(tt.op, tt.sys)
			if tt.wantErr != "" {
				require.ErrorIs(t, err, ErrInvalidTrust)
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantAcct, got.SystemAccount)
		})
	}
}

func TestServerConfig_Auth(t *testing.T) {
	p := mintPlane(t)
	sysPub := publicKey(t, p.sys.Identity)
	for _, tt := range []struct {
		name     string
		resolver clusterv1beta1.ResolverType
		want     map[string]any
	}{
		{"Full by default", "", map[string]any{"type": "full", "dir": "/data/resolver", "allow_delete": true}},
		{"Full", clusterv1beta1.ResolverFull, map[string]any{"type": "full", "dir": "/data/resolver", "allow_delete": true}},
		{"Cache", clusterv1beta1.ResolverCache, map[string]any{"type": "cache", "dir": "/data/resolver"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			nc := storyAuthCluster(t)
			nc.Spec.Auth.Resolver = tt.resolver
			b, err := serverConfig(nc, Inputs{Trust: p.trust}, "demo-0", podLayout(nc), "r1").Render()
			require.NoError(t, err)
			var m map[string]any
			require.NoError(t, json.Unmarshal(b, &m))
			require.Equal(t, p.trust.OperatorJWT, m["operator"])
			require.Equal(t, sysPub, m["system_account"])
			require.Equal(t, tt.want, m["resolver"])
			require.Equal(t, map[string]any{sysPub: p.trust.SystemAccountJWT}, m["resolver_preload"])
		})
	}
	t.Run("no auth plane renders none of it", func(t *testing.T) {
		b, err := serverConfig(storyCluster(t), Inputs{}, "demo-0", podLayout(storyCluster(t)), "r1").Render()
		require.NoError(t, err)
		var m map[string]any
		require.NoError(t, json.Unmarshal(b, &m))
		for _, k := range []string{"operator", "system_account", "resolver", "resolver_preload"} {
			require.NotContains(t, m, k)
		}
	})
}

func TestRender_AuthDataVolume(t *testing.T) {
	p := mintPlane(t)
	dataVolume := func(t *testing.T, nc *clusterv1beta1.NatsCluster) (*corev1.Volume, bool, bool) {
		t.Helper()
		plan, err := Render(nc, Inputs{Trust: p.trust})
		require.NoError(t, err)
		sts := plan.Servers[0].StatefulSet
		mounted := slices.ContainsFunc(container(t, sts, "nats").VolumeMounts, func(m corev1.VolumeMount) bool {
			return m.Name == "data" && m.MountPath == dataDir
		})
		claimed := len(sts.Spec.VolumeClaimTemplates) == 1 && sts.Spec.VolumeClaimTemplates[0].Name == "data"
		for _, v := range sts.Spec.Template.Spec.Volumes {
			if v.Name == "data" {
				return &v, mounted, claimed
			}
		}
		return nil, mounted, claimed
	}

	t.Run("story 2 keeps the resolver on the JetStream volume", func(t *testing.T) {
		v, mounted, claimed := dataVolume(t, storyAuthCluster(t))
		require.Nil(t, v)
		require.True(t, mounted)
		require.True(t, claimed)
	})
	t.Run("without JetStream the resolver gets an emptyDir", func(t *testing.T) {
		nc := storyAuthCluster(t)
		nc.Spec.JetStream = nil
		v, mounted, claimed := dataVolume(t, nc)
		require.NotNil(t, v)
		require.NotNil(t, v.EmptyDir)
		require.True(t, mounted)
		require.False(t, claimed)
	})
}

// authCase is a change to the rendered config of a server under a NATS
// operator.
type authCase struct {
	name   string
	mutate func(t *testing.T, m map[string]any)
	// reason is the classification; "" reloads.
	reason string
	// serverRejects is whether nats-server refuses the reload itself.
	serverRejects bool
}

func authCases(t *testing.T, p testPlane) []authCase {
	sysPub := publicKey(t, p.sys.Identity)
	resigned := p.op
	resigned.Signing = append(slices.Clone(p.op.Signing), jwtplane.SigningKey{Name: "next", Pair: newTestPair(t, nkeys.PrefixByteOperator)})
	withKey := p.sign(t, resigned, jwtplane.SystemAccount{Name: "SYS", Keys: p.sys})
	revoked := p.sign(t, p.op, jwtplane.SystemAccount{Name: "SYS", Keys: p.sys, Revocations: []jwtplane.Revocation{{PublicKey: publicKey(t, newTestPair(t, nkeys.PrefixByteUser)), At: time.Now()}}})
	other := mintPlane(t)
	otherPub := publicKey(t, other.sys.Identity)
	otherPaths := []string{"operator", "resolver_preload." + sysPub, "resolver_preload." + otherPub, "system_account"}
	slices.Sort(otherPaths)

	return []authCase{
		{"server tags", func(_ *testing.T, m map[string]any) { m["server_tags"] = []any{"az:b"} }, "", false},
		{"operator gains a signing key", func(_ *testing.T, m map[string]any) {
			m["operator"] = withKey.OperatorJWT
			m["resolver_preload"] = map[string]any{sysPub: withKey.SystemAccountJWT}
		}, "operator is restart-only", true},
		{"system account re-signed", func(_ *testing.T, m map[string]any) {
			m["resolver_preload"] = map[string]any{sysPub: revoked.SystemAccountJWT}
		}, "", false},
		{"another operator", func(_ *testing.T, m map[string]any) {
			m["operator"] = other.trust.OperatorJWT
			m["system_account"] = otherPub
			m["resolver_preload"] = map[string]any{otherPub: other.trust.SystemAccountJWT}
		}, strings.Join(otherPaths, ", ") + " are restart-only", true},
		{"resolver Full to Cache", func(_ *testing.T, m map[string]any) {
			m["resolver"] = map[string]any{"type": "cache", "dir": m["resolver"].(map[string]any)["dir"]}
		}, "resolver.allow_delete, resolver.type are restart-only", false},
		{"system_account", func(_ *testing.T, m map[string]any) { m["system_account"] = otherPub }, "system_account is restart-only", true},
	}
}

// TestRestartReason_Auth pins the classification of trust changes: the
// trusted NATS operator and system_account are restart-only and nats-server
// refuses to reload them; the system account re-signed, as a revocation or
// a jetstream-stepdown export does, reloads.
func TestRestartReason_Auth(t *testing.T) {
	p := mintPlane(t)
	nc := storyAuthCluster(t)
	tlsDir := writeRouteCert(t, nc)

	t.Run("auth plane added", func(t *testing.T) {
		m := renderedTrustMap(t, nc, p.trust, tlsDir)
		to := encode(t, m)
		for _, k := range []string{"operator", "system_account", "resolver", "resolver_preload"} {
			delete(m, k)
		}
		from := encode(t, m)
		require.Equal(t, "operator, resolver, resolver_preload, system_account are restart-only", restartReason("2.15.0", from, to))
	})
	for _, tt := range authCases(t, p) {
		t.Run(tt.name, func(t *testing.T) {
			m := renderedTrustMap(t, nc, p.trust, tlsDir)
			from := encode(t, m)
			tt.mutate(t, m)
			require.Equal(t, tt.reason, restartReason("2.15.0", from, encode(t, m)))
		})
	}
}

// TestOperatorReload_AgreesWithClassification reloads each trust change
// on a nats-server under a NATS operator: a change nats-server refuses is
// classified restart-only.
func TestOperatorReload_AgreesWithClassification(t *testing.T) {
	skipUnderRace(t)
	p := mintPlane(t)
	nc := storyAuthCluster(t)
	tlsDir := writeRouteCert(t, nc)
	for _, tt := range authCases(t, p) {
		t.Run(tt.name, func(t *testing.T) {
			m := renderedTrustMap(t, nc, p.trust, tlsDir)
			f := filepath.Join(t.TempDir(), "nats.conf")
			require.NoError(t, os.WriteFile(f, encode(t, m), 0o600))
			s := startFile(t, f)

			tt.mutate(t, m)
			require.NoError(t, os.WriteFile(f, encode(t, m), 0o600))
			err := s.Reload()
			if tt.serverRejects {
				require.Error(t, err)
				require.NotEmpty(t, tt.reason, "nats-server refused a change classified as reloading")
				return
			}
			require.NoError(t, err)
		})
	}
}

func startFile(t *testing.T, f string) *server.Server {
	t.Helper()
	o, err := server.ProcessConfigFile(f)
	require.NoError(t, err)
	o.NoLog, o.NoSigs = true, true
	s, err := server.NewServer(o)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	require.True(t, s.ReadyForConnections(10*time.Second))
	return s
}

// authCluster is story 2's NatsCluster booted in-process from its config
// rendered under a hand-minted NATS operator, and the cluster controller's
// system connections to it.
type authCluster struct {
	p     testPlane
	nc    *clusterv1beta1.NatsCluster
	sys   *SystemConnections
	srvs  []*server.Server
	url   string
	files []string
	snap  *sysobs.Snapshot
}

// startAuthCluster boots the cluster and waits until the system user read
// from auth.systemCredentials observes it Settled over $SYS, every server
// reporting revision r1.
func startAuthCluster(t *testing.T) *authCluster {
	t.Helper()
	a := &authCluster{p: mintPlane(t), nc: storyAuthCluster(t)}
	_, a.srvs, a.url, a.files = startRendered(t, a.nc, a.p.trust, "r1")

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: a.nc.Namespace, Name: a.nc.Spec.Auth.SystemCredentials.SecretKeyRef.Name},
		Data:       map[string][]byte{natsconn.DefaultCredentialsKey: a.p.systemCreds(t, jwtplane.PresetClusterController)},
	}
	pool := natsconn.NewPool(natsconn.WithPreset(jwtplane.PresetClusterController))
	t.Cleanup(pool.Close)
	a.sys = &SystemConnections{
		Client:  fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build(),
		Pool:    pool,
		Servers: func(*clusterv1beta1.NatsCluster) []string { return []string{a.url} },
	}
	require.Eventually(t, func() bool {
		s, err := a.sys.Observe(context.Background(), a.nc)
		if err != nil || len(s.Servers) != 3 || !s.Verdict().Settled() {
			return false
		}
		for _, srv := range s.Servers {
			if srv.Metadata[MetadataConfigRevision] != "r1" {
				return false
			}
		}
		a.snap = s
		return true
	}, 30*time.Second, 200*time.Millisecond, "not observed Settled over $SYS")
	return a
}

// pushAccount pushes a new account over $SYS as the auth controller's
// system user, and returns an error from connecting as a user of it.
func pushAccount(t *testing.T, p testPlane, url string) error {
	t.Helper()
	account := newTestKeys(t, nkeys.PrefixByteAccount)
	accJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: "orders", Keys: account}, p.op, time.Now())
	require.NoError(t, err)
	admin, err := natsconn.Dial(natsconn.Endpoint{Servers: []string{url}, Creds: p.systemCreds(t, jwtplane.PresetAuthController)}, nats.CustomInboxPrefix(jwtplane.InboxPrefix(jwtplane.PresetAuthController)))
	require.NoError(t, err)
	defer admin.Close()
	reply, err := admin.Request("$SYS.REQ.CLAIMS.UPDATE", []byte(accJWT), 2*time.Second)
	require.NoError(t, err)
	var resp server.ServerAPIClaimUpdateResponse
	require.NoError(t, json.Unmarshal(reply.Data, &resp))
	require.Nil(t, resp.Error)

	user, err := natsconn.Dial(natsconn.Endpoint{Servers: []string{url}, Creds: p.creds(t, account, jwtplane.User{Name: "orders"})}, nats.NoReconnect())
	if err == nil {
		user.Close()
	}
	return err
}

// TestAuthCluster_SettledOverSystemUser pins that story 2's rendered config
// boots a NATS cluster that a cluster-controller preset user of its preloaded
// system account observes Settled, and whose full resolver serves an account
// pushed over $SYS.
func TestAuthCluster_SettledOverSystemUser(t *testing.T) {
	a := startAuthCluster(t)
	require.NoError(t, pushAccount(t, a.p, a.url))
}

// TestOperatorReload_OverSystemUser pins a reload requested over $SYS by
// the cluster controller's system user, after which the full resolver still
// serves an account pushed over $SYS.
func TestOperatorReload_OverSystemUser(t *testing.T) {
	skipUnderRace(t)
	a := startAuthCluster(t)
	ctx := context.Background()

	var m map[string]any
	b, err := os.ReadFile(a.files[0])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &m))
	m["server_tags"] = []any{"az:b"}
	retagged := encode(t, m)
	require.NoError(t, os.WriteFile(a.files[0], retagged, 0o600))
	rl, err := a.sys.Reloader(ctx, a.nc)
	require.NoError(t, err)
	applied, err := reloadServer(ctx, rl, a.snap, Server{Name: "demo-0", ConfigMap: &corev1.ConfigMap{Data: map[string]string{configFile: string(retagged)}}}, Certs{})
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, []string{"az:b"}, varzTags(t, a.srvs[0]))

	require.NoError(t, pushAccount(t, a.p, a.url), "an account pushed after the reload is not served")
}

// TestSystemConnections_NoSystemUser pins that a NatsCluster naming no
// system user is observed through the fallback and not reloaded, and that
// a missing credentials Secret is an error.
func TestSystemConnections_NoSystemUser(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	fallback := &fakeObserver{}
	fallback.set(&sysobs.Snapshot{Servers: []sysobs.Server{{Name: "demo-0"}}})
	pool := natsconn.NewPool(natsconn.WithPreset(jwtplane.PresetClusterController))
	t.Cleanup(pool.Close)
	sys := &SystemConnections{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), Pool: pool, Fallback: fallback}
	ctx := context.Background()

	for name, mutate := range map[string]func(*clusterv1beta1.NatsCluster){
		"no auth":              func(nc *clusterv1beta1.NatsCluster) { nc.Spec.Auth = nil },
		"no systemCredentials": func(nc *clusterv1beta1.NatsCluster) { nc.Spec.Auth.SystemCredentials = nil },
	} {
		t.Run(name, func(t *testing.T) {
			nc := storyAuthCluster(t)
			mutate(nc)
			snap, err := sys.Observe(ctx, nc)
			require.NoError(t, err)
			require.Equal(t, "demo-0", snap.Servers[0].Name)
			_, err = sys.Reloader(ctx, nc)
			require.ErrorIs(t, err, ErrNoSystemUser)
		})
	}
	t.Run("credentials Secret missing", func(t *testing.T) {
		_, err := sys.Observe(ctx, storyAuthCluster(t))
		require.ErrorIs(t, err, natsconn.ErrSecretNotFound)
		_, err = sys.Reloader(ctx, storyAuthCluster(t))
		require.ErrorIs(t, err, natsconn.ErrSecretNotFound)
	})
}

// TestOperatorReload_ResolverLosesPushedAccounts pins the nats-server 2.15
// defect that keeps resolver out of the reload allow-list: a reload that
// moves the resolver's directory succeeds, the running resolver still
// acknowledges a claims update, and the account it stored is not served.
func TestOperatorReload_ResolverLosesPushedAccounts(t *testing.T) {
	skipUnderRace(t)
	p := mintPlane(t)
	nc := storyAuthCluster(t)
	m := renderedTrustMap(t, nc, p.trust, writeRouteCert(t, nc))
	f := filepath.Join(t.TempDir(), "nats.conf")
	require.NoError(t, os.WriteFile(f, encode(t, m), 0o600))
	s := startFile(t, f)

	m["resolver"].(map[string]any)["dir"] = t.TempDir()
	require.NoError(t, os.WriteFile(f, encode(t, m), 0o600))
	require.NoError(t, s.Reload())

	require.ErrorIs(t, pushAccount(t, p, s.ClientURL()), nats.ErrAuthorization)
}

// TestClustersTrusting pins the NatsOperatorTrust watch mapping: a trust
// object enqueues the NatsClusters whose auth.trustRef names it, from
// their own namespace or another.
func TestClustersTrusting(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clusterv1beta1.AddToScheme(scheme))
	cluster := func(ns, name, trustNS string) *clusterv1beta1.NatsCluster {
		nc := &clusterv1beta1.NatsCluster{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
		if trustNS != "-" {
			nc.Spec.Auth = &clusterv1beta1.Auth{TrustRef: natsv1beta1.ObjectReference{Name: "demo", Namespace: trustNS}}
		}
		return nc
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&clusterv1beta1.NatsCluster{}, TrustField, func(o client.Object) []string {
			if key := trustKey(o.(*clusterv1beta1.NatsCluster)); key != "" {
				return []string{key}
			}
			return nil
		}).
		WithObjects(cluster("a", "same", ""), cluster("b", "cross", "a"), cluster("a", "noauth", "-"), cluster("b", "own", "")).
		Build()
	trust := &natsv1beta1.NatsOperatorTrust{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "demo"}}
	got := enqueued(t, refindex.EnqueueByField(c, &clusterv1beta1.NatsClusterList{}, TrustField), trust)
	require.ElementsMatch(t, []string{"a/same", "b/cross"}, got)
}

// skipUnderRace skips a test under the race detector, which trips on
// nats-server 2.15 comparing a resolver's atomically updated expiry counter
// with reflect.DeepEqual on every reload. `just test` runs these tests again
// without -race.
func skipUnderRace(t *testing.T) {
	t.Helper()
	if raceEnabled {
		t.Skip("nats-server 2.15 races on reload of a config with a resolver directory")
	}
}
