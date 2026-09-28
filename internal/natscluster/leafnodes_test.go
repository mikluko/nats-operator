package natscluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// storyDoc decodes the object of kind and name in story 10's step-1 file
// 01-<file> into into, refusing fields the type does not have.
func storyDoc(t *testing.T, file, kind, name string, into any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("../../docs/content/docs/stories/10-leafnodes", "01-"+file))
	require.NoError(t, err)
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(b), 4096)
	for {
		var obj struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		var raw json.RawMessage
		err := dec.Decode(&raw)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		require.NoError(t, json.Unmarshal(raw, &obj))
		if obj.Kind == kind && obj.Metadata.Name == name {
			d := json.NewDecoder(bytes.NewReader(raw))
			d.DisallowUnknownFields()
			require.NoError(t, d.Decode(into))
			return
		}
	}
	require.Failf(t, "not in story", "%s %s in %s", kind, name, file)
}

// storyLeafCluster reads a NatsCluster from story 10, with store limits an
// in-process server accepts.
func storyLeafCluster(t *testing.T, file, name string) *clusterv1beta1.NatsCluster {
	t.Helper()
	nc := &clusterv1beta1.NatsCluster{}
	storyDoc(t, file, "NatsCluster", name, nc)
	if nc.Spec.JetStream != nil {
		nc.Spec.JetStream.Limits = &clusterv1beta1.JetStreamLimits{MaxMemoryStore: quantity("256Mi"), MaxFileStore: quantity("1Gi")}
	}
	return nc
}

func leafScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, natsv1beta1.AddToScheme(s))
	require.NoError(t, clusterv1beta1.AddToScheme(s))
	return s
}

func credsSecret(ns, name string, creds []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Data:       map[string][]byte{natsconn.DefaultCredentialsKey: creds},
	}
}

// storyConnection reads a NatsConnection from story 10 with its servers
// replaced by url and, when ca is set, a CA from Secret hub-ca.
func storyConnection(t *testing.T, file, name, url string, ca bool) *natsv1beta1.NatsConnection {
	t.Helper()
	c := &natsv1beta1.NatsConnection{}
	storyDoc(t, file, "NatsConnection", name, c)
	c.Spec.Servers = []string{url}
	if ca {
		c.Spec.TLS = &natsv1beta1.ConnectionTLS{CA: &natsv1beta1.CA{SecretKeyRef: natsv1beta1.CASecretKeySelector{Name: "hub-ca"}}}
	}
	return c
}

// leafUser is story 10's edge-site-1 NatsUser as a jwtplane user: a leaf
// connection narrowed to the telemetry subjects.
func leafUser() jwtplane.User {
	return jwtplane.User{
		Name:                   "edge-site-1",
		AllowedConnectionTypes: []string{jwt.ConnectionTypeLeafnode},
		Permissions: &jwtplane.Permissions{
			Publish:   jwtplane.SubjectPermissions{Allow: []string{"telemetry.>"}},
			Subscribe: jwtplane.SubjectPermissions{Allow: []string{"telemetry.>", "_INBOX.>"}},
		},
	}
}

// booted is a NATS cluster started in-process from rendered configs.
type booted struct {
	nc         *clusterv1beta1.NatsCluster
	trust      *Trust
	srvs       []*server.Server
	eps        []sysobs.Endpoint
	layouts    []Layout
	files      []string
	remotesDir string
	// leafPort is the leafnode listener's port on the first server.
	leafPort int
}

func (b *booted) clientURL(i int) string {
	return fmt.Sprintf("nats://%s", b.layouts[i].ClientListen)
}

// bootRendered starts every server of nc from its config rendered under
// trust with remotes, on loopback ports, with the leafnode listener's
// certificate and the leaf remotes Secret's files written where the layout
// names them.
func bootRendered(t *testing.T, nc *clusterv1beta1.NatsCluster, trust *Trust, remotes []LeafRemote, leafTLSDir string) *booted {
	t.Helper()
	b := &booted{nc: nc, trust: trust, remotesDir: t.TempDir()}
	names := serverNames(nc)
	var routes []string
	routePorts := make([]int, len(names))
	for i := range names {
		routePorts[i] = freePort(t)
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", routePorts[i]))
	}
	tlsDir := writeRouteCert(t, nc)
	for i := range names {
		dir := t.TempDir()
		leafPort := freePort(t)
		if i == 0 {
			b.leafPort = leafPort
		}
		b.layouts = append(b.layouts, Layout{
			ClientListen:    fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			RouteListen:     fmt.Sprintf("127.0.0.1:%d", routePorts[i]),
			MonitorListen:   fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			PidFile:         filepath.Join(dir, "nats.pid"),
			StoreDir:        filepath.Join(dir, "jetstream"),
			ResolverDir:     filepath.Join(dir, "resolver"),
			Routes:          routes,
			TLSDir:          tlsDir,
			LeafnodesListen: fmt.Sprintf("127.0.0.1:%d", leafPort),
			LeafnodesTLSDir: leafTLSDir,
			LeafRemotesDir:  b.remotesDir,
		})
		b.files = append(b.files, filepath.Join(dir, "nats.conf"))
	}
	b.write(t, remotes)
	for i, name := range names {
		o, err := server.ProcessConfigFile(b.files[i])
		require.NoError(t, err)
		o.NoLog, o.NoSigs = true, true
		s, err := server.NewServer(o)
		require.NoError(t, err)
		go s.Start()
		t.Cleanup(s.Shutdown)
		b.srvs = append(b.srvs, s)
		b.eps = append(b.eps, sysobs.Endpoint{Name: name, URL: "http://" + b.layouts[i].MonitorListen})
	}
	for _, s := range b.srvs {
		require.True(t, s.ReadyForConnections(15*time.Second))
	}
	return b
}

// write renders every server's config with remotes and writes it, and the
// leaf remotes Secret's files, returning the first server's config before
// and after.
func (b *booted) write(t *testing.T, remotes []LeafRemote) (from, to []byte) {
	t.Helper()
	if s := leafRemotesSecret(b.nc, remotes); s != nil {
		for k, v := range s.Data {
			require.NoError(t, os.WriteFile(filepath.Join(b.remotesDir, k), v, 0o600))
		}
	}
	for i, name := range serverNames(b.nc) {
		cfg, err := serverConfig(b.nc, Inputs{Trust: b.trust}, name, b.layouts[i], "r1", remotes...).Render()
		require.NoError(t, err)
		if i == 0 {
			from, _ = os.ReadFile(b.files[0])
			to = cfg
		}
		require.NoError(t, os.WriteFile(b.files[i], cfg, 0o600))
	}
	return from, to
}

// reload rewrites every server's config with remotes and reloads it,
// requiring the change to be classified as reloading.
func (b *booted) reload(t *testing.T, remotes []LeafRemote) {
	t.Helper()
	from, to := b.write(t, remotes)
	require.Empty(t, restartReason("2.15.0", from, to))
	for _, s := range b.srvs {
		require.NoError(t, s.Reload())
	}
}

// leafStatusOf observes b's leafnode connections through the monitoring
// endpoints, as the cluster controller does for a leaf without a system
// user, and returns the status it computes.
func (b *booted) leafStatusOf(t require.TestingT, remotes []LeafRemote) clusterv1beta1.NatsClusterStatus {
	plan, err := Render(b.nc, Inputs{Trust: b.trust}, remotes...)
	require.NoError(t, err)
	var st clusterv1beta1.NatsClusterStatus
	leafs, err := sysobs.NewMonitor(http.DefaultClient, 0).Leafz(context.Background(), b.eps)
	leafStatus(&st, b.nc, plan, leafs, err)
	return st
}

// waitConnected waits until every remote is held by every server.
func (b *booted) waitConnected(t *testing.T, remotes []LeafRemote) clusterv1beta1.NatsClusterStatus {
	t.Helper()
	var st clusterv1beta1.NatsClusterStatus
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		st = b.leafStatusOf(ct, remotes)
		assert.True(ct, meta.IsStatusConditionTrue(st.Conditions, ConditionLeafnodesConnected))
	}, 30*time.Second, 200*time.Millisecond, "leaf remotes did not connect")
	return st
}

// hub is story 10's prod-east booted in-process as one server under a
// hand-minted NATS operator, its leafnode listener on a self-signed certificate
// and advertising where it listens, with the telemetry account pushed.
type hub struct {
	*booted
	p         testPlane
	caPEM     []byte
	telemetry jwtplane.Keys
}

func startHub(t *testing.T) *hub {
	t.Helper()
	h := &hub{p: mintPlane(t), telemetry: newTestKeys(t, nkeys.PrefixByteAccount)}
	nc := storyLeafCluster(t, "hub.yaml", "prod-east")
	nc.Spec.Replicas = 1
	nc.Spec.Gateway, nc.Spec.JetStream = nil, nil
	leafTLS := t.TempDir()
	cert, err := selfSignedRouteSecret(nc, []string{"127.0.0.1"}, time.Now())
	require.NoError(t, err)
	for k, v := range cert.Data {
		require.NoError(t, os.WriteFile(filepath.Join(leafTLS, k), v, 0o600))
	}
	h.caPEM = cert.Data[caKey]
	port := freePort(t)
	nc.Spec.Leafnodes.Advertise = fmt.Sprintf("127.0.0.1:%d", port)
	h.booted = bootRenderedAt(t, nc, h.p.trust, leafTLS, port)
	h.push(t, jwtplane.Account{Name: "telemetry", Keys: h.telemetry})
	return h
}

// bootRenderedAt is bootRendered for a single server with its leafnode
// listener on port.
func bootRenderedAt(t *testing.T, nc *clusterv1beta1.NatsCluster, trust *Trust, leafTLSDir string, port int) *booted {
	t.Helper()
	require.EqualValues(t, 1, nc.Spec.Replicas)
	b := &booted{nc: nc, trust: trust, remotesDir: t.TempDir(), leafPort: port}
	dir := t.TempDir()
	route := freePort(t)
	b.layouts = []Layout{{
		ClientListen:    fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		RouteListen:     fmt.Sprintf("127.0.0.1:%d", route),
		MonitorListen:   fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		PidFile:         filepath.Join(dir, "nats.pid"),
		StoreDir:        filepath.Join(dir, "jetstream"),
		ResolverDir:     filepath.Join(dir, "resolver"),
		Routes:          []string{fmt.Sprintf("nats-route://127.0.0.1:%d", route)},
		TLSDir:          writeRouteCert(t, nc),
		LeafnodesListen: fmt.Sprintf("127.0.0.1:%d", port),
		LeafnodesTLSDir: leafTLSDir,
		LeafRemotesDir:  t.TempDir(),
	}}
	b.files = []string{filepath.Join(dir, "nats.conf")}
	b.write(t, nil)
	s := startFile(t, b.files[0])
	b.srvs = []*server.Server{s}
	b.eps = []sysobs.Endpoint{{Name: serverName(nc, 0), URL: "http://" + b.layouts[0].MonitorListen}}
	return b
}

// push signs account under the hub's NATS operator and pushes it over $SYS as
// the auth controller's user.
func (h *hub) push(t *testing.T, account jwtplane.Account) {
	t.Helper()
	accJWT, err := jwtplane.SignAccount(account, h.p.op, time.Now())
	require.NoError(t, err)
	admin, err := natsconn.Dial(natsconn.Endpoint{Servers: []string{h.clientURL(0)}, Creds: h.p.systemCreds(t, jwtplane.PresetAuthController)}, nats.CustomInboxPrefix(jwtplane.InboxPrefix(jwtplane.PresetAuthController)))
	require.NoError(t, err)
	defer admin.Close()
	reply, err := admin.Request("$SYS.REQ.CLAIMS.UPDATE", []byte(accJWT), 2*time.Second)
	require.NoError(t, err)
	var resp server.ServerAPIClaimUpdateResponse
	require.NoError(t, json.Unmarshal(reply.Data, &resp))
	require.Nil(t, resp.Error)
}

func (h *hub) leafURL() string { return fmt.Sprintf("tls://127.0.0.1:%d", h.leafPort) }

// fakeLeafClient holds story 10's leaf-side objects in namespace
// nats-system: the NatsConnections given, Secret hub-ca with the hub's CA,
// and the creds Secrets given by name.
func (h *hub) fakeLeafClient(t *testing.T, creds map[string][]byte, objs ...client.Object) client.Client {
	t.Helper()
	objs = append(objs, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "nats-system", Name: "hub-ca"}, Data: map[string][]byte{caKey: h.caPEM}})
	for name, c := range creds {
		objs = append(objs, credsSecret("nats-system", name, c))
	}
	return fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(objs...).Build()
}

// subscribe subscribes to subject on the hub as a new user of account and
// returns the subscription.
func (h *hub) subscribe(t *testing.T, account jwtplane.Keys, subject string) *nats.Subscription {
	t.Helper()
	conn, err := natsconn.Dial(natsconn.Endpoint{Servers: []string{h.clientURL(0)}, Creds: h.p.creds(t, account, jwtplane.User{Name: "reader"})})
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	sub, err := conn.SubscribeSync(subject)
	require.NoError(t, err)
	require.NoError(t, conn.Flush())
	return sub
}

// requireDelivered publishes on pub until sub receives it.
func requireDelivered(t *testing.T, pub *nats.Conn, sub *nats.Subscription, subject string) {
	t.Helper()
	require.Eventually(t, func() bool {
		if err := pub.Publish(subject, []byte("x")); err != nil {
			return false
		}
		_, err := sub.NextMsg(200 * time.Millisecond)
		return err == nil
	}, 20*time.Second, 50*time.Millisecond, "%s published at the leaf did not reach the hub", subject)
}

// TestLeaf_AccountLess pins story 10's edge-site-1: rendered from
// edge.yaml, its three servers dial the hub rendered from hub.yaml over
// TLS as the narrowed leaf user, a leaf client's telemetry reaches the
// hub's telemetry account, and the status reads every server connected.
func TestLeaf_AccountLess(t *testing.T) {
	h := startHub(t)
	c := h.fakeLeafClient(t,
		map[string][]byte{"edge-site-1-leaf-creds": h.p.creds(t, h.telemetry, leafUser())},
		storyConnection(t, "edge.yaml", "hub", h.leafURL(), true))
	nc := storyLeafCluster(t, "edge.yaml", "edge-site-1")
	remotes, cond, err := readLeafRemotes(context.Background(), c, nc, nil)
	require.NoError(t, err)
	require.Nil(t, cond)
	leaf := bootRendered(t, nc, nil, remotes, "")

	st := leaf.waitConnected(t, remotes)
	require.Equal(t, []clusterv1beta1.LeafRemoteStatus{{ConnectionNamespace: "nats-system", ConnectionName: "hub", Connected: 3, Account: publicKey(t, h.telemetry.Identity)}}, st.LeafRemotes)
	require.Equal(t, "3 of 3 servers connected to 1 remote", meta.FindStatusCondition(st.Conditions, ConditionLeafnodesConnected).Message)

	sub := h.subscribe(t, h.telemetry, "telemetry.>")
	local, err := nats.Connect(leaf.clientURL(1))
	require.NoError(t, err)
	t.Cleanup(local.Close)
	requireDelivered(t, local, sub, "telemetry.site1")
}

// TestLeaf_RemotesReload pins that remotes are added and removed by
// reload on a running leaf, the first and the last included, and that the
// status follows.
func TestLeaf_RemotesReload(t *testing.T) {
	h := startHub(t)
	orders := newTestKeys(t, nkeys.PrefixByteAccount)
	h.push(t, jwtplane.Account{Name: "orders", Keys: orders})
	ordersConn := storyConnection(t, "edge.yaml", "hub", h.leafURL(), true)
	ordersConn.Name = "hub-orders"
	ordersConn.Spec.Credentials.SecretKeyRef.Name = "orders-leaf-creds"
	c := h.fakeLeafClient(t,
		map[string][]byte{
			"edge-site-1-leaf-creds": h.p.creds(t, h.telemetry, leafUser()),
			"orders-leaf-creds":      h.p.creds(t, orders, jwtplane.User{Name: "orders-leaf", Preset: jwtplane.PresetLeafnode}),
		},
		storyConnection(t, "edge.yaml", "hub", h.leafURL(), true), ordersConn)

	nc := storyLeafCluster(t, "edge.yaml", "edge-site-1")
	one := nc.Spec.LeafRemotes
	two := []clusterv1beta1.LeafRemote{one[0], {ConnectionRef: natsv1beta1.ObjectReference{Name: "hub-orders"}}}
	resolve := func(t *testing.T, spec []clusterv1beta1.LeafRemote) []LeafRemote {
		t.Helper()
		nc.Spec.LeafRemotes = spec
		remotes, cond, err := readLeafRemotes(context.Background(), c, nc, nil)
		require.NoError(t, err)
		require.Nil(t, cond)
		return remotes
	}
	spokes := func(t *testing.T, leaf *booted, want int) {
		t.Helper()
		require.Eventually(t, func() bool {
			for _, s := range leaf.srvs {
				if s.NumLeafNodes() != want {
					return false
				}
			}
			return true
		}, 20*time.Second, 100*time.Millisecond, "not %d leaf connections on every server", want)
	}

	nc.Spec.LeafRemotes = nil
	leaf := bootRendered(t, nc, nil, nil, "")
	spokes(t, leaf, 0)

	t.Run("the first remote is added", func(t *testing.T) {
		remotes := resolve(t, one)
		leaf.reload(t, remotes)
		leaf.waitConnected(t, remotes)
	})
	t.Run("a second remote is added", func(t *testing.T) {
		remotes := resolve(t, two)
		leaf.reload(t, remotes)
		st := leaf.waitConnected(t, remotes)
		require.Len(t, st.LeafRemotes, 2)
		require.Equal(t, publicKey(t, orders.Identity), st.LeafRemotes[1].Account)
		require.Equal(t, "3 of 3 servers connected to 2 remotes", meta.FindStatusCondition(st.Conditions, ConditionLeafnodesConnected).Message)
	})
	t.Run("a remote is removed", func(t *testing.T) {
		remotes := resolve(t, one)
		leaf.reload(t, remotes)
		spokes(t, leaf, 1)
		leaf.waitConnected(t, remotes)
	})
	t.Run("the last remote is removed", func(t *testing.T) {
		leaf.reload(t, nil)
		spokes(t, leaf, 0)
	})
}

// operatorLeaf is story 10's edge-site-2 booted against h: it trusts the
// hub's NATS operator, binds one remote to its system account and one to the
// telemetry account, whose JWT it preloads.
func operatorLeaf(t *testing.T, h *hub) (*booted, []LeafRemote) {
	t.Helper()
	telemetryJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: "telemetry", Keys: h.telemetry}, h.p.op, time.Now())
	require.NoError(t, err)
	at := &natsv1beta1.NatsAccountTrust{}
	storyDoc(t, "edge-operator.yaml", "NatsAccountTrust", "telemetry", at)
	at.Spec.PublicKey, at.Spec.JWT = publicKey(t, h.telemetry.Identity), telemetryJWT
	c := h.fakeLeafClient(t,
		map[string][]byte{
			"edge-site-2-system-leaf-creds": h.p.creds(t, h.p.sys, jwtplane.User{Name: "edge-site-2-system", SystemAccount: true, Preset: jwtplane.PresetLeafnode}),
			"edge-site-2-leaf-creds":        h.p.creds(t, h.telemetry, leafUser()),
		},
		storyConnection(t, "edge-operator.yaml", "hub-system", h.leafURL(), true),
		storyConnection(t, "edge-operator.yaml", "hub-telemetry", h.leafURL(), true),
		at)
	nc := storyLeafCluster(t, "edge-operator.yaml", "edge-site-2")
	remotes, cond, err := readLeafRemotes(context.Background(), c, nc, h.p.trust)
	require.NoError(t, err)
	require.Nil(t, cond)
	return bootRendered(t, nc, h.p.trust, remotes, ""), remotes
}

// TestLeaf_OperatorMode pins story 10's edge-site-2 against a hub: remotes
// connect, and preloaded and fetched accounts both authenticate.
func TestLeaf_OperatorMode(t *testing.T) {
	h := startHub(t)
	leaf, remotes := operatorLeaf(t, h)
	telemetry := publicKey(t, h.telemetry.Identity)

	var cfg map[string]any
	b, err := os.ReadFile(leaf.files[0])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &cfg))
	require.Equal(t, "full", cfg["resolver"].(map[string]any)["type"])
	require.Contains(t, cfg["resolver_preload"], telemetry)

	st := leaf.waitConnected(t, remotes)
	require.Equal(t, []clusterv1beta1.LeafRemoteStatus{
		{ConnectionNamespace: "nats-system", ConnectionName: "hub-system", Connected: 3, Account: h.p.trust.SystemAccount},
		{ConnectionNamespace: "nats-system", ConnectionName: "hub-telemetry", Connected: 3, Account: telemetry},
	}, st.LeafRemotes)

	pool := natsconn.NewPool(natsconn.WithPreset(jwtplane.PresetClusterController))
	t.Cleanup(pool.Close)
	hubSys := &SystemConnections{
		Client:  fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(credsSecret(h.nc.Namespace, h.nc.Spec.Auth.SystemCredentials.SecretKeyRef.Name, h.p.systemCreds(t, jwtplane.PresetClusterController))).Build(),
		Pool:    pool,
		Servers: func(*clusterv1beta1.NatsCluster) []string { return []string{h.clientURL(0)} },
	}
	require.Eventually(t, func() bool {
		leafs, err := hubSys.ObserveLeafs(context.Background(), h.nc)
		if err != nil || len(leafs["prod-east-0"]) != 6 {
			return false
		}
		for _, l := range leafs["prod-east-0"] {
			if l.Spoke {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond, "the hub does not report six accepted leaf connections over $SYS")

	sub := h.subscribe(t, h.telemetry, "telemetry.>")
	sensor, err := natsconn.Dial(natsconn.Endpoint{Servers: []string{leaf.clientURL(2)}, Creds: h.p.creds(t, h.telemetry, jwtplane.User{Name: "sensor"})})
	require.NoError(t, err)
	t.Cleanup(sensor.Close)
	requireDelivered(t, sensor, sub, "telemetry.site2")

	orders := newTestKeys(t, nkeys.PrefixByteAccount)
	h.push(t, jwtplane.Account{Name: "orders", Keys: orders})
	conn, err := natsconn.Dial(natsconn.Endpoint{Servers: []string{leaf.clientURL(0)}, Creds: h.p.creds(t, orders, jwtplane.User{Name: "orders"})}, nats.NoReconnect())
	require.NoError(t, err, "an account the leaf never preloaded is not fetched over the system remote")
	conn.Close()
}
