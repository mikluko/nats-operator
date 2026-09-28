package natscluster

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// TestReloadAllowLists_EverySupportedVersion pins a reload allow-list for
// every minor version from 2.15 up to the linked nats-server, and that the
// linked version's list names only Options fields, an unexported one only for
// the key reloading the system account's entry.
func TestReloadAllowLists_EverySupportedVersion(t *testing.T) {
	linked := minorVersion(server.VERSION)
	require.NotEmpty(t, linked, server.VERSION)
	major, minor, _ := strings.Cut(linked, ".")
	last, err := strconv.Atoi(minor)
	require.NoError(t, err)
	for m := 15; m <= last; m++ {
		v := major + "." + strconv.Itoa(m)
		require.NotEmpty(t, reloadAllowLists[v], "no reload allow-list for nats-server %s", v)
	}

	exported := map[string]bool{}
	opts := reflect.TypeFor[server.Options]()
	for i := range opts.NumField() {
		f := opts.Field(i)
		exported[strings.ToLower(f.Name)] = f.IsExported()
	}
	for _, k := range reloadAllowLists[linked] {
		want, ok := exported[k.Case]
		require.True(t, ok, "%s: %q is not an Options field", k.Path, k.Case)
		require.Equal(t, k.Rule != reloadSystemAccountEntry, want, "%s: %q", k.Path, k.Case)
	}
}

func TestMinorVersion(t *testing.T) {
	for in, want := range map[string]string{"2.15.0": "2.15", "v2.16.3": "2.16", "2.15": "2.15", "latest": "", "2": "", "": ""} {
		require.Equal(t, want, minorVersion(in), in)
	}
}

// configCase is a change to a rendered config: base prepares the config a
// server runs, mutate turns it into the one rendered next.
type configCase struct {
	name   string
	base   func(t *testing.T, m map[string]any)
	mutate func(t *testing.T, m map[string]any)
	reason string
}

func setPath(m map[string]any, path string, v any) {
	parts := strings.Split(path, ".")
	for _, p := range parts[:len(parts)-1] {
		next, ok := m[p].(map[string]any)
		if !ok {
			next = map[string]any{}
			m[p] = next
		}
		m = next
	}
	if v == nil {
		delete(m, parts[len(parts)-1])
		return
	}
	m[parts[len(parts)-1]] = v
}

// testGateway gives the config a gateway listening on a free port, with one
// remote at remoteURL.
func testGateway(t *testing.T, remoteURL string) func(*testing.T, map[string]any) {
	port := freePort(t)
	return func(t *testing.T, m map[string]any) {
		setPath(m, "gateway", map[string]any{
			"name":     "demo",
			"listen":   fmt.Sprintf("127.0.0.1:%d", port),
			"gateways": []any{map[string]any{"name": "west", "urls": []any{remoteURL}}},
		})
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, l.Close()) }()
	return l.Addr().(*net.TCPAddr).Port
}

// writeRouteCert writes a fresh self-signed route certificate into a new
// directory and returns it.
func writeRouteCert(t *testing.T, nc *clusterv1beta1.NatsCluster) string {
	t.Helper()
	dir := t.TempDir()
	secret, err := selfSignedRouteSecret(nc, []string{"127.0.0.1"}, time.Now())
	require.NoError(t, err)
	for k, v := range secret.Data {
		require.NoError(t, os.WriteFile(filepath.Join(dir, k), v, 0o600))
	}
	return dir
}

// configCases are the changes both the classification table and the
// running server judge. Each builds on story 1's rendered config with
// memory and file store limits set.
func configCases(t *testing.T, nc *clusterv1beta1.NatsCluster) []configCase {
	none := func(*testing.T, map[string]any) {}
	newCert := func(t *testing.T, m map[string]any) {
		dir := writeRouteCert(t, nc)
		setPath(m, "cluster.tls.cert_file", filepath.Join(dir, "tls.crt"))
		setPath(m, "cluster.tls.key_file", filepath.Join(dir, "tls.key"))
		setPath(m, "cluster.tls.ca_file", filepath.Join(dir, "ca.crt"))
	}
	otherRemote := func(_ *testing.T, m map[string]any) {
		setPath(m, "gateway.gateways", []any{map[string]any{"name": "west", "urls": []any{"nats://127.0.0.1:1"}}})
	}
	set := func(path string, v any) func(*testing.T, map[string]any) {
		return func(_ *testing.T, m map[string]any) { setPath(m, path, v) }
	}
	return []configCase{
		{"TLS certificate", none, newCert, ""},
		{"server tags", none, set("server_tags", []any{"az:b"}), ""},
		{"config revision", none, set("server_metadata.config_revision", "r2"), ""},
		{"routes", none, func(_ *testing.T, m map[string]any) {
			setPath(m, "cluster.routes", append(m["cluster"].(map[string]any)["routes"].([]any), "nats-route://127.0.0.1:1"))
		}, ""},
		{"max_payload", none, set("max_payload", 2<<20), ""},
		{"memory store raised", none, set("jetstream.max_memory_store", 512<<20), ""},
		{"memory store lowered", none, set("jetstream.max_memory_store", 128<<20), "jetstream.max_memory_store is restart-only"},
		{"file store unset", none, set("jetstream.max_file_store", nil), "jetstream.max_file_store is restart-only"},
		{"domain", none, set("jetstream.domain", "hub"), "jetstream.domain is restart-only"},
		{"store_dir", none, func(t *testing.T, m map[string]any) {
			setPath(m, "jetstream.store_dir", t.TempDir())
		}, "jetstream.store_dir is restart-only"},
		{"server_name", none, set("server_name", "other"), "server_name is restart-only"},
		{"gateway added", none, testGateway(t, "nats://127.0.0.1:1"), "gateway is restart-only"},
		{"gateway remote", testGateway(t, "nats://127.0.0.1:2"), otherRemote, "gateway.gateways is restart-only"},
		{"TLS certificate and gateway remote", testGateway(t, "nats://127.0.0.1:2"), func(t *testing.T, m map[string]any) {
			newCert(t, m)
			otherRemote(t, m)
		}, "gateway.gateways is restart-only"},
		{"tags and domain", none, func(t *testing.T, m map[string]any) {
			setPath(m, "server_tags", []any{"az:b"})
			setPath(m, "jetstream.domain", "hub")
		}, "jetstream.domain is restart-only"},
		{"route listener and server name", none, func(t *testing.T, m map[string]any) {
			setPath(m, "cluster.listen", fmt.Sprintf("127.0.0.1:%d", freePort(t)))
			setPath(m, "server_name", "other")
		}, "cluster.listen, server_name are restart-only"},
	}
}

// renderedMap renders server demo-0 of nc under a loopback layout with
// route TLS from tlsDir, as a decoded config.
func renderedMap(t *testing.T, nc *clusterv1beta1.NatsCluster, tlsDir string) map[string]any {
	return renderedTrustMap(t, nc, nil, tlsDir)
}

// renderedTrustMap is renderedMap under trust.
func renderedTrustMap(t *testing.T, nc *clusterv1beta1.NatsCluster, trust *Trust, tlsDir string) map[string]any {
	t.Helper()
	dir := t.TempDir()
	route := freePort(t)
	l := Layout{
		ClientListen:  fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		RouteListen:   fmt.Sprintf("127.0.0.1:%d", route),
		MonitorListen: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		PidFile:       filepath.Join(dir, "nats.pid"),
		StoreDir:      filepath.Join(dir, "jetstream"),
		ResolverDir:   filepath.Join(dir, "resolver"),
		Routes:        []string{fmt.Sprintf("nats-route://127.0.0.1:%d", route)},
		TLSDir:        tlsDir,
	}
	b, err := serverConfig(nc, Inputs{Trust: trust}, "demo-0", l, "r1").Render()
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	return m
}

func limitedStoryCluster(t *testing.T) *clusterv1beta1.NatsCluster {
	nc := storyCluster(t)
	nc.Spec.JetStream.Limits = &clusterv1beta1.JetStreamLimits{MaxMemoryStore: quantity("256Mi"), MaxFileStore: quantity("1Gi")}
	return nc
}

func encode(t *testing.T, m map[string]any) []byte {
	t.Helper()
	b, err := json.MarshalIndent(m, "", "  ")
	require.NoError(t, err)
	return b
}

// TestRestartReason pins the classification of rendered-config changes
// on nats-server 2.15.
func TestRestartReason(t *testing.T) {
	nc := limitedStoryCluster(t)
	tlsDir := writeRouteCert(t, nc)
	for _, tt := range configCases(t, nc) {
		t.Run(tt.name, func(t *testing.T) {
			m := renderedMap(t, nc, tlsDir)
			tt.base(t, m)
			from := encode(t, m)
			tt.mutate(t, m)
			require.Equal(t, tt.reason, restartReason("2.15.0", from, encode(t, m)))
		})
	}

	t.Run("resolver", func(t *testing.T) {
		base := map[string]any{"server_name": "a"}
		withResolver := map[string]any{"server_name": "a", "resolver": map[string]any{"type": "full", "dir": "/data/jwt"}}
		changed := map[string]any{"server_name": "a", "resolver": map[string]any{"type": "full", "dir": "/data/jwt", "allow_delete": true}}
		require.Equal(t, "resolver is restart-only", restartReason("2.15.1", encode(t, base), encode(t, withResolver)))
		require.Equal(t, "resolver is restart-only", restartReason("2.15.1", encode(t, withResolver), encode(t, base)))
		require.Equal(t, "resolver.allow_delete is restart-only", restartReason("2.15.1", encode(t, withResolver), encode(t, changed)))
	})
	t.Run("resolver_preload", func(t *testing.T) {
		from := map[string]any{"resolver_preload": map[string]any{"A": "jwt1"}}
		to := map[string]any{"resolver_preload": map[string]any{"A": "jwt2"}}
		require.Equal(t, "resolver_preload.A is restart-only", restartReason("2.15.0", encode(t, from), encode(t, to)))

		from["system_account"], to["system_account"] = "A", "A"
		require.Empty(t, restartReason("2.15.0", encode(t, from), encode(t, to)))

		from["resolver_preload"] = map[string]any{"A": "jwt1", "B": "jwt1"}
		to["resolver_preload"] = map[string]any{"A": "jwt2", "B": "jwt2"}
		require.Equal(t, "resolver_preload.B is restart-only", restartReason("2.15.0", encode(t, from), encode(t, to)))

		to["system_account"] = "B"
		require.Equal(t, "resolver_preload.A, resolver_preload.B, system_account are restart-only", restartReason("2.15.0", encode(t, from), encode(t, to)))
	})
	t.Run("version without a list", func(t *testing.T) {
		b := encode(t, map[string]any{"server_tags": []any{"a"}})
		require.Equal(t, "nats-server 2.99.0 has no reload allow-list", restartReason("2.99.0", b, b))
	})
	t.Run("no change", func(t *testing.T) {
		b := encode(t, renderedMap(t, nc, tlsDir))
		require.Empty(t, restartReason("2.15.0", b, b))
	})
}

// TestRestartReason_AgreesWithServer runs every classified change against
// nats-server itself: a change classified as reloading reloads, and leaves
// the server reporting configDigest of the new file; one classified as
// restart-only is rejected by the reload.
func TestRestartReason_AgreesWithServer(t *testing.T) {
	nc := limitedStoryCluster(t)
	tlsDir := writeRouteCert(t, nc)
	for _, tt := range configCases(t, nc) {
		t.Run(tt.name, func(t *testing.T) {
			m := renderedMap(t, nc, tlsDir)
			tt.base(t, m)
			f := filepath.Join(t.TempDir(), "nats.conf")
			require.NoError(t, os.WriteFile(f, encode(t, m), 0o600))
			o, err := server.ProcessConfigFile(f)
			require.NoError(t, err)
			o.NoLog, o.NoSigs = true, true
			s, err := server.NewServer(o)
			require.NoError(t, err)
			go s.Start()
			t.Cleanup(s.Shutdown)
			require.True(t, s.ReadyForConnections(10*time.Second))

			tt.mutate(t, m)
			next := encode(t, m)
			require.NoError(t, os.WriteFile(f, next, 0o600))
			err = s.Reload()
			if tt.reason != "" {
				require.Error(t, err, "nats-server reloaded a change classified restart-only")
				return
			}
			require.NoError(t, err)
			v, err := s.Varz(nil)
			require.NoError(t, err)
			want, err := configDigest(next)
			require.NoError(t, err)
			require.Equal(t, want, v.ConfigDigest)
		})
	}
}
