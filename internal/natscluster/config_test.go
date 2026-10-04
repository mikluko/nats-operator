package natscluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/natstest"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// storyCluster reads story 1's NatsCluster manifest.
func storyCluster(t *testing.T) *clusterv1beta1.NatsCluster {
	t.Helper()
	b, err := os.ReadFile("../../docs/content/docs/stories/01-quickstart/01-natscluster.yaml")
	require.NoError(t, err)
	nc := &clusterv1beta1.NatsCluster{}
	require.NoError(t, yaml.UnmarshalStrict(b, nc))
	return nc
}

func quantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

func TestDeriveLimits(t *testing.T) {
	gi := int64(1) << 30
	tests := []struct {
		name       string
		mutate     func(*clusterv1beta1.NatsClusterSpec)
		mem, file  string
		goMemLimit int64
	}{
		{"story 1", func(*clusterv1beta1.NatsClusterSpec) {}, "3Gi", "19Gi", 4 * gi * 9 / 10},
		{"overrides", func(s *clusterv1beta1.NatsClusterSpec) {
			s.JetStream.Limits = &clusterv1beta1.JetStreamLimits{MaxMemoryStore: quantity("1Gi"), MaxFileStore: quantity("10Gi")}
		}, "1Gi", "10Gi", 4 * gi * 9 / 10},
		{"no memory limit", func(s *clusterv1beta1.NatsClusterSpec) {
			s.Resources.Limits = nil
		}, "", "19Gi", 0},
		{"no volume", func(s *clusterv1beta1.NatsClusterSpec) {
			s.JetStream.VolumeClaimTemplate = nil
		}, "3Gi", "", 4 * gi * 9 / 10},
		{"no JetStream", func(s *clusterv1beta1.NatsClusterSpec) {
			s.JetStream = nil
		}, "3Gi", "", 4 * gi * 9 / 10},
	}
	str := func(q *resource.Quantity) string {
		if q == nil {
			return ""
		}
		return q.String()
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := storyCluster(t)
			tt.mutate(&nc.Spec)
			l := deriveLimits(&nc.Spec)
			require.Equal(t, tt.mem, str(l.JetStream.MaxMemoryStore))
			require.Equal(t, tt.file, str(l.JetStream.MaxFileStore))
			require.Equal(t, tt.goMemLimit, l.GoMemLimit)
		})
	}
}

func TestServerConfig(t *testing.T) {
	gi := int64(1) << 30
	story := storyCluster(t)
	tests := []struct {
		name   string
		mutate func(*clusterv1beta1.NatsCluster)
		want   string
	}{
		{"story 1", func(*clusterv1beta1.NatsCluster) {}, fmt.Sprintf(`{
			"server_name": "demo-1",
			"listen": "0.0.0.0:4222",
			"http": "0.0.0.0:8222",
			"pid_file": "/var/run/nats/nats.pid",
			"lame_duck_duration": "2m0s",
			"lame_duck_grace_period": "10s",
			"server_metadata": {"config_revision": "r1"},
			"cluster": {
				"name": "demo",
				"listen": "0.0.0.0:6222",
				"routes": [
					"nats-route://demo-0-0.demo-headless.nats-system.svc:6222",
					"nats-route://demo-1-0.demo-headless.nats-system.svc:6222",
					"nats-route://demo-2-0.demo-headless.nats-system.svc:6222"
				],
				"tls": {
					"cert_file": "/etc/nats-routes-tls/tls.crt",
					"key_file": "/etc/nats-routes-tls/tls.key",
					"ca_file": "/etc/nats-routes-tls/ca.crt",
					"verify": true
				}
			},
			"jetstream": {"store_dir": "/data/jetstream", "max_memory_store": %d, "max_file_store": %d}
		}`, 3*gi, 19*gi)},
		{"tags, domain, route TLS off, no JetStream limits", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Replicas = 1
			nc.Spec.ServerTags = map[string]string{"zone": "a", "az": "1"}
			nc.Spec.Routes = &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{Enabled: ptr.To(false)}}
			nc.Spec.Resources = corev1.ResourceRequirements{}
			nc.Spec.JetStream = &clusterv1beta1.JetStream{Domain: "leaf"}
		}, `{
			"server_name": "demo-1",
			"listen": "0.0.0.0:4222",
			"http": "0.0.0.0:8222",
			"pid_file": "/var/run/nats/nats.pid",
			"lame_duck_duration": "2m0s",
			"lame_duck_grace_period": "10s",
			"server_tags": ["az:1", "zone:a"],
			"server_metadata": {"config_revision": "r1"},
			"cluster": {
				"name": "demo",
				"listen": "0.0.0.0:6222",
				"routes": ["nats-route://demo-0-0.demo-headless.nats-system.svc:6222"]
			},
			"jetstream": {"store_dir": "/data/jetstream", "domain": "leaf"}
		}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := story.DeepCopy()
			tt.mutate(nc)
			b, err := serverConfig(nc, Inputs{}, "demo-1", podLayout(nc), "r1").Render()
			require.NoError(t, err)
			require.JSONEq(t, tt.want, string(b))
		})
	}
}

// startRendered boots every server of nc from its config rendered under
// trust, on loopback ports the servers bind and temporary directories, with
// each of override applied to the parsed options, and returns their
// monitoring endpoints, the servers, the first server's client URL and each
// server's config file, rewritten with its bound ports and every route.
func startRendered(t *testing.T, nc *clusterv1beta1.NatsCluster, trust *Trust, revision string, override ...func(*server.Options)) ([]sysobs.Endpoint, []*server.Server, string, []string) {
	t.Helper()
	return startRenderedWith(t, nc, Inputs{Trust: trust}, revision, nil, override...)
}

// startRenderedWith is startRendered from in, with layout, when not nil,
// applied to the i-th server's Layout before its config is rendered; a
// gateway listener layout sets is bound by the server. The monitoring port
// stays server-picked in the rewritten files, since nats-server refuses to
// reload a changed one.
func startRenderedWith(t *testing.T, nc *clusterv1beta1.NatsCluster, in Inputs, revision string, layout func(i int, l *Layout), override ...func(*server.Options)) ([]sysobs.Endpoint, []*server.Server, string, []string) {
	t.Helper()
	tlsDir := t.TempDir()
	secret, err := selfSignedRouteSecret(nc, []string{"127.0.0.1"}, time.Now())
	require.NoError(t, err)
	for k, v := range secret.Data {
		require.NoError(t, os.WriteFile(filepath.Join(tlsDir, k), v, 0o600))
	}

	names := serverNames(nc)
	ports := make([]natstest.Ports, len(names))
	files := make([]string, len(names))
	render := func(i int) {
		dir := filepath.Dir(files[i])
		l := Layout{
			ClientListen:  natstest.Listen(ports[i].Client),
			RouteListen:   natstest.Listen(ports[i].Route),
			MonitorListen: natstest.Listen(0),
			PidFile:       filepath.Join(dir, "nats.pid"),
			StoreDir:      filepath.Join(dir, "jetstream"),
			ResolverDir:   filepath.Join(dir, "resolver"),
			Routes:        boundRoutes(ports),
			TLSDir:        tlsDir,
		}
		if layout != nil {
			layout(i, &l)
		}
		if l.GatewayListen != "" {
			l.GatewayListen = natstest.Listen(ports[i].Gateway)
		}
		cfg, err := serverConfig(nc, in, names[i], l, revision).Render()
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(files[i], cfg, 0o600))
	}
	parsed := func(o *server.Options) { require.NotNil(t, o.Cluster.TLSConfig, "route TLS not parsed") }

	var eps []sysobs.Endpoint
	var srvs []*server.Server
	for i, name := range names {
		files[i] = filepath.Join(t.TempDir(), "nats.conf")
		render(i)
		s := natstest.Start(t, files[i], append([]func(*server.Options){parsed}, override...)...)
		ports[i] = s.Bound(t)
		srvs = append(srvs, s.Server)
		eps = append(eps, sysobs.Endpoint{Name: name, URL: "http://" + s.MonitorAddr().String()})
	}
	for i := range names {
		render(i)
	}
	require.Eventually(t, func() bool {
		for _, s := range srvs {
			if s.NumRoutes() < len(srvs)-1 {
				return false
			}
		}
		return true
	}, 15*time.Second, 100*time.Millisecond, "routes over self-signed TLS did not form")
	return eps, srvs, natstest.URL(ports[0].Client), files
}

// boundRoutes are the route URLs of the bound route ports of ports, or
// [natstest.Unroutable] where none is.
func boundRoutes(ports []natstest.Ports) []string {
	var out []string
	for _, p := range ports {
		if p.Route != 0 {
			out = append(out, fmt.Sprintf("nats-route://127.0.0.1:%d", p.Route))
		}
	}
	if len(out) == 0 {
		return []string{natstest.Unroutable}
	}
	return out
}

// TestRenderedConfigRunsCluster pins that nats-server 2.15.0 runs story 1's
// rendered config as a NATS cluster observed Settled.
func TestRenderedConfigRunsCluster(t *testing.T) {
	nc := storyCluster(t)
	nc.Spec.JetStream.Limits = &clusterv1beta1.JetStreamLimits{MaxMemoryStore: quantity("256Mi"), MaxFileStore: quantity("1Gi")}
	eps, srvs, url, _ := startRendered(t, nc, nil, "r1")
	for _, s := range srvs {
		cfg := s.JetStreamConfig()
		require.NotNil(t, cfg)
		require.Equal(t, int64(256)<<20, cfg.MaxMemory)
		require.Equal(t, int64(1)<<30, cfg.MaxStore)
	}

	conn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	js, err := conn.JetStream()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := js.AddStream(&nats.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}, Replicas: 3})
		return err == nil
	}, 30*time.Second, 200*time.Millisecond)

	o := sysobs.NewMonitor(http.DefaultClient, 0)
	require.Eventually(t, func() bool {
		s, err := o.Observe(context.Background(), eps)
		if err != nil || !s.Verdict().Settled() || len(s.Groups) != 2 {
			return false
		}
		for _, srv := range s.Servers {
			if srv.Metadata[MetadataConfigRevision] != "r1" || srv.Version != "2.15.0" {
				return false
			}
		}
		return true
	}, 30*time.Second, 200*time.Millisecond)
}

func TestRoutesSecret(t *testing.T) {
	tests := []struct {
		name   string
		routes *clusterv1beta1.Routes
		secret string
		issuer bool
	}{
		{"self-signed by default", nil, "demo-routes-tls", false},
		{"self-signed with an empty tls block", &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{}}, "demo-routes-tls", false},
		{"disabled", &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{Enabled: ptr.To(false)}}, "", false},
		{"named Secret", &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{CertificateSource: clusterv1beta1.CertificateSource{
			SecretRef: &natsv1beta1.SecretReference{Name: "mine"},
		}}}, "mine", false},
		{"cert-manager", &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{CertificateSource: clusterv1beta1.CertificateSource{
			CertManager: &clusterv1beta1.CertManagerCertificate{IssuerRef: clusterv1beta1.IssuerReference{Name: "ca"}},
		}}}, "demo-routes-tls", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := storyCluster(t)
			nc.Spec.Routes = tt.routes
			require.Equal(t, tt.secret, routesSecret(nc))
			require.Equal(t, tt.issuer, certManagerIssuer(nc) != nil)
		})
	}
}

func TestRoutesCertificate(t *testing.T) {
	nc := storyCluster(t)
	got := routesCertificate(nc, &clusterv1beta1.IssuerReference{Name: "ca", Kind: "ClusterIssuer"})
	b, err := json.Marshal(got.Object)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"apiVersion": "cert-manager.io/v1",
		"kind": "Certificate",
		"metadata": {"name": "demo-routes", "namespace": "nats-system", "labels": {
			"app.kubernetes.io/instance": "demo", "app.kubernetes.io/managed-by": "cluster-controller",
			"app.kubernetes.io/name": "nats", "cluster.nats-operator.io/cluster": "demo"}},
		"spec": {
			"secretName": "demo-routes-tls",
			"dnsNames": ["*.demo-headless.nats-system.svc", "*.demo-headless.nats-system.svc.cluster.local"],
			"usages": ["server auth", "client auth"],
			"privateKey": {"algorithm": "ECDSA", "size": 256},
			"issuerRef": {"name": "ca", "kind": "ClusterIssuer", "group": "cert-manager.io"}
		}
	}`, string(b))
}
