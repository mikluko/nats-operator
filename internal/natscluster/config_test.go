package natscluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
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
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// storyCluster reads story 1's NatsCluster manifest.
func storyCluster(t *testing.T) *clusterv1beta1.NatsCluster {
	t.Helper()
	b, err := os.ReadFile("../../docs/content/stories/01-quickstart/natscluster.yaml")
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
			b, err := serverConfig(nc, "demo-1", podLayout(nc), "r1").Render()
			require.NoError(t, err)
			require.JSONEq(t, tt.want, string(b))
		})
	}
}

// startRendered boots every server of nc from its rendered config, on
// loopback ports and temporary directories, and returns their monitoring
// endpoints, the servers and the first server's client URL.
func startRendered(t *testing.T, nc *clusterv1beta1.NatsCluster, revision string) ([]sysobs.Endpoint, []*server.Server, string) {
	t.Helper()
	free := func() int {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() { require.NoError(t, l.Close()) }()
		return l.Addr().(*net.TCPAddr).Port
	}
	names := serverNames(nc)
	var client, route, monitor []int
	var routes []string
	for range names {
		client, route, monitor = append(client, free()), append(route, free()), append(monitor, free())
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", route[len(route)-1]))
	}

	tlsDir := t.TempDir()
	secret, err := selfSignedRouteSecret(nc, []string{"127.0.0.1"}, time.Now())
	require.NoError(t, err)
	for k, v := range secret.Data {
		require.NoError(t, os.WriteFile(filepath.Join(tlsDir, k), v, 0o600))
	}

	var eps []sysobs.Endpoint
	var srvs []*server.Server
	for i, name := range names {
		dir := t.TempDir()
		l := Layout{
			ClientListen:  fmt.Sprintf("127.0.0.1:%d", client[i]),
			RouteListen:   fmt.Sprintf("127.0.0.1:%d", route[i]),
			MonitorListen: fmt.Sprintf("127.0.0.1:%d", monitor[i]),
			PidFile:       filepath.Join(dir, "nats.pid"),
			StoreDir:      filepath.Join(dir, "jetstream"),
			Routes:        routes,
			TLSDir:        tlsDir,
		}
		cfg, err := serverConfig(nc, name, l, revision).Render()
		require.NoError(t, err)
		f := filepath.Join(dir, "nats.conf")
		require.NoError(t, os.WriteFile(f, cfg, 0o600))
		o, err := server.ProcessConfigFile(f)
		require.NoError(t, err)
		require.NotNil(t, o.Cluster.TLSConfig, "route TLS not parsed")
		o.NoLog, o.NoSigs = true, true
		s, err := server.NewServer(o)
		require.NoError(t, err)
		go s.Start()
		t.Cleanup(s.Shutdown)
		srvs = append(srvs, s)
		eps = append(eps, sysobs.Endpoint{Name: name, URL: fmt.Sprintf("http://127.0.0.1:%d", monitor[i])})
	}
	for _, s := range srvs {
		require.True(t, s.ReadyForConnections(15*time.Second))
	}
	require.Eventually(t, func() bool {
		for _, s := range srvs {
			if s.NumRoutes() < len(srvs)-1 {
				return false
			}
		}
		return true
	}, 15*time.Second, 100*time.Millisecond, "routes over self-signed TLS did not form")
	return eps, srvs, fmt.Sprintf("nats://127.0.0.1:%d", client[0])
}

// TestRenderedConfigRunsCluster pins that story 1's rendered config is one
// nats-server 2.15.0 accepts: the servers route to each other over
// self-signed TLS, JetStream takes the derived limits, each reports its
// config revision, and the cluster is observed Settled without an auth
// plane.
func TestRenderedConfigRunsCluster(t *testing.T) {
	nc := storyCluster(t)
	nc.Spec.JetStream.Limits = &clusterv1beta1.JetStreamLimits{MaxMemoryStore: quantity("256Mi"), MaxFileStore: quantity("1Gi")}
	eps, srvs, url := startRendered(t, nc, "r1")
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
			"app.kubernetes.io/name": "nats", "cluster.nats.mikluko.io/cluster": "demo"}},
		"spec": {
			"secretName": "demo-routes-tls",
			"dnsNames": ["*.demo-headless.nats-system.svc", "*.demo-headless.nats-system.svc.cluster.local"],
			"usages": ["server auth", "client auth"],
			"privateKey": {"algorithm": "ECDSA", "size": 256},
			"issuerRef": {"name": "ca", "kind": "ClusterIssuer", "group": "cert-manager.io"}
		}
	}`, string(b))
}
