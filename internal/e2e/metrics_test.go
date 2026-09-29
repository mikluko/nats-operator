package e2e

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/mikluko/nats-operator/internal/e2e/fixtures"
)

const serviceMonitor = `apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: r-cluster-controller-metrics
  namespace: ns
spec:
  selector:
    matchLabels: {app.kubernetes.io/name: cluster-controller}
  endpoints:
    - port: metrics
      path: /metrics
      scheme: https
      bearerTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
      tlsConfig:
        ca:
          secret: {name: tls, key: ca.crt}
        serverName: r-cluster-controller-metrics.ns.svc
    - port: otel-metrics
      path: /metrics
      scheme: http
---
apiVersion: v1
kind: Service
metadata:
  name: r-cluster-controller-metrics
  namespace: ns
`

func TestMetricsEndpoints(t *testing.T) {
	objs, err := DecodeObjects([]byte(serviceMonitor))
	require.NoError(t, err)
	eps, err := MetricsEndpoints(objs)
	require.NoError(t, err)
	require.Equal(t, []MetricsEndpoint{{
		Namespace:       "ns",
		Monitor:         "r-cluster-controller-metrics",
		Selector:        map[string]string{"app.kubernetes.io/name": "cluster-controller"},
		Port:            "metrics",
		Path:            "/metrics",
		CA:              SecretKey{Name: "tls", Key: "ca.crt"},
		ServerName:      "r-cluster-controller-metrics.ns.svc",
		BearerTokenFile: "/var/run/secrets/kubernetes.io/serviceaccount/token",
	}}, eps)

	insecure := strings.Replace(serviceMonitor, `ca:
          secret: {name: tls, key: ca.crt}
        serverName: r-cluster-controller-metrics.ns.svc`, "insecureSkipVerify: true", 1)
	objs, err = DecodeObjects([]byte(insecure))
	require.NoError(t, err)
	_, err = MetricsEndpoints(objs)
	require.ErrorContains(t, err, "endpoint 0 does not verify")
}

// metricsSecret returns the stringData of the Secret secret that
// fixtures.MetricsTLS generates for name.
func metricsSecret(t *testing.T, secret, name string) map[string]string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tls.yaml")
	require.NoError(t, fixtures.MetricsTLS(path, secret, "ns", []string{name}))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	objs, err := DecodeObjects(raw)
	require.NoError(t, err)
	require.Len(t, objs, 1)
	data, _, err := unstructured.NestedStringMap(objs[0].Object, "stringData")
	require.NoError(t, err)
	return data
}

// TestScrapeCommand pins that a scrape succeeds only with the token, against
// a certificate that chains to the CA file and names serverName.
func TestScrapeCommand(t *testing.T) {
	curl, err := exec.LookPath("curl")
	require.NoError(t, err, "TestScrapeCommand runs the scrape with curl")
	const name = "r-cluster-controller-metrics.ns.svc"
	secret := metricsSecret(t, "tls", name)
	pair, err := tls.X509KeyPair([]byte(secret["tls.crt"]), []byte(secret["tls.key"]))
	require.NoError(t, err)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" || r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte("# TYPE up gauge\nup 1\n"))
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	srv.StartTLS()
	defer srv.Close()
	at := netip.MustParseAddrPort(srv.Listener.Addr().String())

	dir := t.TempDir()
	file := func(name, content string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		return path
	}
	ca, otherCA := file("ca.crt", secret["ca.crt"]), file("other.crt", metricsSecret(t, "other", name)["ca.crt"])
	token, otherToken := file("token", "tok\n"), file("other-token", "nope")

	for desc, tc := range map[string]struct {
		serverName, ca, token string
		exit                  int
	}{
		"verified":      {name, ca, token, 0},
		"another CA":    {name, otherCA, token, ExitUnverified},
		"another name":  {"other.ns.svc", ca, token, ExitUnverified},
		"another token": {name, ca, otherToken, 22},
	} {
		t.Run(desc, func(t *testing.T) {
			args := ScrapeCommand(at.Addr(), int32(at.Port()), tc.serverName, "/metrics", tc.ca, tc.token)
			require.Equal(t, "curl", args[0])
			out, err := exec.CommandContext(t.Context(), curl, args[1:]...).CombinedOutput()
			if tc.exit == 0 {
				require.NoError(t, err, string(out))
				return
			}
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(out))
			require.Equal(t, tc.exit, exit.ExitCode(), string(out))
		})
	}
}
