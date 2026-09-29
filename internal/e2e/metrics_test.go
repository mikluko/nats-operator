package e2e

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"os"
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
		Namespace:  "ns",
		Monitor:    "r-cluster-controller-metrics",
		Selector:   map[string]string{"app.kubernetes.io/name": "cluster-controller"},
		Port:       "metrics",
		Path:       "/metrics",
		CA:         SecretKey{Name: "tls", Key: "ca.crt"},
		ServerName: "r-cluster-controller-metrics.ns.svc",
	}}, eps)

	insecure := strings.Replace(serviceMonitor, `ca:
          secret: {name: tls, key: ca.crt}
        serverName: r-cluster-controller-metrics.ns.svc`, "insecureSkipVerify: true", 1)
	objs, err = DecodeObjects([]byte(insecure))
	require.NoError(t, err)
	_, err = MetricsEndpoints(objs)
	require.ErrorContains(t, err, "endpoint 0 does not verify")
}

// metricsSecret returns the stringData of a Secret fixtures.MetricsTLS
// generates for name.
func metricsSecret(t *testing.T, name string) map[string]string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tls.yaml")
	require.NoError(t, fixtures.MetricsTLS(path, "tls", "ns", []string{name}))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	objs, err := DecodeObjects(raw)
	require.NoError(t, err)
	require.Len(t, objs, 1)
	data, _, err := unstructured.NestedStringMap(objs[0].Object, "stringData")
	require.NoError(t, err)
	return data
}

// TestScrape pins that a scrape succeeds only with a token, against a
// certificate that chains to roots and names serverName.
func TestScrape(t *testing.T) {
	const name = "r-cluster-controller-metrics.ns.svc"
	secret := metricsSecret(t, name)
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
	pool := func(ca string) *x509.CertPool {
		p := x509.NewCertPool()
		require.True(t, p.AppendCertsFromPEM([]byte(ca)))
		return p
	}
	roots, otherRoots := pool(secret["ca.crt"]), pool(metricsSecret(t, name)["ca.crt"])
	addr := srv.Listener.Addr().String()

	body, err := Scrape(t.Context(), addr, "/metrics", roots, name, "tok")
	require.NoError(t, err)
	require.Contains(t, string(body), "up 1")

	for desc, tc := range map[string]struct {
		roots      *x509.CertPool
		serverName string
		token      string
		want       string
	}{
		"another CA":    {otherRoots, name, "tok", "certificate signed by unknown authority"},
		"another name":  {roots, "other.ns.svc", "tok", "not other.ns.svc"},
		"another token": {roots, name, "nope", "403 Forbidden"},
	} {
		t.Run(desc, func(t *testing.T) {
			_, err := Scrape(t.Context(), addr, "/metrics", tc.roots, tc.serverName, tc.token)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
