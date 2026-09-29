package fixtures

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

// TestMetricsTLS pins the metrics Secret: owner-readable, a TLS Secret whose
// key matches its certificate, which chains to ca.crt under every DNS name
// and no other.
func TestMetricsTLS(t *testing.T) {
	dir := t.TempDir()
	names := []string{"r-cluster-controller-metrics.ns.svc", "r-auth-controller-metrics.ns.svc"}
	path := filepath.Join(dir, "metrics-tls.yaml")
	require.NoError(t, MetricsTLS(path, "r-metrics-tls", "ns", names))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode())

	s := decodeDir(t, dir).secrets["r-metrics-tls"]
	require.NotNil(t, s)
	require.Equal(t, "ns", s.Namespace)
	require.Equal(t, corev1.SecretTypeTLS, s.Type)
	pair, err := tls.X509KeyPair([]byte(s.StringData[corev1.TLSCertKey]), []byte(s.StringData[corev1.TLSPrivateKeyKey]))
	require.NoError(t, err, "the key matches its certificate")
	cert, err := x509.ParseCertificate(pair.Certificate[0])
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM([]byte(s.StringData["ca.crt"])))
	for _, n := range names {
		_, err = cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: n})
		require.NoError(t, err, n)
	}
	_, err = cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: "r-jetstream-controller-metrics.ns.svc"})
	require.Error(t, err)
}
