package manager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	certutil "k8s.io/client-go/util/cert"
)

// writeKeyPair writes a self-signed key pair for host into a new directory as
// tls.crt and tls.key and returns the directory and the certificate's PEM.
func writeKeyPair(t *testing.T, host string) (string, []byte) {
	t.Helper()
	crt, key, err := certutil.GenerateSelfSignedCertKey(host, nil, nil)
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, metricsCertName), crt, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, metricsKeyName), key, 0o600))
	return dir, crt
}

func TestMetricsOptions(t *testing.T) {
	valid, _ := writeKeyPair(t, "metrics.example")
	keyless, _ := writeKeyPair(t, "metrics.example")
	require.NoError(t, os.Remove(filepath.Join(keyless, metricsKeyName)))
	mismatched, _ := writeKeyPair(t, "metrics.example")
	other, _ := writeKeyPair(t, "metrics.example")
	require.NoError(t, os.Rename(filepath.Join(other, metricsKeyName), filepath.Join(mismatched, metricsKeyName)))

	tests := []struct {
		name    string
		certDir string
		wantDir string
		wantErr bool
	}{
		{name: "self-signed", certDir: ""},
		{name: "key pair", certDir: valid, wantDir: valid},
		{name: "missing directory", certDir: filepath.Join(t.TempDir(), "absent"), wantErr: true},
		{name: "missing key", certDir: keyless, wantErr: true},
		{name: "mismatched key", certDir: mismatched, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := metricsOptions(":8080", tt.certDir)
			if tt.wantErr {
				require.ErrorContains(t, err, "metrics certificate")
				return
			}
			require.NoError(t, err)
			require.True(t, got.SecureServing)
			require.NotNil(t, got.FilterProvider)
			require.Equal(t, tt.wantDir, got.CertDir)
			if tt.wantDir != "" {
				require.Equal(t, "tls.crt", got.CertName)
				require.Equal(t, "tls.key", got.KeyName)
			}
		})
	}
}
