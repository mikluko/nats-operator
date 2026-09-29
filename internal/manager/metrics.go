package manager

import (
	"crypto/tls"
	"fmt"
	"path/filepath"

	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

const (
	metricsCertName = "tls.crt"
	metricsKeyName  = "tls.key"
)

// metricsOptions serves metrics on addr over HTTPS, only to a bearer token
// of a user the API server allows the request's verb on the request's path
// as a non-resource URL. With certDir set, it serves the key pair certDir
// holds and fails unless that pair loads, where controller-runtime would
// fall back to a self-signed certificate.
func metricsOptions(addr, certDir string) (metricsserver.Options, error) {
	o := metricsserver.Options{
		BindAddress:    addr,
		SecureServing:  true,
		FilterProvider: filters.WithAuthenticationAndAuthorization,
	}
	if certDir == "" {
		return o, nil
	}
	if _, err := tls.LoadX509KeyPair(filepath.Join(certDir, metricsCertName), filepath.Join(certDir, metricsKeyName)); err != nil {
		return metricsserver.Options{}, fmt.Errorf("metrics certificate: %w", err)
	}
	o.CertDir, o.CertName, o.KeyName = certDir, metricsCertName, metricsKeyName
	return o, nil
}
