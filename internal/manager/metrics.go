package manager

import (
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// metricsOptions serves metrics on addr over HTTPS, only to a bearer token
// the API server authenticates, of a user it allows the request's verb on
// the request's path as a non-resource URL. A token's identity is cached for
// a minute, an allow for five and a denial for thirty seconds, so a grant
// or its revocation takes that long to show. Without a certificate in
// controller-runtime's default directory, the server's certificate is
// self-signed.
func metricsOptions(addr string) metricsserver.Options {
	return metricsserver.Options{
		BindAddress:    addr,
		SecureServing:  true,
		FilterProvider: filters.WithAuthenticationAndAuthorization,
	}
}
