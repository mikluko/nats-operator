package manager

import (
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// metricsOptions serves metrics on addr over HTTPS, only to a bearer token
// of a user the API server allows the request's verb on the request's path
// as a non-resource URL. A revocation takes up to five minutes to show and a
// grant up to thirty seconds, as long as controller-runtime caches an allow
// and a denial.
func metricsOptions(addr string) metricsserver.Options {
	return metricsserver.Options{
		BindAddress:    addr,
		SecureServing:  true,
		FilterProvider: filters.WithAuthenticationAndAuthorization,
	}
}
