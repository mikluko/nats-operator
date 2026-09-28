package manager

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-logr/logr"
	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// metricsOptions serves metrics on addr over HTTPS, behind metricsAuth.
// Without a certificate in controller-runtime's default directory, the
// server's certificate is self-signed.
func metricsOptions(addr string) metricsserver.Options {
	return metricsserver.Options{BindAddress: addr, SecureServing: true, FilterProvider: metricsAuth}
}

// metricsAuth is a metrics server filter provider for authorize, reviewing
// through the API server cfg reaches.
func metricsAuth(cfg *rest.Config, httpClient *http.Client) (metricsserver.Filter, error) {
	c, err := client.New(cfg, client.Options{HTTPClient: httpClient})
	if err != nil {
		return nil, fmt.Errorf("metrics review client: %w", err)
	}
	return func(log logr.Logger, h http.Handler) (http.Handler, error) {
		return authorize(c, log, h), nil
	}, nil
}

// authorize serves a request through h only for a bearer token the API
// server authenticates, of a user it allows the request's method, in lower
// case, on the request's path as a non-resource URL. It answers 401 to a
// request without an authenticated token, 403 to one without the grant, and
// 500 when a review fails. Every request costs a TokenReview and a
// SubjectAccessReview.
func authorize(c client.Client, log logr.Logger, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, err := authenticate(r.Context(), c, r.Header.Get("Authorization"))
		if err != nil {
			log.Error(err, "metrics request")
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		if user == nil {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		sar := &authzv1.SubjectAccessReview{Spec: authzv1.SubjectAccessReviewSpec{
			User:                  user.Username,
			UID:                   user.UID,
			Groups:                user.Groups,
			Extra:                 extra(user.Extra),
			NonResourceAttributes: &authzv1.NonResourceAttributes{Path: r.URL.Path, Verb: strings.ToLower(r.Method)},
		}}
		if err := c.Create(r.Context(), sar); err != nil {
			log.Error(err, "metrics request: review access")
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		if !sar.Status.Allowed {
			http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// authenticate returns the user the bearer token in header authenticates
// as, or nil when header carries no bearer token or the API server does not
// authenticate it.
func authenticate(ctx context.Context, c client.Client, header string) (*authnv1.UserInfo, error) {
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return nil, nil
	}
	tr := &authnv1.TokenReview{Spec: authnv1.TokenReviewSpec{Token: token}}
	if err := c.Create(ctx, tr); err != nil {
		return nil, fmt.Errorf("review token: %w", err)
	}
	if !tr.Status.Authenticated {
		return nil, nil
	}
	return &tr.Status.User, nil
}

func extra(in map[string]authnv1.ExtraValue) map[string]authzv1.ExtraValue {
	if in == nil {
		return nil
	}
	out := make(map[string]authzv1.ExtraValue, len(in))
	for k, v := range in {
		out[k] = authzv1.ExtraValue(v)
	}
	return out
}
