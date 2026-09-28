package manager

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestAuthorize(t *testing.T) {
	alice := authnv1.UserInfo{Username: "alice", UID: "u1", Groups: []string{"g"}, Extra: map[string]authnv1.ExtraValue{"k": {"v"}}}
	tests := []struct {
		name       string
		header     string
		authn      bool
		allowed    bool
		reviewErr  error
		wantStatus int
		wantSAR    bool
	}{
		{name: "no header", wantStatus: http.StatusUnauthorized},
		{name: "not bearer", header: "Basic abc", wantStatus: http.StatusUnauthorized},
		{name: "empty bearer", header: "Bearer ", wantStatus: http.StatusUnauthorized},
		{name: "unauthenticated", header: "Bearer t", wantStatus: http.StatusUnauthorized},
		{name: "forbidden", header: "Bearer t", authn: true, wantStatus: http.StatusForbidden, wantSAR: true},
		{name: "allowed", header: "bearer t", authn: true, allowed: true, wantStatus: http.StatusOK, wantSAR: true},
		{name: "review fails", header: "Bearer t", reviewErr: errors.New("down"), wantStatus: http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sar *authzv1.SubjectAccessReview
			c := interceptor.NewClient(fake.NewClientBuilder().Build(), interceptor.Funcs{
				Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
					if tt.reviewErr != nil {
						return tt.reviewErr
					}
					switch o := obj.(type) {
					case *authnv1.TokenReview:
						require.Equal(t, "t", o.Spec.Token)
						o.Status.Authenticated = tt.authn
						if tt.authn {
							o.Status.User = alice
						}
					case *authzv1.SubjectAccessReview:
						sar = o
						o.Status.Allowed = tt.allowed
					}
					return nil
				},
			})
			h := authorize(c, logr.Discard(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, tt.wantStatus, rec.Code)
			if !tt.wantSAR {
				require.Nil(t, sar)
				return
			}
			require.Equal(t, authzv1.SubjectAccessReviewSpec{
				User:                  "alice",
				UID:                   "u1",
				Groups:                []string{"g"},
				Extra:                 map[string]authzv1.ExtraValue{"k": {"v"}},
				NonResourceAttributes: &authzv1.NonResourceAttributes{Path: "/metrics", Verb: "get"},
			}, sar.Spec)
		})
	}
}
