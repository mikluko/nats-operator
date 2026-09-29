// Package secretreads records the lists and watches on Secrets a client
// makes, for tests that pin them metadata-only.
package secretreads

import (
	"mime"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

const (
	envWait = 30 * time.Second
	envTick = 100 * time.Millisecond
)

// Reads records the list and watch requests on Secrets that pass through a
// rest.Config Record wraps.
type Reads struct {
	mu   sync.Mutex
	meta int
	full []string
}

// Record returns a copy of cfg whose requests r records.
func Record(cfg *rest.Config) (*rest.Config, *Reads) {
	r := &Reads{}
	out := rest.CopyConfig(cfg)
	out.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripFunc(func(req *http.Request) (*http.Response, error) {
			r.record(req)
			return rt.RoundTrip(req)
		})
	})
	return out, r
}

// RequireMetadataOnly waits for a metadata-only list or watch on Secrets and
// fails the test for any list or watch that asked for whole Secrets.
func (r *Reads) RequireMetadataOnly(t *testing.T) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		r.mu.Lock()
		defer r.mu.Unlock()
		assert.Positive(ct, r.meta, "metadata-only lists and watches on Secrets")
	}, envWait, envTick)
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Empty(t, r.full, "lists and watches on whole Secrets")
}

func (r *Reads) record(req *http.Request) {
	if req.Method != http.MethodGet || !secretCollection(req.URL.Path) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if metadataOnly(req.Header.Values("Accept")) {
		r.meta++
		return
	}
	r.full = append(r.full, req.URL.String())
}

// secretCollection reports whether path addresses the Secrets of the
// Kubernetes cluster or of a namespace rather than one Secret.
func secretCollection(path string) bool {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case slices.Equal(segs, []string{"api", "v1", "secrets"}):
		return true
	case len(segs) == 5 && segs[0] == "api" && segs[1] == "v1" && segs[2] == "namespaces" && segs[4] == "secrets":
		return true
	}
	return false
}

// metadataOnly reports whether the first media type accept names asks for
// PartialObjectMetadata or PartialObjectMetadataList, as client-go's metadata
// client does ahead of its plain JSON fallback.
func metadataOnly(accept []string) bool {
	if len(accept) == 0 {
		return false
	}
	first, _, _ := strings.Cut(accept[0], ",")
	_, params, err := mime.ParseMediaType(strings.TrimSpace(first))
	if err != nil {
		return false
	}
	switch params["as"] {
	case "PartialObjectMetadata", "PartialObjectMetadataList":
		return true
	}
	return false
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }
