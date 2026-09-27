package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// TestPoll_RetriesErrors pins that an API error during a round is retried
// rather than failing the step, and is reported when the deadline passes.
func TestPoll_RetriesErrors(t *testing.T) {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "x"}, Data: map[string]string{"v": "1"}}
	target := &unstructured.Unstructured{}
	target.SetAPIVersion("v1")
	target.SetKind("ConfigMap")
	target.SetNamespace("a")
	target.SetName("x")
	stalled := errors.New("net/http: TLS handshake timeout")
	for _, tt := range []struct {
		name     string
		failures int
		want     string
	}{
		{name: "errors then holds", failures: 2, want: ""},
		{name: "errors until the deadline", failures: 1 << 30, want: "  no status read before the deadline\n  last error: get coredns ConfigMap: net/http: TLS handshake timeout\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			failures := tt.failures
			c := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(cm.DeepCopy()).
				WithInterceptorFuncs(interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if failures > 0 {
						failures--
						return stalled
					}
					return c.Get(ctx, key, obj, opts...)
				}}).Build()
			r := &Runner{Clients: []client.Client{c}, Timeout: 500 * time.Millisecond, Interval: 10 * time.Millisecond}
			sh := share{client: c, step: Step{Expectations: []Expectation{{File: "01-live-configmap.yaml", Want: map[string]any{"data": map[string]any{"v": "1"}}}}},
				targets: []*unstructured.Unstructured{target}}
			diff, err := r.poll(t.Context(), []share{sh})
			require.NoError(t, err)
			require.Equal(t, tt.want, diff)
		})
	}
}
