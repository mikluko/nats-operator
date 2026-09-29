package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	jsv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
)

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
		{name: "errors until the deadline", failures: 1 << 30, want: "  no status read before the deadline\n  last error: get ConfigMap a/x: net/http: TLS handshake timeout\n"},
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
			start := time.Now()
			o, err := r.poll(t.Context(), stage{at: "01-x step 1", wait: r.Timeout, shares: []share{sh}}, [][]string{nil})
			require.NoError(t, err)
			require.Equal(t, outcome{diff: tt.want}, o)
			require.Less(t, time.Since(start), 2*r.Timeout, "retries end at the step's wait")
		})
	}
}

func TestPoll_Publish(t *testing.T) {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "x"}, Data: map[string]string{"v": "1"}}
	target := &unstructured.Unstructured{}
	target.SetAPIVersion("v1")
	target.SetKind("ConfigMap")
	target.SetNamespace("a")
	target.SetName("x")
	exp := Expectation{File: "01-live-configmap.yaml", Want: map[string]any{"data": map[string]any{"v": "2"}}}
	for _, tt := range []struct {
		name    string
		publish bool
		fail    error
		want    string
	}{
		{name: "unset", want: "  01-live-configmap.yaml -> ConfigMap a/x\n    .data.v: want \"2\", got \"1\"\n"},
		{name: "set", publish: true, want: "  01-live-configmap.yaml -> ConfigMap a/x\n    .data.v: want \"2\", got \"1\"\n"},
		{name: "failing", publish: true, fail: errors.New("coredns unreachable"),
			want: "  no status read before the deadline\n  last error: coredns unreachable\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var reads []string
			c := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(cm.DeepCopy()).
				WithInterceptorFuncs(interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					reads = append(reads, key.String())
					return c.Get(ctx, key, obj, opts...)
				}}).Build()
			r := &Runner{Clients: []client.Client{c}, Timeout: 100 * time.Millisecond, Interval: 10 * time.Millisecond}
			rounds := 0
			if tt.publish {
				r.Publish = func(_ context.Context, clients []client.Client) error {
					require.Equal(t, r.Clients, clients)
					rounds++
					return tt.fail
				}
			}
			sh := share{client: c, step: Step{Expectations: []Expectation{exp}}, targets: []*unstructured.Unstructured{target}}
			o, err := r.poll(t.Context(), stage{at: "01-x step 1", wait: r.Timeout, shares: []share{sh}}, [][]string{nil})
			require.NoError(t, err)
			require.Equal(t, outcome{diff: tt.want}, o)
			if tt.publish {
				require.Greater(t, rounds, 1)
			}
			if tt.fail != nil {
				require.Empty(t, reads, "a round whose Publish fails reads nothing")
				return
			}
			require.NotEmpty(t, reads)
			for _, k := range reads {
				require.Equal(t, "a/x", k)
			}
		})
	}
}

// writeStories writes one story directory per entry of stories, each file
// of an entry into its directory, and returns their root.
func writeStories(t *testing.T, stories map[string]map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for dir, files := range stories {
		require.NoError(t, os.Mkdir(filepath.Join(root, dir), 0o755))
		for name, body := range files {
			require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, dir, name)), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(root, dir, name), []byte(body), 0o600))
		}
	}
	return root
}

func TestRunner_Plan(t *testing.T) {
	cm := func(ns, name string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + name + ", namespace: " + ns + "}\ndata: {v: \"1\"}\n"
	}
	const live = "data: {v: \"1\"}\n"
	root := writeStories(t, map[string]map[string]string{
		"01-base": {
			"01-cm.yaml":             cm("a", "x"),
			"01-live-configmap.yaml": live,
			"02-delete-cm.yaml":      cm("a", "x"),
		},
		"02-chained": {
			"index.md":                   "---\nparams:\n  e2e:\n    after: 1\n    waits:\n      - {step: 1, wait: 3m, reason: slow}\n---\n",
			"01-cm.yaml":                 cm("b", "bee"),
			"01-live-configmap-bee.yaml": live,
			"e2e/00-fixture.yaml":        cm("c", "z"),
		},
		"03-placed": {
			"index.md": "---\nparams:\n  e2e:\n    clusters:\n      - {name: east, files: [01-east.yaml]}\n" +
				"      - {name: west, files: [01-west.yaml, 01-live-configmap.yaml]}\n---\n",
			"01-east.yaml":           cm("east", "e"),
			"01-west.yaml":           cm("west", "w"),
			"01-live-configmap.yaml": live,
		},
		"04-skipped": {
			"index.md":   "---\nparams:\n  e2e:\n    skip: needs a GPU\n---\n",
			"01-cm.yaml": cm("a", "x"),
		},
		"05-after-skipped": {
			"index.md":   "---\nparams:\n  e2e:\n    after: 4\n---\n",
			"01-cm.yaml": cm("a", "x"),
		},
		"06-unmatched": {
			"01-cm.yaml":               cm("a", "x"),
			"01-live-configmap-y.yaml": live,
			"01-other.yaml":            cm("a", "other"),
		},
	})
	bundles, err := LoadBundles(root)
	require.NoError(t, err)
	byNumber := func(n int) *Bundle {
		i := slices.IndexFunc(bundles, func(b *Bundle) bool { return b.Number == n })
		require.GreaterOrEqual(t, i, 0)
		return bundles[i]
	}

	render := func(stages []stage) []string {
		var out []string
		for _, st := range stages {
			line := fmt.Sprintf("%s %s:", st.at, st.wait)
			for _, sh := range st.shares {
				line += fmt.Sprintf(" [%d%s", sh.cluster, sh.where)
				for _, tg := range sh.targets {
					line += " " + key(tg)
				}
				line += "]"
			}
			out = append(out, line)
		}
		return out
	}
	for _, tt := range []struct {
		name       string
		story      int
		clusters   int
		stages     []string
		namespaces [][]string
		wheres     []string
		skip       string
		err        string
	}{
		{
			name: "one bundle", story: 1, clusters: 2,
			stages:     []string{"01-base step 1 1m30s: [0 a/x]", "01-base step 2 1m30s: [0]"},
			namespaces: [][]string{{"a"}, nil}, wheres: []string{"", ""},
		},
		{
			name: "chained after its base", story: 2, clusters: 1,
			stages: []string{
				"01-base step 1 1m30s: [0 a/x]", "01-base step 2 1m30s: [0]",
				"02-chained step 0 1m30s: [0]", "02-chained step 1 3m0s: [0 b/bee]",
			},
			namespaces: [][]string{{"a", "b", "c"}}, wheres: []string{""},
		},
		{
			name: "placed in two clusters", story: 3, clusters: 2,
			stages:     []string{"03-placed step 1 1m30s: [0 in east] [1 in west west/w]"},
			namespaces: [][]string{{"east"}, {"west"}}, wheres: []string{" in east", " in west"},
		},
		{name: "too few clusters", story: 3, clusters: 1, skip: "needs 2 Kubernetes clusters, the run has 1"},
		{name: "skipped", story: 4, clusters: 1, skip: "needs a GPU"},
		{name: "starts from a skipped story", story: 5, clusters: 1, skip: "starts from 04-skipped, which is skipped: needs a GPU"},
		{name: "expectation naming no object", story: 6, clusters: 1, err: `06-unmatched: 01-live-configmap-y.yaml names no object of the bundle`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &Runner{Clients: make([]client.Client, tt.clusters), Timeout: 90 * time.Second}
			pl, err := r.plan(byNumber(tt.story))
			switch {
			case tt.skip != "":
				require.Equal(t, skipped(tt.skip), err)
				return
			case tt.err != "":
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.stages, render(pl.stages))
			require.Equal(t, tt.namespaces, pl.namespaces)
			require.Equal(t, tt.wheres, pl.wheres)
		})
	}
}

// TestRetainJetStream_MissingKind pins that a JetStream kind the API server
// does not serve leaves the kinds after it retained.
func TestRetainJetStream_MissingKind(t *testing.T) {
	s := runtime.NewScheme()
	require.NoError(t, jsv1beta1.AddToScheme(s))
	consumer := &jsv1beta1.NatsConsumer{ObjectMeta: metav1.ObjectMeta{Namespace: "guarded", Name: "audit"}}
	consumer.Spec.DeletionPolicy = jsv1beta1.DeletionDelete
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(consumer).
		WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if gvk := list.GetObjectKind().GroupVersionKind(); gvk.Kind == jetStreamKinds[0]+"List" {
				return &meta.NoKindMatchError{GroupKind: gvk.GroupKind(), SearchedVersions: []string{gvk.Version}}
			}
			return c.List(ctx, list, opts...)
		}}).
		Build()
	require.Equal(t, "NatsStream", jetStreamKinds[0])

	require.NoError(t, retainJetStream(t.Context(), c, "guarded"))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(consumer), consumer))
	require.Equal(t, jsv1beta1.DeletionRetain, consumer.Spec.DeletionPolicy)
}
