package manager

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authnv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// startEnvtest starts an API server for the test.
func startEnvtest(t *testing.T) *rest.Config {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	return cfg
}

func newScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	return scheme, clientgoscheme.AddToScheme(scheme)
}

// TestEnvtestStart pins that start runs what Setup adds and returns nil once
// its context ends.
func TestEnvtestStart(t *testing.T) {
	cfg := startEnvtest(t)
	ran := make(chan struct{})
	c := Controller{
		Name:      "test-controller",
		Group:     "test.nats.mikluko.io",
		NewScheme: newScheme,
		Setup: func(_ context.Context, mgr ctrl.Manager) error {
			return mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
				close(ran)
				<-ctx.Done()
				return nil
			}))
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- start(ctx, cfg, &Options{MetricsAddr: "0", ProbeAddr: "0"}, c) }()
	select {
	case <-ran:
	case err := <-done:
		require.FailNow(t, "start returned before Setup's runnable ran", "%v", err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "Setup's runnable did not run")
	}
	cancel()
	require.NoError(t, <-done)
}

// TestEnvtestCache pins that New's cache holds only labelled objects of an
// owned kind and Secrets without data or annotations, kept current, while the
// manager's client reads a Secret whole.
func TestEnvtestCache(t *testing.T) {
	cfg := startEnvtest(t)
	scheme, err := newScheme()
	require.NoError(t, err)
	owned := Owned{Label: "test.nats.mikluko.io/owner", Kinds: []client.Object{&corev1.ConfigMap{}}}
	mgr, err := New(cfg, &Options{MetricsAddr: "0", ProbeAddr: "0"}, scheme, owned)
	require.NoError(t, err)
	_, err = mgr.GetCache().GetInformer(t.Context(), &corev1.ConfigMap{}, cache.BlockUntilSynced(false))
	require.NoError(t, err)
	ready := cacheSynced(mgr.GetCache())
	probe := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	require.Error(t, ready(probe), "ready before the cache started")
	go func() { _ = mgr.Start(t.Context()) }()
	require.True(t, mgr.GetCache().WaitForCacheSync(t.Context()))
	require.NoError(t, ready(probe))

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ns := "default"
	labelled := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "labelled", Labels: map[string]string{owned.Label: ""}}}
	unlabelled := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "unlabelled"}}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "creds", Annotations: map[string]string{"kubectl.kubernetes.io/last-applied-configuration": "{}"}},
		Data:       map[string][]byte{"key": []byte("v1")},
	}
	for _, o := range []client.Object{labelled, unlabelled, secret} {
		require.NoError(t, c.Create(t.Context(), o))
	}

	t.Run("owned kind", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			assert.NoError(ct, mgr.GetClient().Get(t.Context(), client.ObjectKeyFromObject(labelled), &corev1.ConfigMap{}))
		}, 10*time.Second, 50*time.Millisecond)
		err := mgr.GetClient().Get(t.Context(), client.ObjectKeyFromObject(unlabelled), &corev1.ConfigMap{})
		require.True(t, apierrors.IsNotFound(err), "%v", err)
	})

	t.Run("secret", func(t *testing.T) {
		secret.Data["key"] = []byte("v2")
		require.NoError(t, c.Update(t.Context(), secret))
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			var cached corev1.Secret
			if assert.NoError(ct, mgr.GetCache().Get(t.Context(), client.ObjectKeyFromObject(secret), &cached)) {
				assert.Equal(ct, secret.ResourceVersion, cached.ResourceVersion)
				assert.Empty(ct, cached.Data)
				assert.Empty(ct, cached.Annotations)
			}
		}, 10*time.Second, 50*time.Millisecond)
		var read corev1.Secret
		require.NoError(t, mgr.GetClient().Get(t.Context(), client.ObjectKeyFromObject(secret), &read))
		require.Equal(t, []byte("v2"), read.Data["key"])
	})
}

// TestEnvtestMetrics pins that the metrics endpoint answers over HTTPS
// only to a token allowed to get /metrics.
func TestEnvtestMetrics(t *testing.T) {
	cfg := startEnvtest(t)
	scheme, err := newScheme()
	require.NoError(t, err)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	mgr, err := New(cfg, &Options{MetricsAddr: addr, ProbeAddr: "0"}, scheme, Owned{})
	require.NoError(t, err)
	go func() { _ = mgr.Start(t.Context()) }()

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "scraper"}}
	require.NoError(t, c.Create(t.Context(), sa))
	tr := &authnv1.TokenRequest{}
	require.NoError(t, c.SubResource("token").Create(t.Context(), sa, tr))
	token := tr.Status.Token
	require.NotEmpty(t, token)

	httpc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	get := func(token string) (int, error) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+addr+"/metrics", nil)
		if err != nil {
			return 0, err
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := httpc.Do(req)
		if err != nil {
			return 0, err
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode, nil
	}
	requireStatus := func(t *testing.T, token string, want int) {
		t.Helper()
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			code, err := get(token)
			if assert.NoError(ct, err) {
				assert.Equal(ct, want, code)
			}
		}, 10*time.Second, 100*time.Millisecond)
	}

	requireStatus(t, "", http.StatusUnauthorized)
	requireStatus(t, "not-a-token", http.StatusUnauthorized)
	requireStatus(t, token, http.StatusForbidden)

	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "metrics-reader"},
		Rules:      []rbacv1.PolicyRule{{NonResourceURLs: []string{"/metrics"}, Verbs: []string{"get"}}},
	}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "metrics-reader"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: sa.Namespace, Name: sa.Name}},
	}
	require.NoError(t, c.Create(t.Context(), role))
	require.NoError(t, c.Create(t.Context(), binding))
	requireStatus(t, token, http.StatusOK)
}
