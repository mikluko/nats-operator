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
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/mikluko/nats-operator/internal/manager/secretreads"
)

// startEnvtest starts an API server for the test, its admin's config in
// the returned Config.
func startEnvtest(t *testing.T) *envtest.Environment {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{}
	_, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	return env
}

// TestEnvtestStart pins that start runs what Setup adds and returns nil once
// its context ends.
func TestEnvtestStart(t *testing.T) {
	cfg := startEnvtest(t).Config
	ran := make(chan struct{})
	c := Controller{
		Name:  "test-controller",
		Group: "test.nats.mikluko.io",
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
// owned kind and the metadata of Secrets without annotations, kept current,
// while the manager's client reads a Secret whole and never lists or watches
// whole Secrets.
func TestEnvtestCache(t *testing.T) {
	cfg := startEnvtest(t).Config
	scheme, err := NewScheme()
	require.NoError(t, err)
	owned := Owned{Label: "test.nats.mikluko.io/owner", Kinds: []client.Object{&corev1.ConfigMap{}}}
	recorded, reads := secretreads.Record(cfg)
	mgr, err := New(recorded, &Options{MetricsAddr: "0", ProbeAddr: "0"}, scheme, owned)
	require.NoError(t, err)
	_, err = mgr.GetCache().GetInformer(t.Context(), &corev1.ConfigMap{}, cache.BlockUntilSynced(false))
	require.NoError(t, err)
	ready := cacheSynced(mgr.GetCache().WaitForCacheSync)
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
			cached := &metav1.PartialObjectMetadata{}
			cached.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
			if assert.NoError(ct, mgr.GetCache().Get(t.Context(), client.ObjectKeyFromObject(secret), cached)) {
				assert.Equal(ct, secret.ResourceVersion, cached.ResourceVersion)
				assert.Empty(ct, cached.Annotations)
			}
		}, 10*time.Second, 50*time.Millisecond)
		var read corev1.Secret
		require.NoError(t, mgr.GetClient().Get(t.Context(), client.ObjectKeyFromObject(secret), &read))
		require.Equal(t, []byte("v2"), read.Data["key"])
		reads.RequireMetadataOnly(t)
	})
}

// TestEnvtestMetrics pins that the metrics endpoint answers over HTTPS
// only to a token allowed to get /metrics; a malformed token is answered
// 500, not 401.
func TestEnvtestMetrics(t *testing.T) {
	cfg := startEnvtest(t).Config
	scheme, err := NewScheme()
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
	tokenOf := func(name string) (*corev1.ServiceAccount, string) {
		sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name}}
		require.NoError(t, c.Create(t.Context(), sa))
		tr := &authnv1.TokenRequest{}
		require.NoError(t, c.SubResource("token").Create(t.Context(), sa, tr))
		require.NotEmpty(t, tr.Status.Token)
		return sa, tr.Status.Token
	}
	sa, token := tokenOf("scraper")
	_, stranger := tokenOf("stranger")

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
	requireStatus(t, "not-a-token", http.StatusInternalServerError)
	requireStatus(t, stranger, http.StatusForbidden)

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

// TestEnvtestReadyUnelected pins that a replica another holds the lease
// against is ready only once it can list and watch what its controllers
// watch.
func TestEnvtestReadyUnelected(t *testing.T) {
	env := startEnvtest(t)
	cfg := env.Config
	scheme, err := NewScheme()
	require.NoError(t, err)
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	const ns, id = "default", "test.nats.mikluko.io"
	now := metav1.NewMicroTime(time.Now())
	require.NoError(t, admin.Create(t.Context(), &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: id},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       new("another-replica"),
			LeaseDurationSeconds: new(int32(3600)),
			AcquireTime:          &now,
			RenewTime:            &now,
		},
	}))
	replica, err := env.AddUser(envtest.User{Name: "replica"}, cfg)
	require.NoError(t, err)
	grant := func(name string, rule rbacv1.PolicyRule) {
		role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: []rbacv1.PolicyRule{rule}}
		binding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name},
			Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: "replica"}},
		}
		require.NoError(t, admin.Create(t.Context(), role))
		require.NoError(t, admin.Create(t.Context(), binding))
	}
	grant("leases", rbacv1.PolicyRule{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch"}})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	opts, err := managerOptions(&Options{MetricsAddr: "0", ProbeAddr: addr, LeaderElection: true, LeaderElectionID: id}, scheme, Owned{})
	require.NoError(t, err)
	opts.LeaderElectionNamespace = ns
	mgr, err := newManager(replica.Config(), opts)
	require.NoError(t, err)
	require.NoError(t, ctrl.NewControllerManagedBy(mgr).
		For(&corev1.ConfigMap{}).
		Named("unelected").
		Complete(reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		})))
	ready := func() bool {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/readyz", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}
	go func() { _ = mgr.Start(t.Context()) }()

	require.Never(t, ready, 5*time.Second, 20*time.Millisecond, "ready without list and watch on ConfigMaps")
	grant("configmaps", rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"list", "watch"}})
	require.Eventually(t, ready, 60*time.Second, 100*time.Millisecond)
	select {
	case <-mgr.Elected():
		require.FailNow(t, "elected against another replica's lease")
	default:
	}
}

// TestEnvtestReleasesLease pins that an elected replica gives up its lease
// as Start returns.
func TestEnvtestReleasesLease(t *testing.T) {
	cfg := startEnvtest(t).Config
	scheme, err := NewScheme()
	require.NoError(t, err)
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	const ns, id = "default", "release.nats.mikluko.io"
	opts, err := managerOptions(&Options{MetricsAddr: "0", ProbeAddr: "0", LeaderElection: true, LeaderElectionID: id}, scheme, Owned{})
	require.NoError(t, err)
	opts.LeaderElectionNamespace = ns
	mgr, err := newManager(cfg, opts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	select {
	case <-mgr.Elected():
	case <-time.After(60 * time.Second):
		require.FailNow(t, "not elected")
	}
	var lease coordinationv1.Lease
	require.NoError(t, admin.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: id}, &lease))
	require.NotEmpty(t, ptr.Deref(lease.Spec.HolderIdentity, ""))

	cancel()
	require.NoError(t, <-done)
	require.NoError(t, admin.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: id}, &lease))
	require.Empty(t, ptr.Deref(lease.Spec.HolderIdentity, ""), "the lease is still held")
}
