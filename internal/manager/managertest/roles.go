package managertest

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	ctrlmanager "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	"github.com/mikluko/nats-operator/internal/manager"
)

// ReadyUnderRoles fails the test unless setup's manager, confined to two
// namespaces and run by a user holding only the ClusterRole of
// rbacDir/cluster-scoped.yaml and, in each namespace, the Role of
// rbacDir/namespaced.yaml, passes /readyz within a minute. It runs against
// an API server serving the CRDs under crdDir, and skips without
// KUBEBUILDER_ASSETS. Its controllers skip controller-runtime's process-wide
// name check, so another manager in the test binary may register the same
// names.
func ReadyUnderRoles(t *testing.T, scheme *runtime.Scheme, owned manager.Owned, setup func(context.Context, ctrlmanager.Manager) error, rbacDir, crdDir string) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{crdDir}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	const user = "controller"
	namespaces := []string{"watched-a", "watched-b"}
	subjects := []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}}
	var cluster rbacv1.ClusterRole
	readRole(t, filepath.Join(rbacDir, "cluster-scoped.yaml"), &cluster)
	cluster.ObjectMeta = metav1.ObjectMeta{Name: user}
	require.NoError(t, admin.Create(t.Context(), &cluster))
	require.NoError(t, admin.Create(t.Context(), &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: user},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: user},
		Subjects:   subjects,
	}))
	for _, ns := range namespaces {
		require.NoError(t, admin.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		var role rbacv1.Role
		readRole(t, filepath.Join(rbacDir, "namespaced.yaml"), &role)
		role.ObjectMeta = metav1.ObjectMeta{Namespace: ns, Name: user}
		require.NoError(t, admin.Create(t.Context(), &role))
		require.NoError(t, admin.Create(t.Context(), &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: user},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: user},
			Subjects:   subjects,
		}))
	}
	controller, err := env.AddUser(envtest.User{Name: user}, cfg)
	require.NoError(t, err)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	mgr, err := manager.New(controller.Config(), &manager.Options{MetricsAddr: "0", ProbeAddr: addr, WatchNamespaces: namespaces}, scheme, owned)
	require.NoError(t, err)
	require.NoError(t, setup(t.Context(), unvalidated{mgr}))
	exited := make(chan error, 1)
	go func() { exited <- mgr.Start(t.Context()) }()

	deadline := time.After(time.Minute)
	for {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/readyz", nil)
		require.NoError(t, err)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case err := <-exited:
			require.FailNow(t, "manager exited before it was ready", "%v", err)
		case <-deadline:
			require.FailNow(t, "manager not ready within a minute")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// readRole reads the role controller-gen wrote at path into role.
func readRole(t *testing.T, path string, role any) {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, yaml.Unmarshal(b, role))
}

// unvalidated is a manager whose controllers skip the name check.
type unvalidated struct{ ctrlmanager.Manager }

func (u unvalidated) GetControllerOptions() config.Controller {
	o := u.Manager.GetControllerOptions()
	o.SkipNameValidation = new(true)
	return o
}
