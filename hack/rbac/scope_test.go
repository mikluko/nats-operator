package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"
)

func TestSplitRules(t *testing.T) {
	rules := []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get"}},
		{APIGroups: []string{"authentication.k8s.io"}, Resources: []string{"tokenreviews"}, Verbs: []string{"create"}},
		{APIGroups: []string{"authorization.k8s.io"}, Resources: []string{"localsubjectaccessreviews", "subjectaccessreviews"}, Verbs: []string{"create"}},
	}
	cluster, namespaced := splitRules(rules)
	require.Equal(t, []rbacv1.PolicyRule{
		{APIGroups: []string{"authentication.k8s.io"}, Resources: []string{"tokenreviews"}, Verbs: []string{"create"}},
		{APIGroups: []string{"authorization.k8s.io"}, Resources: []string{"subjectaccessreviews"}, Verbs: []string{"create"}},
	}, cluster)
	require.Equal(t, []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get"}},
		{APIGroups: []string{"authorization.k8s.io"}, Resources: []string{"localsubjectaccessreviews"}, Verbs: []string{"create"}},
	}, namespaced)
}

// TestEnvtestScopes pins clusterScoped to the API server's discovery: every
// resource a generated role names is cluster-scoped exactly when
// clusterScoped holds it, and so is every entry of clusterScoped. A resource
// envtest does not serve, cert-manager's, goes unchecked.
func TestEnvtestScopes(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(root, "config", "crd")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	require.NoError(t, err)
	_, lists, err := dc.ServerGroupsAndResources()
	require.NoError(t, err)
	namespaced := map[string]bool{}
	for _, l := range lists {
		group, _, _ := strings.Cut(l.GroupVersion, "/")
		if !strings.Contains(l.GroupVersion, "/") {
			group = ""
		}
		for _, r := range l.APIResources {
			namespaced[group+"/"+r.Name] = r.Namespaced
		}
	}
	for key := range clusterScoped {
		ns, known := namespaced[key]
		require.True(t, known, "%s is not served", key)
		require.False(t, ns, "%s is namespaced", key)
	}
	owned, err := ownedPackages(root)
	require.NoError(t, err)
	for c := range owned {
		b, err := os.ReadFile(filepath.Join(root, "config", "rbac", c, "role.yaml"))
		require.NoError(t, err)
		var role rbacv1.ClusterRole
		require.NoError(t, yaml.Unmarshal(b, &role))
		for _, r := range role.Rules {
			for _, g := range r.APIGroups {
				for _, res := range r.Resources {
					parent, _, _ := strings.Cut(res, "/")
					key := g + "/" + parent
					if _, served := namespaced[key]; !served {
						continue
					}
					require.Equal(t, !namespaced[key], clusterScoped[g+"/"+res], "%s in %s", key, c)
				}
			}
		}
	}
}
