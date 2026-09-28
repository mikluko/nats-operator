package chart_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

const chartDir = "../../charts/nats-operator"

const rbacDir = "../../config/rbac"

// rulesTest is the name of the test in each controller's helm-unittest suite
// whose equal assertion is that controller's ClusterRole rules, exactly.
const rulesTest = "grants exactly its ClusterRole rules"

var controllers = []string{"cluster", "auth", "jetstream"}

var ownGroups = map[string]string{
	"cluster":   "cluster.nats.mikluko.io",
	"auth":      "auth.nats.mikluko.io",
	"jetstream": "jetstream.nats.mikluko.io",
}

// grants maps "group/resource" to the verbs granted on it.
type grants map[string][]string

// TestChartRBAC_Generated pins that each controller's role in the chart's
// files/rbac is config/rbac/<controller>/role.yaml byte for byte, and that its
// helm-unittest suite asserts that role's rules exactly.
func TestChartRBAC_Generated(t *testing.T) {
	for _, c := range controllers {
		want, err := os.ReadFile(filepath.Join(rbacDir, c+"-controller", "role.yaml"))
		require.NoError(t, err)
		got, err := os.ReadFile(filepath.Join(chartDir, "files", "rbac", c+"-controller.yaml"))
		require.NoError(t, err)
		require.Equal(t, string(want), string(got), "%s: run just chart-rbac", c)
		require.Equal(t, generatedRole(t, c), clusterRoleFixture(t, c), c)
	}
}

// TestChartRBAC_Groups pins that no controller's generated ClusterRole grants
// on another controller's API group or names a resource of the chart's API
// groups that no CRD in crds/ defines.
func TestChartRBAC_Groups(t *testing.T) {
	plurals := crdPlurals(t)
	for _, c := range controllers {
		for key := range generatedRole(t, c) {
			group, resource, _ := strings.Cut(key, "/")
			for other, own := range ownGroups {
				require.False(t, other != c && group == own, "%s holds %s", c, key)
			}
			if strings.HasSuffix(group, "nats.mikluko.io") {
				base, _, _ := strings.Cut(resource, "/")
				require.True(t, plurals[group+"/"+base], "%s names no CRD", key)
			}
		}
	}
}

// generatedRole returns controller c's ClusterRole rules as controller-gen
// writes them to config/rbac.
func generatedRole(t *testing.T, c string) grants {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(rbacDir, c+"-controller", "role.yaml"))
	require.NoError(t, err)
	var role rbacv1.ClusterRole
	require.NoError(t, yaml.Unmarshal(b, &role))
	require.NotEmpty(t, role.Rules, c)
	return flatten(t, role.Rules)
}

// clusterRoleFixture returns controller c's ClusterRole rules as the rulesTest
// of charts/nats-operator/tests/<c>-controller_test.yaml asserts them.
func clusterRoleFixture(t *testing.T, c string) grants {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(chartDir, "tests", c+"-controller_test.yaml"))
	require.NoError(t, err)
	var suite struct {
		Tests []struct {
			It      string `json:"it"`
			Asserts []struct {
				Equal *struct {
					Path  string          `json:"path"`
					Value json.RawMessage `json:"value"`
				} `json:"equal"`
			} `json:"asserts"`
		} `json:"tests"`
	}
	require.NoError(t, yaml.Unmarshal(b, &suite))
	var found []rbacv1.PolicyRule
	for _, tc := range suite.Tests {
		if tc.It != rulesTest {
			continue
		}
		for _, a := range tc.Asserts {
			if a.Equal != nil && a.Equal.Path == "rules" {
				require.Nil(t, found, "%s: two rules assertions", c)
				require.NoError(t, json.Unmarshal(a.Equal.Value, &found), c)
			}
		}
	}
	require.NotEmpty(t, found, "%s: no %q test asserting rules", c, rulesTest)
	return flatten(t, found)
}

// crdPlurals returns "group/plural" for every CRD in the chart.
func crdPlurals(t *testing.T) map[string]bool {
	t.Helper()
	files, err := filepath.Glob(chartDir + "/crds/*.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	out := map[string]bool{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		var crd apiextensionsv1.CustomResourceDefinition
		require.NoError(t, yaml.Unmarshal(b, &crd))
		out[crd.Spec.Group+"/"+crd.Spec.Names.Plural] = true
	}
	return out
}

func merge(gs ...grants) grants {
	out := grants{}
	for _, g := range gs {
		for k, v := range g {
			out[k] = v
		}
	}
	return out
}

// flatten turns rules into grants, refusing a group/resource granted twice so
// that a rule cannot hide behind another.
func flatten(t *testing.T, rules []rbacv1.PolicyRule) grants {
	t.Helper()
	g := grants{}
	for _, r := range rules {
		require.Empty(t, r.ResourceNames)
		require.Empty(t, r.NonResourceURLs)
		verbs := slices.Sorted(slices.Values(r.Verbs))
		for _, group := range r.APIGroups {
			for _, res := range r.Resources {
				key := group + "/" + res
				require.NotContains(t, g, key)
				g[key] = verbs
			}
		}
	}
	return g
}
