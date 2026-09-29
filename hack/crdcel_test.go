package hack_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/cel/environment"
	"sigs.k8s.io/yaml"
)

// TestCRDRules_ChartKubeVersion pins the chart's kubeVersion as the oldest
// Kubernetes minor whose CEL compiles every validation rule of its CRDs.
func TestCRDRules_ChartKubeVersion(t *testing.T) {
	raw, err := os.ReadFile("../charts/nats-operator/Chart.yaml")
	require.NoError(t, err)
	var chart struct {
		KubeVersion string `json:"kubeVersion"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &chart))
	floor, ok := strings.CutPrefix(chart.KubeVersion, ">=")
	require.True(t, ok, "kubeVersion %q is not a lower bound", chart.KubeVersion)
	minimum, err := version.ParseSemantic(floor)
	require.NoError(t, err)

	files, err := filepath.Glob("../charts/nats-operator/crds/*.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	var rules []string
	for _, f := range files {
		raw, err := os.ReadFile(f)
		require.NoError(t, err)
		var crd map[string]any
		require.NoError(t, yaml.Unmarshal(raw, &crd), f)
		rules = appendRules(rules, crd)
	}
	require.NotEmpty(t, rules)

	failing := func(major, minor uint) []string {
		env, err := environment.MustBaseEnvSet(version.MajorMinor(major, minor)).NewExpressionsEnv().Extend(
			cel.Variable("self", cel.DynType), cel.Variable("oldSelf", cel.DynType))
		require.NoError(t, err)
		var out []string
		for _, r := range rules {
			if _, iss := env.Compile(r); iss.Err() != nil {
				out = append(out, r)
			}
		}
		return out
	}
	require.Empty(t, failing(minimum.Major(), minimum.Minor()), "rules the chart's kubeVersion %s cannot compile", chart.KubeVersion)
	require.NotEmpty(t, failing(minimum.Major(), minimum.Minor()-1), "every rule compiles below the chart's kubeVersion %s", chart.KubeVersion)
}

// appendRules appends every x-kubernetes-validations rule under v to rules.
func appendRules(rules []string, v any) []string {
	switch v := v.(type) {
	case map[string]any:
		if vs, ok := v["x-kubernetes-validations"].([]any); ok {
			for _, x := range vs {
				rules = append(rules, x.(map[string]any)["rule"].(string))
			}
		}
		for _, x := range v {
			rules = appendRules(rules, x)
		}
	case []any:
		for _, x := range v {
			rules = appendRules(rules, x)
		}
	}
	return rules
}
