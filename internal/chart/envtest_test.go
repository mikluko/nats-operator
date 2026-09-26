package chart_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// TestEnvtest_ChartApplies pins that the chart's CRDs install and that every
// object of each single-controller subset and of all three is accepted by an
// API server under strict field validation.
func TestEnvtest_ChartApplies(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{chartDir + "/crds"},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	require.Len(t, env.CRDs, 16)

	c, err := client.New(cfg, client.Options{})
	require.NoError(t, err)
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nats"}}))

	subsets := [][]string{{"cluster"}, {"auth"}, {"jetstream"}, {"cluster", "auth", "jetstream"}}
	for _, enabled := range subsets {
		t.Run(strings.Join(enabled, "+"), func(t *testing.T) {
			for key, obj := range render(t, enabledSets(enabled)...) {
				require.NoError(t, c.Create(t.Context(), obj.DeepCopy(), client.DryRunAll, client.FieldValidation("Strict")), key)
			}
		})
	}
}
