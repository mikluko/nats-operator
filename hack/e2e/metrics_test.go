package main

import (
	"crypto/tls"
	"crypto/x509"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	"github.com/mikluko/nats-operator/internal/e2e"
)

// metricsSecretData returns the stringData of the Secret in the fixture
// file name under generated.
func metricsSecretData(t *testing.T, generated, name string) map[string]string {
	t.Helper()
	path := filepath.Join(generated, name)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode(), name)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	objs, err := e2e.DecodeObjects(raw)
	require.NoError(t, err)
	require.Len(t, objs, 1)
	require.Equal(t, metricsSecret, objs[0].GetName())
	require.Equal(t, releaseNS, objs[0].GetNamespace())
	data, _, err := unstructured.NestedStringMap(objs[0].Object, "stringData")
	require.NoError(t, err)
	return data
}

// TestMetricsChart pins the chain a story scraping metrics runs: the chart,
// installed with metricsValues, renders a ServiceMonitor endpoint per
// controller that verifies against the generated Secret's ca.crt as a name
// its certificate holds and the other fixture's CA does not sign, and a
// NetworkPolicy per controller whose egress is metricsValues' rules.
func TestMetricsChart(t *testing.T) {
	work := t.TempDir()
	generated, err := generateFixtures(work)
	require.NoError(t, err)
	secret := metricsSecretData(t, generated, metricsFixture)
	other := metricsSecretData(t, generated, otherMetricsFixture)
	pair, err := tls.X509KeyPair([]byte(secret["tls.crt"]), []byte(secret["tls.key"]))
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(pair.Certificate[0])
	require.NoError(t, err)

	api := apiServer{
		service:   netip.MustParseAddrPort("10.96.0.1:443"),
		endpoints: []netip.AddrPort{netip.MustParseAddrPort("172.18.0.3:6443"), netip.MustParseAddrPort("172.18.0.2:6443")},
	}
	vals := metricsValues(api, []string{"nats-system"})
	path := filepath.Join(work, "values.yaml")
	require.NoError(t, writeValues(path, vals))
	images := []image{
		{Key: "cluster", Repository: "c", Tag: "t"}, {Key: "auth", Repository: "a", Tag: "t"}, {Key: "jetstream", Repository: "j", Tag: "t"},
	}
	args := append(chartSets([]string{"cluster", "auth", "jetstream"}, images, chartValues{values: path}),
		"--set", "metrics.serviceMonitor.enabled=true")
	rendered, err := renderChart(t.Context(), "../..", args)
	require.NoError(t, err)
	objs, err := e2e.DecodeObjects(rendered)
	require.NoError(t, err)

	endpoints, err := e2e.MetricsEndpoints(objs)
	require.NoError(t, err)
	var names []string
	for _, ep := range endpoints {
		require.Equal(t, e2e.SecretKey{Name: metricsSecret, Key: "ca.crt"}, ep.CA, ep.Monitor)
		require.Equal(t, releaseNS, ep.Namespace)
		for _, ca := range []string{secret["ca.crt"], other["ca.crt"]} {
			roots := x509.NewCertPool()
			require.True(t, roots.AppendCertsFromPEM([]byte(ca)))
			_, err := cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: ep.ServerName})
			if ca == secret["ca.crt"] {
				require.NoError(t, err, ep.ServerName)
			} else {
				require.ErrorAs(t, err, new(x509.UnknownAuthorityError), ep.ServerName)
			}
		}
		names = append(names, ep.ServerName)
	}
	require.ElementsMatch(t, metricsDNSNames(), names)

	var wantEgress []networkingv1.NetworkPolicyEgressRule
	raw, err := yaml.Marshal(vals["networkPolicy"].(map[string]any)["egress"])
	require.NoError(t, err)
	require.NoError(t, yaml.UnmarshalStrict(raw, &wantEgress))
	require.Len(t, wantEgress, 4)
	require.Equal(t, []networkingv1.NetworkPolicyPeer{
		{IPBlock: &networkingv1.IPBlock{CIDR: "172.18.0.2/32"}}, {IPBlock: &networkingv1.IPBlock{CIDR: "172.18.0.3/32"}},
	}, wantEgress[1].To)
	require.Len(t, wantEgress[1].Ports, 1)

	policies := 0
	for _, o := range objs {
		if o.GetKind() != "NetworkPolicy" {
			continue
		}
		policies++
		var np networkingv1.NetworkPolicy
		require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(o.Object, &np))
		require.Contains(t, np.Spec.PolicyTypes, networkingv1.PolicyTypeEgress, np.Name)
		require.Equal(t, wantEgress, np.Spec.Egress, np.Name)
	}
	require.Equal(t, 3, policies)
}

// TestValuesFor pins that only the home cluster of a story scraping metrics
// runs under metricsValues, for the story's namespaces.
func TestValuesFor(t *testing.T) {
	work := t.TempDir()
	generated, err := generateFixtures(work)
	require.NoError(t, err)
	stories, err := selectBundles("../..", generated, "1,12")
	require.NoError(t, err)
	require.Len(t, stories, 2)
	require.False(t, stories[0].ScrapeMetrics)
	require.True(t, stories[1].ScrapeMetrics)

	base := chartValues{allowGatewayWithoutTLS: true}
	ci := &chartInstall{work: work, base: base, api: apiServer{
		service:   netip.MustParseAddrPort("10.96.0.1:443"),
		endpoints: []netip.AddrPort{netip.MustParseAddrPort("172.18.0.2:6443")},
	}}
	for _, tc := range []struct {
		story, cluster int
		scrapes        bool
	}{{0, 0, false}, {1, 1, false}, {1, 0, true}} {
		vals, err := ci.valuesFor(stories[tc.story], tc.cluster)
		require.NoError(t, err)
		if !tc.scrapes {
			require.Equal(t, base, vals)
			continue
		}
		require.Equal(t, filepath.Join(work, "12-metrics-values.yaml"), vals.values)
		raw, err := os.ReadFile(vals.values)
		require.NoError(t, err)
		want, err := yaml.Marshal(metricsValues(ci.api, []string{"nats-system"}))
		require.NoError(t, err)
		require.Equal(t, string(want), string(raw))
	}
}
