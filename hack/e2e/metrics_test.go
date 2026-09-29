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

// metricsSecretData returns the stringData of the Secret secret in the
// fixture file name under generated.
func metricsSecretData(t *testing.T, generated, name, secret string) map[string]string {
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
	require.Equal(t, secret, objs[0].GetName())
	require.Equal(t, releaseNS, objs[0].GetNamespace())
	data, _, err := unstructured.NestedStringMap(objs[0].Object, "stringData")
	require.NoError(t, err)
	return data
}

// exampleAPI is an API server as kind places it.
var exampleAPI = apiServer{
	service:   netip.MustParseAddrPort("10.96.0.1:443"),
	endpoints: []netip.AddrPort{netip.MustParseAddrPort("172.18.0.3:6443"), netip.MustParseAddrPort("172.18.0.2:6443")},
}

// renderWith returns the objects the chart renders with every controller
// enabled, the values vals, and sets.
func renderWith(t *testing.T, vals map[string]any, sets ...string) []*unstructured.Unstructured {
	t.Helper()
	path := filepath.Join(t.TempDir(), "values.yaml")
	require.NoError(t, writeValues(path, vals))
	images := []image{
		{Key: "cluster", Repository: "c", Tag: "t"}, {Key: "auth", Repository: "a", Tag: "t"}, {Key: "jetstream", Repository: "j", Tag: "t"},
	}
	args := append(chartSets([]string{"cluster", "auth", "jetstream"}, images, chartValues{values: path}), sets...)
	rendered, err := renderChart(t.Context(), "../..", args)
	require.NoError(t, err)
	objs, err := e2e.DecodeObjects(rendered)
	require.NoError(t, err)
	return objs
}

// egressOf decodes the networkPolicy.egress of vals.
func egressOf(t *testing.T, vals map[string]any) []networkingv1.NetworkPolicyEgressRule {
	t.Helper()
	var rules []networkingv1.NetworkPolicyEgressRule
	raw, err := yaml.Marshal(vals["networkPolicy"].(map[string]any)["egress"])
	require.NoError(t, err)
	require.NoError(t, yaml.UnmarshalStrict(raw, &rules))
	return rules
}

// TestMetricsChart pins that story 12's page values render under the chart's
// schema as written, and that metricsValues of them render one ServiceMonitor
// endpoint per controller, verifying against the generated ca.crt and not the
// other fixture's CA, and one NetworkPolicy per controller whose egress is the
// page's with its ipBlock rules replaced by the API server's.
func TestMetricsChart(t *testing.T) {
	work := t.TempDir()
	generated, err := generateFixtures(work)
	require.NoError(t, err)
	secret := metricsSecretData(t, generated, metricsFixture, metricsSecret)
	other := metricsSecretData(t, generated, otherMetricsFixture, otherMetricsSecret)
	pair, err := tls.X509KeyPair([]byte(secret["tls.crt"]), []byte(secret["tls.key"]))
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(pair.Certificate[0])
	require.NoError(t, err)
	stories, err := selectBundles("../..", generated, "12")
	require.NoError(t, err)
	page := stories[0].ChartValues

	asWritten, err := e2e.MetricsEndpoints(renderWith(t, page))
	require.NoError(t, err)
	require.Len(t, asWritten, 3)

	vals, err := metricsValues(page, exampleAPI)
	require.NoError(t, err)
	objs := renderWith(t, vals, "--set", "metrics.serviceMonitor.enabled=true")
	endpoints, err := e2e.MetricsEndpoints(objs)
	require.NoError(t, err)
	var names []string
	for _, ep := range endpoints {
		require.Equal(t, e2e.SecretKey{Name: metricsSecret, Key: "ca.crt"}, ep.CA, ep.Monitor)
		require.Equal(t, releaseNS, ep.Namespace)
		require.NotEmpty(t, ep.BearerTokenFile, ep.Monitor)
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

	pageEgress, wantEgress := egressOf(t, page), egressOf(t, vals)
	require.Equal(t, []networkingv1.NetworkPolicyPeer{
		{IPBlock: &networkingv1.IPBlock{CIDR: "10.96.0.1/32"}},
	}, wantEgress[0].To)
	require.Equal(t, []networkingv1.NetworkPolicyPeer{
		{IPBlock: &networkingv1.IPBlock{CIDR: "172.18.0.2/32"}}, {IPBlock: &networkingv1.IPBlock{CIDR: "172.18.0.3/32"}},
	}, wantEgress[1].To)
	require.Len(t, wantEgress[1].Ports, 1)
	require.Equal(t, pageEgress[2:], wantEgress[2:])

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

// TestMetricsValues_Refuses pins that metricsValues fails on page values the
// harness cannot run under.
func TestMetricsValues_Refuses(t *testing.T) {
	page := func(secret string, egress ...any) map[string]any {
		return map[string]any{
			"metrics": map[string]any{
				"tls":     map[string]any{"secretName": secret},
				"scraper": map[string]any{"serviceAccount": "monitoring/prometheus"},
			},
			"networkPolicy": map[string]any{"enabled": true, "egress": egress},
		}
	}
	ipRule := map[string]any{"to": []any{map[string]any{"ipBlock": map[string]any{"cidr": "10.0.0.1/32"}}}}
	mixed := map[string]any{"to": []any{
		map[string]any{"ipBlock": map[string]any{"cidr": "10.0.0.1/32"}},
		map[string]any{"namespaceSelector": map[string]any{}},
	}}
	_, err := metricsValues(page(metricsSecret, ipRule), exampleAPI)
	require.NoError(t, err)
	_, err = metricsValues(page("other", ipRule), exampleAPI)
	require.ErrorContains(t, err, "metrics.tls.secretName")
	_, err = metricsValues(page(metricsSecret, mixed), exampleAPI)
	require.ErrorContains(t, err, "no rule of ipBlocks alone")
}

// TestValuesFor pins that only the home cluster of a story scraping metrics
// runs under metricsValues of its page.
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
	ci := &chartInstall{work: work, base: base, api: exampleAPI}
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
		metrics, err := metricsValues(stories[1].ChartValues, ci.api)
		require.NoError(t, err)
		want, err := yaml.Marshal(metrics)
		require.NoError(t, err)
		require.Equal(t, string(want), string(raw))
	}
}
