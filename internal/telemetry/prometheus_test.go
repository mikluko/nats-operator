package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// scrape serves controller's instruments over the fixtures through the
// OpenTelemetry Prometheus exporter, as autoexport builds it, and returns
// the names of the nats_operator metrics a scrape of its /metrics reads.
func scrape(t *testing.T, controller string) []string {
	t.Helper()
	reg := prometheus.NewRegistry()
	reader, err := promexporter.New(promexporter.WithRegisterer(reg))
	require.NoError(t, err)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, mp.Shutdown(context.Background())) })
	record(t, controller, mp.Meter("test"), fakeReader(t))

	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	t.Cleanup(srv.Close)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeTextPlain)))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	require.NoError(t, err)
	var names []string
	for name := range families {
		if strings.HasPrefix(name, "nats_operator_") {
			names = append(names, name)
		}
	}
	return names
}

// TestPrometheusName pins each instrument's PrometheusName to the name a
// scrape of the Prometheus exporter reads, and the 0/1 gauges' names to
// carry no unit suffix.
func TestPrometheusName(t *testing.T) {
	for in, want := range map[*Instrument]string{
		&Condition:   "nats_operator_condition",
		&RolloutGate: "nats_operator_rollout_gate",
	} {
		name, err := in.PrometheusName()
		require.NoError(t, err)
		require.Equal(t, want, name)
	}
	for _, controller := range controllers {
		t.Run(controller, func(t *testing.T) {
			var want []string
			for _, in := range Instruments {
				if slices.Contains(in.Controllers, controller) {
					name, err := in.PrometheusName()
					require.NoError(t, err)
					want = append(want, name)
				}
			}
			require.ElementsMatch(t, want, scrape(t, controller))
		})
	}
}
