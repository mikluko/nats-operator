package main

import (
	"context"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"github.com/yuin/goldmark"
	gast "github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	east "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/text"
	"go.opentelemetry.io/otel"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/mikluko/nats-operator/internal/telemetry"
)

// envVar is one row of the environment table.
type envVar struct {
	// Names are the row's variables, the first the one it is known by.
	Names []string
	// Default is the cell's Markdown source, "unset" for none.
	Default string
}

// envTable returns the rows of the table in environment.md.
func envTable(t *testing.T) []envVar {
	t.Helper()
	src := []byte(environment)
	doc := goldmark.New(goldmark.WithExtensions(extension.Table)).Parser().Parse(text.NewReader(src))
	var rows []envVar
	require.NoError(t, gast.Walk(doc, func(n gast.Node, entering bool) (gast.WalkStatus, error) {
		row, ok := n.(*east.TableRow)
		if !entering || !ok {
			return gast.WalkContinue, nil
		}
		var v envVar
		for c := row.FirstChild().FirstChild(); c != nil; c = c.NextSibling() {
			if code, ok := c.(*gast.CodeSpan); ok {
				var name strings.Builder
				for t := code.FirstChild(); t != nil; t = t.NextSibling() {
					name.Write(t.(*gast.Text).Value(src))
				}
				v.Names = append(v.Names, name.String())
			}
		}
		lines := row.FirstChild().NextSibling().Lines()
		v.Default = strings.TrimSpace(string(lines.Value(src)))
		require.NotEmpty(t, v.Names, "a row names its variables in code spans")
		rows = append(rows, v)
		return gast.WalkSkipChildren, nil
	}))
	require.NotEmpty(t, rows)
	return rows
}

// clearEnvironment unsets every variable the environment table names until
// t ends.
func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, v := range envTable(t) {
		for _, name := range v.Names {
			t.Setenv(name, "")
			require.NoError(t, os.Unsetenv(name))
		}
	}
}

// start runs telemetry.Start for the cluster controller under env and
// returns its shutdown, restoring the global providers afterwards.
func start(t *testing.T, env map[string]string) telemetry.Shutdown {
	t.Helper()
	clearEnvironment(t)
	for k, v := range env {
		t.Setenv(k, v)
	}
	mp, tp := otel.GetMeterProvider(), otel.GetTracerProvider()
	t.Cleanup(func() {
		otel.SetMeterProvider(mp)
		otel.SetTracerProvider(tp)
	})
	stop, err := telemetry.Start(t.Context(), telemetry.ClusterController, logr.Discard())
	require.NoError(t, err)
	return stop
}

// export records one span and one counter increment, then shuts the
// providers down, which exports both; the export's error is returned.
func export(t *testing.T, stop telemetry.Shutdown, timeout time.Duration) error {
	t.Helper()
	_, span := otel.Tracer("test").Start(t.Context(), "span")
	span.End()
	c, err := otel.Meter("test").Int64Counter("test.counter")
	require.NoError(t, err)
	c.Add(t.Context(), 1)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return stop(ctx)
}

// received is one OTLP/HTTP request as the collector got it.
type received struct {
	path, contentType, contentEncoding string
	body                               []byte
}

// collector is an OTLP/HTTP endpoint over plain HTTP that keeps what it
// receives.
func collector(t *testing.T) (string, func() []received) {
	t.Helper()
	var mu sync.Mutex
	var got []received
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, received{r.URL.Path, r.Header.Get("Content-Type"), r.Header.Get("Content-Encoding"), b})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() []received {
		mu.Lock()
		defer mu.Unlock()
		return append([]received(nil), got...)
	}
}

// exportedTo starts telemetry with only a collector's endpoint set,
// exports, and returns what the collector received by path.
func exportedTo(t *testing.T) map[string]received {
	t.Helper()
	url, got := collector(t)
	require.NoError(t, export(t, start(t, map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": url}), 10*time.Second))
	byPath := map[string]received{}
	for _, r := range got() {
		byPath[r.path] = r
	}
	return byPath
}

// sdkDefaults are the rows whose Default is the OpenTelemetry SDK's own
// rather than the telemetry package's, which no check here re-tests.
var sdkDefaults = map[string]string{
	"OTEL_EXPORTER_OTLP_PROTOCOL":    "`http/protobuf`",
	"OTEL_EXPORTER_OTLP_ENDPOINT":    "`https://localhost:4318` over `http/protobuf`, `https://localhost:4317` over `grpc`",
	"OTEL_EXPORTER_OTLP_INSECURE":    "`false`",
	"OTEL_EXPORTER_OTLP_COMPRESSION": "none",
	"OTEL_EXPORTER_PROMETHEUS_HOST":  "`localhost`, `9464`",
	"OTEL_TRACES_SAMPLER":            "`parentbased_always_on`",
}

// TestEnvironmentDefaults pins every Default the environment table names
// to what the controllers do with the variable unset, sdkDefaults aside. A
// row with a Default and neither a check here nor an sdkDefaults entry
// fails, as does a check or entry whose row names another Default.
func TestEnvironmentDefaults(t *testing.T) {
	checks := map[string]struct {
		def   string
		check func(t *testing.T)
	}{
		"OTEL_SDK_DISABLED": {"`false`", func(t *testing.T) {
			require.NotEmpty(t, exportedTo(t), "nothing exported with OTEL_SDK_DISABLED unset")
		}},
		"OTEL_SERVICE_NAME": {"the controller's name, such as `cluster-controller`", func(t *testing.T) {
			r, ok := exportedTo(t)["/v1/traces"]
			require.True(t, ok, "no traces exported")
			var req collectortrace.ExportTraceServiceRequest
			require.NoError(t, proto.Unmarshal(r.body, &req))
			require.NotEmpty(t, req.ResourceSpans)
			var name string
			for _, a := range req.ResourceSpans[0].Resource.Attributes {
				if a.Key == "service.name" {
					name = a.Value.GetStringValue()
				}
			}
			require.Equal(t, telemetry.ClusterController, name)
		}},
		"OTEL_METRICS_EXPORTER": {"`otlp` once metrics are on", func(t *testing.T) {
			require.Contains(t, exportedTo(t), "/v1/metrics")
		}},
		"OTEL_TRACES_EXPORTER": {"`otlp` once traces are on", func(t *testing.T) {
			require.Contains(t, exportedTo(t), "/v1/traces")
		}},
	}
	sdk := maps.Clone(sdkDefaults)
	for _, v := range envTable(t) {
		if v.Default == "unset" {
			continue
		}
		name := v.Names[0]
		if def, ok := sdk[name]; ok {
			require.Equal(t, def, v.Default, name)
			delete(sdk, name)
			continue
		}
		t.Run(name, func(t *testing.T) {
			c, ok := checks[name]
			require.True(t, ok, "no check pins the default of %s", name)
			require.Equal(t, c.def, v.Default)
			c.check(t)
		})
		delete(checks, name)
	}
	require.Empty(t, checks, "checks for rows without a Default")
	require.Empty(t, sdk, "sdkDefaults for rows without a Default")
}
