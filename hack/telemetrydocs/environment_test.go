package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/protobuf/proto"

	"github.com/mikluko/nats-operator/internal/telemetry"
)

// init makes the OTLP gRPC exporter resolve localhost without grpc-go's DNS
// resolver, whose _grpc_config TXT lookup outlives the export's deadline
// where the network's DNS server is slow to answer NXDOMAIN.
func init() {
	resolver.SetDefaultScheme("passthrough")
}

// clearEnvironment unsets every variable the environment table names until
// t ends.
func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, v := range environment {
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

// clientHello listens on port over both loopback addresses and
// returns the first TLS ClientHello a client sends there.
func clientHello(t *testing.T, port string) <-chan *tls.ClientHelloInfo {
	t.Helper()
	hellos := make(chan *tls.ClientHelloInfo, 1)
	cfg := &tls.Config{GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) {
		select {
		case hellos <- h:
		default:
		}
		return nil, errors.New("captured")
	}}
	var listened bool
	for _, addr := range []string{"127.0.0.1", "::1"} {
		l, err := net.Listen("tcp", net.JoinHostPort(addr, port))
		if err != nil {
			continue
		}
		listened = true
		t.Cleanup(func() { _ = l.Close() })
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				go func() {
					defer func() { _ = c.Close() }()
					_ = tls.Server(c, cfg).Handshake()
				}()
			}
		}()
	}
	require.True(t, listened, "port %s is taken on both loopback addresses", port)
	return hellos
}

// defaultEndpoint pins that, with only an exporter named, the SDK dials
// localhost on port over TLS offering proto by ALPN.
func defaultEndpoint(t *testing.T, env map[string]string, port, proto string) {
	t.Helper()
	hellos := clientHello(t, port)
	full := map[string]string{"OTEL_TRACES_EXPORTER": "otlp"}
	for k, v := range env {
		full[k] = v
	}
	_ = export(t, start(t, full), 5*time.Second)
	select {
	case h := <-hellos:
		require.Equal(t, "localhost", h.ServerName)
		if proto != "" {
			require.Contains(t, h.SupportedProtos, proto)
		}
	default:
		t.Fatalf("no TLS connection on localhost:%s", port)
	}
}

// TestEnvironmentDefaults pins every Default the environment table names
// to what the SDK does with the variable unset. A row with a Default and
// no check here fails, as does a check whose row names another Default.
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
		"OTEL_EXPORTER_OTLP_PROTOCOL": {"`http/protobuf`", func(t *testing.T) {
			got := exportedTo(t)
			require.NotEmpty(t, got)
			for path, r := range got {
				require.Equal(t, "application/x-protobuf", r.contentType, path)
			}
		}},
		"OTEL_EXPORTER_OTLP_ENDPOINT": {"`https://localhost:4318` over `http/protobuf`, `https://localhost:4317` over `grpc`", func(t *testing.T) {
			t.Run("http/protobuf", func(t *testing.T) { defaultEndpoint(t, nil, "4318", "") })
			t.Run("grpc", func(t *testing.T) {
				defaultEndpoint(t, map[string]string{"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, "4317", "h2")
			})
		}},
		"OTEL_EXPORTER_OTLP_INSECURE": {"`false`", func(t *testing.T) {
			defaultEndpoint(t, nil, "4318", "")
		}},
		"OTEL_EXPORTER_OTLP_COMPRESSION": {"none", func(t *testing.T) {
			got := exportedTo(t)
			require.NotEmpty(t, got)
			for path, r := range got {
				require.Empty(t, r.contentEncoding, path)
			}
		}},
		"OTEL_EXPORTER_PROMETHEUS_HOST": {"`localhost`, `9464`", func(t *testing.T) {
			stop := start(t, map[string]string{"OTEL_METRICS_EXPORTER": "prometheus"})
			t.Cleanup(func() { require.NoError(t, stop(context.Background())) })
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				resp, err := http.Get("http://localhost:9464/metrics")
				if !assert.NoError(c, err) {
					return
				}
				assert.NoError(c, resp.Body.Close())
				assert.Equal(c, http.StatusOK, resp.StatusCode)
			}, 5*time.Second, 50*time.Millisecond, "the Prometheus listener serves on its start")
		}},
		"OTEL_TRACES_SAMPLER": {"`parentbased_always_on`", func(t *testing.T) {
			url, _ := collector(t)
			stop := start(t, map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": url})
			t.Cleanup(func() { _ = stop(context.Background()) })
			tracer := otel.Tracer("test")
			_, root := tracer.Start(t.Context(), "root")
			require.True(t, root.SpanContext().IsSampled(), "a root span was dropped")
			root.End()
			parent := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}, Remote: true,
			})
			_, child := tracer.Start(trace.ContextWithRemoteSpanContext(t.Context(), parent), "child")
			require.False(t, child.SpanContext().IsSampled(), "the child of an unsampled parent was kept")
			child.End()
		}},
	}
	for _, v := range environment {
		if v.Default == "" {
			continue
		}
		name := v.Names[0]
		t.Run(name, func(t *testing.T) {
			c, ok := checks[name]
			require.True(t, ok, "no check pins the default of %s", name)
			require.Equal(t, c.def, v.Default)
			c.check(t)
		})
		delete(checks, name)
	}
	require.Empty(t, checks, "checks for rows without a Default")
}
