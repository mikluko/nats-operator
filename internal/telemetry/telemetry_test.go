package telemetry

import (
	"context"
	"errors"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// signalVars are every variable signalOn reads; each test starts with all
// of them empty, which the SDK and signalOn read as unset.
var signalVars = []string{
	"OTEL_METRICS_EXPORTER",
	"OTEL_TRACES_EXPORTER",
	"OTEL_EXPORTER_OTLP_ENDPOINT",
	"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
	"OTEL_SDK_DISABLED",
}

func clearSignalVars(t *testing.T) {
	t.Helper()
	for _, k := range signalVars {
		t.Setenv(k, "")
	}
}

// TestExporters pins that a signal is exported only while its exporter or
// an OTLP endpoint applying to it is named.
func TestExporters(t *testing.T) {
	tests := []struct {
		name           string
		env            map[string]string
		metrics, spans bool
	}{
		{name: "nothing set", env: nil},
		{name: "endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318"}, metrics: true, spans: true},
		{name: "metrics endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://127.0.0.1:4318"}, metrics: true},
		{name: "traces endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://127.0.0.1:4318"}, spans: true},
		{name: "metrics exporter", env: map[string]string{"OTEL_METRICS_EXPORTER": "console"}, metrics: true},
		{name: "traces exporter none", env: map[string]string{"OTEL_TRACES_EXPORTER": "none"}, spans: true},
		{name: "protocol alone", env: map[string]string{"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}},
		{name: "SDK disabled", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318", "OTEL_METRICS_EXPORTER": "prometheus", "OTEL_SDK_DISABLED": "TRUE"}},
		{name: "SDK not disabled", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318", "OTEL_SDK_DISABLED": "yes"}, metrics: true, spans: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearSignalVars(t)
			t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			reader, spans, err := exporters(t.Context())
			require.NoError(t, err)
			require.Equal(t, tt.metrics, reader != nil, "metric reader")
			require.Equal(t, tt.spans, spans != nil, "span exporter")
			if reader != nil {
				require.NoError(t, reader.Shutdown(context.Background()))
			}
			if spans != nil {
				require.NoError(t, spans.Shutdown(context.Background()))
			}
		})
	}
}

// TestStart pins that Start leaves the global noop providers in place for
// a signal left off, installs the SDK's for one turned on, and routes the
// SDK's errors to the logger it is given.
func TestStart(t *testing.T) {
	mp, tp, handler := otel.GetMeterProvider(), otel.GetTracerProvider(), otel.GetErrorHandler()
	t.Cleanup(func() {
		otel.SetMeterProvider(mp)
		otel.SetTracerProvider(tp)
		otel.SetErrorHandler(handler)
	})

	t.Run("nothing set", func(t *testing.T) {
		clearSignalVars(t)
		var logged []string
		log := funcr.New(func(prefix, args string) { logged = append(logged, args) }, funcr.Options{})
		stop, err := Start(t.Context(), ClusterController, log)
		require.NoError(t, err)
		_, isSDKMeter := otel.GetMeterProvider().(*sdkmetric.MeterProvider)
		_, isSDKTracer := otel.GetTracerProvider().(*sdktrace.TracerProvider)
		require.False(t, isSDKMeter, "a meter provider was installed with nothing configured")
		require.False(t, isSDKTracer, "a tracer provider was installed with nothing configured")
		otel.Handle(errors.New("export failed"))
		require.Len(t, logged, 1)
		require.Contains(t, logged[0], "export failed")
		require.NoError(t, stop(context.Background()))
	})

	t.Run("metrics on", func(t *testing.T) {
		clearSignalVars(t)
		t.Setenv("OTEL_METRICS_EXPORTER", "none")
		stop, err := Start(t.Context(), ClusterController, funcr.New(func(string, string) {}, funcr.Options{}))
		require.NoError(t, err)
		_, isSDKMeter := otel.GetMeterProvider().(*sdkmetric.MeterProvider)
		_, isSDKTracer := otel.GetTracerProvider().(*sdktrace.TracerProvider)
		require.True(t, isSDKMeter)
		require.False(t, isSDKTracer)
		require.NoError(t, stop(context.Background()))
	})
}

func TestNewResource(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want map[string]string
	}{
		{
			name: "the controller's name",
			want: map[string]string{"service.name": "auth-controller", "telemetry.sdk.language": "go"},
		},
		{
			name: "overridden",
			env:  map[string]string{"OTEL_SERVICE_NAME": "auth", "OTEL_RESOURCE_ATTRIBUTES": "deployment.environment=prod"},
			want: map[string]string{"service.name": "auth", "deployment.environment": "prod"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_SERVICE_NAME", "")
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			res, err := newResource(t.Context(), AuthController)
			require.NoError(t, err)
			for k, v := range tt.want {
				got, ok := res.Set().Value(attribute.Key(k))
				require.True(t, ok, k)
				require.Equal(t, v, got.AsString(), k)
			}
		})
	}
}
