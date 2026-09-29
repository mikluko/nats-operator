// Package telemetry wires the controllers to OpenTelemetry and Kubernetes
// events: the SDK's meter and tracer providers configured from the
// environment alone, the instruments each controller registers, a span
// around every reconcile, and the events the reconcilers record.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// The controllers, as their instruments, events and service.name name them.
const (
	ClusterController   = "cluster-controller"
	AuthController      = "auth-controller"
	JetStreamController = "jetstream-controller"
)

const shutdownTimeout = 5 * time.Second

// Shutdown flushes and stops the providers Start installed.
type Shutdown func(context.Context) error

// Flush calls s with shutdownTimeout, whether or not ctx has ended.
func (s Shutdown) Flush(ctx context.Context) error {
	flush, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	return s(flush)
}

// Start installs the global meter and tracer providers of the controller
// named service at version for each signal exporters turns on, the SDK
// logging to log; a signal left off keeps the global noop provider.
func Start(ctx context.Context, service, version string, log logr.Logger) (Shutdown, error) {
	otel.SetLogger(log)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { log.Error(err, "opentelemetry") }))
	res, err := newResource(ctx, service, version)
	if err != nil {
		return nil, err
	}
	reader, spans, err := exporters(ctx)
	if err != nil {
		return nil, err
	}
	var stops []func(context.Context) error
	if reader != nil {
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(res))
		otel.SetMeterProvider(mp)
		stops = append(stops, mp.Shutdown)
	}
	if spans != nil {
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(spans), sdktrace.WithResource(res))
		otel.SetTracerProvider(tp)
		stops = append(stops, tp.Shutdown)
	}
	return func(ctx context.Context) error {
		var errs []error
		for _, stop := range stops {
			errs = append(errs, stop(ctx))
		}
		return errors.Join(errs...)
	}, nil
}

// exporters returns autoexport's reader and span exporter for each signal
// signalOn turns on unless sdkDisabled, and nil for a signal left off.
func exporters(ctx context.Context) (sdkmetric.Reader, sdktrace.SpanExporter, error) {
	if sdkDisabled() {
		return nil, nil, nil
	}
	var reader sdkmetric.Reader
	if signalOn("METRICS") {
		r, err := autoexport.NewMetricReader(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("metric reader: %w", err)
		}
		reader = r
	}
	if !signalOn("TRACES") {
		return reader, nil, nil
	}
	spans, err := autoexport.NewSpanExporter(ctx)
	if err != nil {
		err = fmt.Errorf("span exporter: %w", err)
		if reader != nil {
			err = errors.Join(err, reader.Shutdown(ctx))
		}
		return nil, nil, err
	}
	return reader, spans, nil
}

// sdkDisabled reads OTEL_SDK_DISABLED, which the Go SDK does not, as the
// specification reads a boolean: true only for "true" in any case
// (https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/).
func sdkDisabled() bool {
	return strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true")
}

// signalOn reports whether the environment names an exporter or an OTLP
// endpoint for signal, "METRICS" or "TRACES".
func signalOn(signal string) bool {
	for _, k := range []string{"OTEL_" + signal + "_EXPORTER", "OTEL_EXPORTER_OTLP_" + signal + "_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"} {
		if os.Getenv(k) != "" {
			return true
		}
	}
	return false
}

// newResource is the SDK's own attributes and those of OTEL_RESOURCE_ATTRIBUTES
// and OTEL_SERVICE_NAME, over service as service.name, version as
// service.version and the host name as service.instance.id; an attribute the
// environment malforms is left out.
func newResource(ctx context.Context, service, version string) (*resource.Resource, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("resource: host name: %w", err)
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", service),
			attribute.String("service.version", version),
			attribute.String("service.instance.id", host),
		),
		resource.WithTelemetrySDK(),
		resource.WithFromEnv(),
	)
	if errors.Is(err, resource.ErrPartialResource) {
		otel.Handle(err)
		err = nil
	}
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}
	return res, nil
}
