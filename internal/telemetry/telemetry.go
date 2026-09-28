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
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// The controllers, as their instruments, events and service.name name them.
const (
	ClusterController   = "cluster-controller"
	AuthController      = "auth-controller"
	JetStreamController = "jetstream-controller"
)

const shutdownTimeout = 5 * time.Second

// Shutdown flushes and stops the providers Start installed. It is a manager
// runnable that runs on every replica, leader or not, and shuts down once
// the manager stops.
type Shutdown func(context.Context) error

// Start implements manager.Runnable.
func (s Shutdown) Start(ctx context.Context) error {
	<-ctx.Done()
	flush, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	return s(flush)
}

// NeedLeaderElection implements manager.LeaderElectionRunnable.
func (Shutdown) NeedLeaderElection() bool { return false }

// Start installs the global meter and tracer providers of the controller
// named service, for the signals exporters turns on, with service as
// service.name unless OTEL_SERVICE_NAME is set; a signal left off keeps
// the global noop provider. The SDK's errors go to log.
func Start(ctx context.Context, service string, log logr.Logger) (Shutdown, error) {
	otel.SetLogger(log)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { log.Error(err, "opentelemetry") }))
	res, err := newResource(ctx, service)
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

// exporters decides what a controller exports: per signal, nothing unless
// the environment turns it on, and then autoexport's choice under the SDK's
// defaults. A signal is on while OTEL_<SIGNAL>_EXPORTER,
// OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT is
// set to anything, and every signal is off while OTEL_SDK_DISABLED is
// true; the reader or exporter of a signal left off is nil.
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
// and OTEL_SERVICE_NAME, over service as service.name and the host name,
// which in Kubernetes is the pod's name, as service.instance.id. An
// attribute the environment malforms is reported to the SDK's error handler
// and left out.
func newResource(ctx context.Context, service string) (*resource.Resource, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("resource: host name: %w", err)
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", service), attribute.String("service.instance.id", host)),
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

// Install starts telemetry for service and adds its shutdown to mgr.
func Install(ctx context.Context, mgr manager.Manager, service string) error {
	stop, err := Start(ctx, service, mgr.GetLogger().WithName("opentelemetry"))
	if err != nil {
		return fmt.Errorf("start telemetry: %w", err)
	}
	if err := mgr.Add(stop); err != nil {
		return errors.Join(fmt.Errorf("add telemetry shutdown: %w", err), stop(ctx))
	}
	return nil
}
