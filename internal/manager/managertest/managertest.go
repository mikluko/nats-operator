// Package managertest reads what a controller's setup registers with a
// controller-runtime manager, and runs it under its Roles on envtest.
package managertest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/mikluko/nats-operator/internal/telemetry"
)

// ReconciledKinds returns, in registration order, the kind each controller
// setup traces its reconciles under, failing the test for a controller that
// records no span. It never runs in parallel, since it replaces the
// global tracer provider. Its controllers skip controller-runtime's
// process-wide name check, so another manager in the test binary may
// register the same names.
func ReconciledKinds(t *testing.T, scheme *runtime.Scheme, setup func(context.Context, manager.Manager) error) []string {
	t.Helper()
	mgr, err := ctrl.NewManager(&rest.Config{Host: "http://127.0.0.1:1"}, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: ptr.To(true)},
	})
	require.NoError(t, err)
	rec := &recording{Manager: mgr, client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	require.NoError(t, setup(t.Context(), rec))

	spans := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "managertest", Name: "absent"}}
	var kinds []string
	for i, c := range rec.controllers {
		_, _ = c.Reconcile(t.Context(), req)
		ended := spans.Ended()
		require.Len(t, ended, i+1, "controller %d of setup records no reconcile span", i)
		kind := ""
		for _, a := range ended[i].Attributes() {
			if string(a.Key) == telemetry.AttrKind {
				kind = a.Value.AsString()
			}
		}
		require.NotEmpty(t, kind, "span %q carries no %s", ended[i].Name(), telemetry.AttrKind)
		kinds = append(kinds, kind)
	}
	return kinds
}

// recording is a manager that keeps the controllers added to it rather
// than running them, and hands setup a client holding no object.
type recording struct {
	manager.Manager
	client      client.Client
	controllers []reconcile.Reconciler
}

// Add keeps a controller and adds any other runnable to the manager.
func (r *recording) Add(run manager.Runnable) error {
	if c, ok := run.(reconcile.Reconciler); ok {
		r.controllers = append(r.controllers, c)
		return nil
	}
	return r.Manager.Add(run)
}

// GetClient is a client holding no object.
func (r *recording) GetClient() client.Client { return r.client }

// GetFieldIndexer drops every index.
func (r *recording) GetFieldIndexer() client.FieldIndexer { return noIndexer{} }

type noIndexer struct{}

var _ client.FieldIndexer = noIndexer{}

// IndexField registers nothing.
func (noIndexer) IndexField(context.Context, client.Object, string, client.IndexerFunc) error {
	return nil
}
