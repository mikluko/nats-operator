package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// tracerName is the instrumentation scope of the reconcile spans.
const tracerName = "github.com/mikluko/nats-operator/internal/telemetry"

// Traced wraps r so each reconcile of a resource of kind runs in a span
// named "Reconcile <kind>" of the global tracer provider, carrying the
// kind, namespace and name, and marked failed with any error r returns.
func Traced(kind string, r reconcile.Reconciler) reconcile.Reconciler {
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		ctx, span := otel.Tracer(tracerName).Start(ctx, "Reconcile "+kind, trace.WithAttributes(
			attribute.String(AttrKind, kind),
			attribute.String(AttrNamespace, req.Namespace),
			attribute.String(AttrName, req.Name),
		))
		defer span.End()
		res, err := r.Reconcile(ctx, req)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return res, err
	})
}
