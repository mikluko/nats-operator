package telemetry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestTraced(t *testing.T) {
	prev := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	spans := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))

	failed := errors.New("patch status: conflict")
	tests := []struct {
		name   string
		err    error
		status codes.Code
	}{
		{name: "ok", status: codes.Unset},
		{name: "failed", err: failed, status: codes.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spans.Reset()
			var inSpan bool
			r := Traced("NatsCluster", reconcile.Func(func(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
				inSpan = otelSpanRecording(ctx)
				return reconcile.Result{RequeueAfter: time.Second}, tt.err
			}))
			res, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "demo"}})
			require.ErrorIs(t, err, tt.err)
			require.Equal(t, time.Second, res.RequeueAfter)
			require.True(t, inSpan, "the reconcile ran outside its span")
			ended := spans.Ended()
			require.Len(t, ended, 1)
			s := ended[0]
			require.Equal(t, "Reconcile NatsCluster", s.Name())
			require.ElementsMatch(t, []attribute.KeyValue{
				attribute.String(AttrKind, "NatsCluster"),
				attribute.String(AttrNamespace, "ns"),
				attribute.String(AttrName, "demo"),
			}, s.Attributes())
			require.Equal(t, tt.status, s.Status().Code)
		})
	}
}

func otelSpanRecording(ctx context.Context) bool {
	return trace.SpanFromContext(ctx).IsRecording()
}
