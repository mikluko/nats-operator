package lifecycle

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// TestSync_ResolveResult pins that a failed Resolve returns its error
// alone, which controller-runtime retries with backoff, and a Resolve
// without a server object looks again after natsconn.DefaultRetryAfter.
func TestSync_ResolveResult(t *testing.T) {
	failed := errors.New("no responders")
	tests := []struct {
		name    string
		err     error
		want    reconcile.Result
		wantErr error
	}{
		{"resolve failed", failed, reconcile.Result{}, failed},
		{"nothing to resolve", nil, reconcile.Result{RequeueAfter: natsconn.DefaultRetryAfter}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := Kind[*jetstreamv1beta1.NatsStream]{
				Resolve: func(context.Context, client.Client, *natsconn.Dialer, *jetstreamv1beta1.NatsStream, bool) (Object, bool, error) {
					return nil, false, tt.err
				},
			}
			res, err := k.sync(t.Context(), nil, nil, Syncer{}, &jetstreamv1beta1.NatsStream{})
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.want, res)
		})
	}
}
