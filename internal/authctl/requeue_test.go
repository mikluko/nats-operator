package authctl

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestResult pins that a reconcile failing returns its error alone, which
// controller-runtime retries with backoff, and one succeeding its requeue.
func TestResult(t *testing.T) {
	failed := errors.New("status update conflict")
	again := reconcile.Result{RequeueAfter: time.Minute}
	tests := []struct {
		name    string
		err     error
		want    reconcile.Result
		wantErr error
	}{
		{"failed", failed, reconcile.Result{}, failed},
		{"succeeded", nil, again, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := result(again, tt.err)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.want, res)
		})
	}
}
