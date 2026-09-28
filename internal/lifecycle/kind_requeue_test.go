package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// TestSync_ResolveResult pins that a failed Resolve returns its error with no
// requeue, and a Resolve without a server object requeues after
// natsconn.DefaultRetryAfter.
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

// TestSync_ObserveResult pins that an Observe error on a synced object
// returns that error with no requeue and turns Ready False naming it, and
// that a nil Observe error leaves Ready True with the resync requeue.
func TestSync_ObserveResult(t *testing.T) {
	const owner = types.UID("5b1e0c4a")
	const resync = time.Hour
	failed := errors.New("consumer list: timeout")
	tests := []struct {
		name       string
		err        error
		want       reconcile.Result
		wantReady  metav1.ConditionStatus
		wantReason string
	}{
		{"observe failed", failed, reconcile.Result{}, metav1.ConditionFalse, ReasonObserveFailed},
		{"observed", nil, reconcile.Result{RequeueAfter: resync}, metav1.ConditionTrue, ReasonSynced},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{"metadata": map[string]any{OwnerKey: string(owner), OriginKey: string(jetstreamv1beta1.OwnershipCreated)}}
			k := Kind[*jetstreamv1beta1.NatsStream]{
				List: func() client.ObjectList { return &jetstreamv1beta1.NatsStreamList{} },
				Fields: func(s *jetstreamv1beta1.NatsStream) Fields {
					return Fields{Policies: s.Spec.Policies, Sync: &s.Status.SyncStatus, Status: s.Status}
				},
				Resolve: func(context.Context, client.Client, *natsconn.Dialer, *jetstreamv1beta1.NatsStream, bool) (Object, bool, error) {
					return fetchOnly{info: &Info{Config: cfg}}, false, nil
				},
				Record:  func(*jetstreamv1beta1.NatsStream, *Info) {},
				Observe: func(context.Context, Object, *jetstreamv1beta1.NatsStream, *Info) error { return tt.err },
			}
			s := &jetstreamv1beta1.NatsStream{ObjectMeta: metav1.ObjectMeta{UID: owner, Generation: 1}}
			res, err := k.sync(t.Context(), nil, nil, Syncer{Resync: resync}, s)
			require.ErrorIs(t, err, tt.err)
			require.Equal(t, tt.want, res)
			ready := meta.FindStatusCondition(s.Status.Conditions, ConditionReady)
			require.NotNil(t, ready)
			require.Equal(t, tt.wantReady, ready.Status)
			require.Equal(t, tt.wantReason, ready.Reason)
			if tt.err != nil {
				require.Contains(t, ready.Message, tt.err.Error())
			}
		})
	}
}
