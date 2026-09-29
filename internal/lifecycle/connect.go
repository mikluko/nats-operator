package lifecycle

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// Ready reasons while a resource cannot reach its connection.
const (
	ReasonConnectionNotFound = "ConnectionNotFound"
	ReasonConnectionFailed   = "ConnectionFailed"
)

// Connect returns the API through the NatsConnection that from's ref names.
// Where Dial yields no connection it applies why to status at generation and
// returns it with a nil API. The error is one the Kubernetes API server
// returned.
func Connect(ctx context.Context, d *natsconn.Dialer, from grant.Referrer, ref natsv1beta1.ObjectReference, status *jetstreamv1beta1.SyncStatus, generation int64) (*API, *NoConn, error) {
	nc, why, err := Dial(ctx, d, from, ref)
	if why != nil {
		status.ObservedGeneration = generation
		why.Apply(&status.Conditions, generation)
		return nil, why, nil
	}
	if err != nil {
		return nil, nil, err
	}
	meta.RemoveStatusCondition(&status.Conditions, grant.ConditionReferencesResolved)
	return &API{Conn: nc}, nil, nil
}

// Resolve returns the connection from's ref names and clears the
// ReferencesResolved condition from conds. Where Dial yields no connection it
// applies why to conds at generation and returns nil and no error. The error
// is one the Kubernetes API server returned.
func Resolve(ctx context.Context, d *natsconn.Dialer, from grant.Referrer, ref natsv1beta1.ObjectReference, conds *[]metav1.Condition, generation int64) (*nats.Conn, error) {
	nc, why, err := Dial(ctx, d, from, ref)
	if why != nil {
		why.Apply(conds, generation)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	meta.RemoveStatusCondition(conds, grant.ConditionReferencesResolved)
	return nc, nil
}

// A NoConn is why a reference yields no connection: Ready's reason and
// message, and the ReferencesResolved condition a grant's refusal sets.
type NoConn struct {
	Reason, Message string
	Denied          *metav1.Condition
}

// Apply records n in conds at generation: Ready False, and Denied where a
// grant refused the reference.
func (n *NoConn) Apply(conds *[]metav1.Condition, generation int64) {
	if n.Denied != nil {
		conditions.Set(conds, generation, *n.Denied)
	}
	conditions.Set(conds, generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: n.Reason, Message: n.Message})
}

// Released reports whether a resource being deleted without a connection
// drops its finalizer without running its deletion policy: true when the
// NatsConnection does not exist or no grant admits it, false when it exists
// and fails or n is nil.
func (n *NoConn) Released() bool {
	return n != nil && (n.Reason == ReasonConnectionNotFound || n.Reason == grant.ReasonReferenceNotPermitted)
}

// Dial returns the connection from's ref names, or why there is none: no
// grant admits the reference, the NatsConnection does not exist, or its
// Secrets or servers fail. The error is one the Kubernetes API server
// returned.
func Dial(ctx context.Context, d *natsconn.Dialer, from grant.Referrer, ref natsv1beta1.ObjectReference) (*nats.Conn, *NoConn, error) {
	nc, denied, err := d.Reference(ctx, from, ref)
	switch {
	case denied != nil:
		return nil, &NoConn{Reason: grant.ReasonReferenceNotPermitted, Message: denied.Message, Denied: denied}, nil
	case apierrors.IsNotFound(err):
		return nil, &NoConn{Reason: ReasonConnectionNotFound, Message: err.Error()}, nil
	case isAPIStatus(err):
		return nil, nil, err
	case err != nil:
		return nil, &NoConn{Reason: ReasonConnectionFailed, Message: err.Error()}, nil
	}
	return nc, nil, nil
}

func isAPIStatus(err error) bool {
	var s apierrors.APIStatus
	return errors.As(err, &s)
}
