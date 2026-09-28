package natscluster

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/nats-io/jwt/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
)

// Trust is the trust roots a NatsCluster with an auth plane renders.
type Trust struct {
	OperatorJWT      string
	SystemAccountJWT string
	// SystemAccount is the system account's public key.
	SystemAccount string
}

// ErrInvalidTrust wraps every reason ParseTrust refuses a pair of JWTs.
var ErrInvalidTrust = errors.New("invalid trust roots")

// ParseTrust checks that operatorJWT is a NATS operator JWT, that
// systemAccountJWT is an account JWT signed by its identity key or one of its
// signing keys, and, where the NATS operator names a system account, that it
// names this one.
func ParseTrust(operatorJWT, systemAccountJWT string) (*Trust, error) {
	op, err := jwt.DecodeOperatorClaims(operatorJWT)
	if err != nil {
		return nil, fmt.Errorf("%w: operator JWT: %w", ErrInvalidTrust, err)
	}
	acc, err := jwt.DecodeAccountClaims(systemAccountJWT)
	if err != nil {
		return nil, fmt.Errorf("%w: system account JWT: %w", ErrInvalidTrust, err)
	}
	if acc.Issuer != op.Subject && !slices.Contains(op.SigningKeys, acc.Issuer) {
		return nil, fmt.Errorf("%w: system account %s is signed by %s, not by operator %s or its signing keys", ErrInvalidTrust, acc.Subject, acc.Issuer, op.Subject)
	}
	if op.SystemAccount != "" && op.SystemAccount != acc.Subject {
		return nil, fmt.Errorf("%w: operator %s names system account %s, not %s", ErrInvalidTrust, op.Subject, op.SystemAccount, acc.Subject)
	}
	return &Trust{OperatorJWT: operatorJWT, SystemAccountJWT: systemAccountJWT, SystemAccount: acc.Subject}, nil
}

// readTrust returns the trust roots nc's auth.trustRef names, or nil when nc
// has no auth plane. A trust object that is absent, not admitted, not yet
// filled in, or invalid returns nil and the Progressing condition saying so.
func readTrust(ctx context.Context, r client.Reader, nc *clusterv1beta1.NatsCluster) (*Trust, *metav1.Condition, error) {
	if nc.Spec.Auth == nil {
		return nil, nil, nil
	}
	ref := nc.Spec.Auth.TrustRef
	ns := ref.Namespace
	if ns == "" {
		ns = nc.Namespace
	}
	notProgressing := func(reason, msg string) *metav1.Condition {
		return &metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: reason, Message: msg}
	}

	denied, err := grant.Admit(ctx, r,
		grant.Referrer{Group: clusterv1beta1.GroupVersion.Group, Kind: "NatsCluster", Namespace: nc.Namespace},
		grant.Target{Group: natsv1beta1.GroupVersion.Group, Kind: "NatsOperatorTrust", Namespace: ns, Name: ref.Name})
	if err != nil {
		return nil, nil, err
	}
	if denied != nil {
		return nil, notProgressing(denied.Reason, denied.Message), nil
	}

	t := &natsv1beta1.NatsOperatorTrust{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: ref.Name}, t); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, notProgressing(ReasonTrustNotFound, fmt.Sprintf("NatsOperatorTrust %s/%s does not exist", ns, ref.Name)), nil
		}
		return nil, nil, fmt.Errorf("get NatsOperatorTrust %s/%s: %w", ns, ref.Name, err)
	}
	opJWT, sysJWT := t.Spec.OperatorJWT, t.Spec.SystemAccountJWT
	if t.Spec.OperatorRef != nil {
		opJWT, sysJWT = t.Status.OperatorJWT, t.Status.SystemAccountJWT
		if opJWT == "" || sysJWT == "" {
			return nil, notProgressing(ReasonTrustNotReady, fmt.Sprintf("NatsOperatorTrust %s/%s has no JWTs in its status yet", ns, ref.Name)), nil
		}
	}
	trust, err := ParseTrust(opJWT, sysJWT)
	if err != nil {
		return nil, notProgressing(ReasonTrustInvalid, fmt.Sprintf("NatsOperatorTrust %s/%s: %v", ns, ref.Name, err)), nil
	}
	return trust, nil, nil
}
