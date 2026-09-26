// Package grant decides whether a reference may cross into another
// namespace, which it may only where a NatsReferenceGrant in the target
// namespace admits it. Nothing is cached between calls: a controller asks on
// every reconcile, so deleting a grant revokes what it admitted.
package grant

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// Condition vocabulary a referrer's status carries for its references.
const (
	// ConditionReferencesResolved is the condition type Admit reports on.
	ConditionReferencesResolved = "ReferencesResolved"
	// ReasonNoGrant is the ReferencesResolved reason when no grant admits
	// a reference.
	ReasonNoGrant = "NoGrant"
	// ReasonReferenceNotPermitted is the Ready reason of a referrer whose
	// ReferencesResolved is False with ReasonNoGrant.
	ReasonReferenceNotPermitted = "ReferenceNotPermitted"
)

// Referrer is the object that holds a reference.
type Referrer struct {
	Group     string
	Kind      string
	Namespace string
}

// Target is the object a reference names. An empty Namespace is the
// referrer's own.
type Target struct {
	Group     string
	Kind      string
	Namespace string
	Name      string
}

// Admit reports whether from may reference to. It returns nil when the
// reference stays in from's namespace, without reading anything, or when a
// NatsReferenceGrant in to's namespace lists from in its from and to in its
// to. Otherwise it returns a ReferencesResolved=False condition with
// ReasonNoGrant; the caller sets ObservedGeneration. An error is a failure to
// list grants, never a denial.
func Admit(ctx context.Context, r client.Reader, from Referrer, to Target) (*metav1.Condition, error) {
	if to.Namespace == "" || to.Namespace == from.Namespace {
		return nil, nil
	}
	var grants natsv1beta1.NatsReferenceGrantList
	if err := r.List(ctx, &grants, client.InNamespace(to.Namespace)); err != nil {
		return nil, fmt.Errorf("list NatsReferenceGrants in %s: %w", to.Namespace, err)
	}
	for i := range grants.Items {
		if admits(&grants.Items[i].Spec, from, to) {
			return nil, nil
		}
	}
	return &metav1.Condition{
		Type:   ConditionReferencesResolved,
		Status: metav1.ConditionFalse,
		Reason: ReasonNoGrant,
		Message: fmt.Sprintf("no NatsReferenceGrant in %s admits %s from namespace %s to %s %s",
			to.Namespace, from.Kind, from.Namespace, to.Kind, to.Name),
	}, nil
}

// admits reports whether spec lists from and to; a to entry without a name
// covers every object of its kind.
func admits(spec *natsv1beta1.NatsReferenceGrantSpec, from Referrer, to Target) bool {
	var fromOK bool
	for _, f := range spec.From {
		if f.Group == from.Group && f.Kind == from.Kind && f.Namespace == from.Namespace {
			fromOK = true
			break
		}
	}
	if !fromOK {
		return false
	}
	for _, t := range spec.To {
		if t.Group == to.Group && t.Kind == to.Kind && (t.Name == "" || t.Name == to.Name) {
			return true
		}
	}
	return false
}
