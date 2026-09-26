package auth

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
)

// OperatorTrustReconciler mirrors into a reference-form NatsOperatorTrust's
// status the operator and system account JWTs of the NatsOperator it names.
// A literal-form one is left alone. Where the reference is not admitted,
// or the operator has not signed both JWTs, status carries neither.
type OperatorTrustReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsoperatortrusts,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsoperatortrusts/status,verbs=get;update;patch

// Reconcile implements reconcile.Reconciler.
func (r *OperatorTrustReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var t natsv1beta1.NatsOperatorTrust
	if err := r.Get(ctx, req.NamespacedName, &t); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if t.Spec.OperatorRef == nil {
		return reconcile.Result{}, nil
	}
	before := t.Status.DeepCopy()
	err := r.reconcile(ctx, &t)
	t.Status.ObservedGeneration = t.Generation
	return reconcile.Result{}, updateStatus(ctx, r.Client, &t, before, &t.Status, err)
}

func (r *OperatorTrustReconciler) reconcile(ctx context.Context, t *natsv1beta1.NatsOperatorTrust) error {
	st := &t.Status
	notReady := func(reason, msg string) {
		st.OperatorJWT, st.SystemAccountJWT = "", ""
		setCondition(&st.Conditions, t.Generation, ConditionReady, metav1.ConditionFalse, reason, msg)
	}
	key := refKey(*t.Spec.OperatorRef, t.Namespace)
	cond, err := admit(ctx, r.Client, natsGroup, "NatsOperatorTrust", t, "NatsOperator", key)
	if err != nil {
		return err
	}
	if !referenceAdmitted(&st.Conditions, t.Generation, cond, notReady) {
		return nil
	}
	var op authv1beta1.NatsOperator
	if err := r.Get(ctx, key, &op); err != nil {
		if apierrors.IsNotFound(err) {
			notReady(ReasonNotFound, fmt.Sprintf("NatsOperator %s does not exist", key))
			return nil
		}
		return err
	}
	if op.Status.JWT == "" || op.Status.SystemAccount == nil || op.Status.SystemAccount.JWT == "" {
		notReady(ReasonPending, fmt.Sprintf("NatsOperator %s has not signed its JWTs yet", key))
		return nil
	}
	st.OperatorJWT = op.Status.JWT
	st.SystemAccountJWT = op.Status.SystemAccount.JWT
	setCondition(&st.Conditions, t.Generation, ConditionReady, metav1.ConditionTrue, ReasonMirrored, "")
	return nil
}

// SetupWithManager registers the reconciler with mgr. The indexes Setup
// registers must be in place.
func (r *OperatorTrustReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	return ctrl.NewControllerManagedBy(mgr).
		Named("natsoperatortrust").
		For(&natsv1beta1.NatsOperatorTrust{}).
		Watches(&authv1beta1.NatsOperator{}, enqueueIndexed(c, &natsv1beta1.NatsOperatorTrustList{}, operatorField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: natsGroup, Kind: "NatsOperatorTrust"}, &natsv1beta1.NatsOperatorTrustList{})).
		Complete(r)
}

// AccountTrustReconciler mirrors into a reference-form NatsAccountTrust's
// status the public key and JWT of the NatsAccount it names. A literal-form
// one is left alone. Where the reference is not admitted, or the account
// has no JWT yet, status carries neither.
type AccountTrustReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsaccounttrusts,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsaccounttrusts/status,verbs=get;update;patch

// Reconcile implements reconcile.Reconciler.
func (r *AccountTrustReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var t natsv1beta1.NatsAccountTrust
	if err := r.Get(ctx, req.NamespacedName, &t); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if t.Spec.AccountRef == nil {
		return reconcile.Result{}, nil
	}
	before := t.Status.DeepCopy()
	err := r.reconcile(ctx, &t)
	t.Status.ObservedGeneration = t.Generation
	return reconcile.Result{}, updateStatus(ctx, r.Client, &t, before, &t.Status, err)
}

func (r *AccountTrustReconciler) reconcile(ctx context.Context, t *natsv1beta1.NatsAccountTrust) error {
	st := &t.Status
	notReady := func(reason, msg string) {
		st.PublicKey, st.JWT = "", ""
		setCondition(&st.Conditions, t.Generation, ConditionReady, metav1.ConditionFalse, reason, msg)
	}
	key := refKey(*t.Spec.AccountRef, t.Namespace)
	cond, err := admit(ctx, r.Client, natsGroup, "NatsAccountTrust", t, "NatsAccount", key)
	if err != nil {
		return err
	}
	if !referenceAdmitted(&st.Conditions, t.Generation, cond, notReady) {
		return nil
	}
	var acc authv1beta1.NatsAccount
	if err := r.Get(ctx, key, &acc); err != nil {
		if apierrors.IsNotFound(err) {
			notReady(ReasonNotFound, fmt.Sprintf("NatsAccount %s does not exist", key))
			return nil
		}
		return err
	}
	if acc.Status.JWT == "" {
		notReady(ReasonPending, fmt.Sprintf("NatsAccount %s has no JWT yet", key))
		return nil
	}
	st.PublicKey = acc.Status.PublicKey
	st.JWT = acc.Status.JWT
	setCondition(&st.Conditions, t.Generation, ConditionReady, metav1.ConditionTrue, ReasonMirrored, "")
	return nil
}

// SetupWithManager registers the reconciler with mgr. The indexes Setup
// registers must be in place.
func (r *AccountTrustReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	return ctrl.NewControllerManagedBy(mgr).
		Named("natsaccounttrust").
		For(&natsv1beta1.NatsAccountTrust{}).
		Watches(&authv1beta1.NatsAccount{}, enqueueIndexed(c, &natsv1beta1.NatsAccountTrustList{}, accountField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: natsGroup, Kind: "NatsAccountTrust"}, &natsv1beta1.NatsAccountTrustList{})).
		Complete(r)
}

// referenceAdmitted records on conds whether cond, from admit, admitted a
// reference, calling notReady where it did not.
func referenceAdmitted(conds *[]metav1.Condition, gen int64, cond *metav1.Condition, notReady func(reason, msg string)) bool {
	if cond != nil {
		cond.ObservedGeneration = gen
		setConditionFrom(conds, *cond)
		notReady(grant.ReasonReferenceNotPermitted, cond.Message)
		return false
	}
	setCondition(conds, gen, grant.ConditionReferencesResolved, metav1.ConditionTrue, ReasonResolved, "")
	return true
}

// updateStatus writes obj's status where it differs from before, joining
// any failure to err.
func updateStatus(ctx context.Context, c client.Client, obj client.Object, before, after any, err error) error {
	if equality.Semantic.DeepEqual(before, after) {
		return err
	}
	return errors.Join(err, c.Status().Update(ctx, obj))
}
