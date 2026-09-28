package authctl

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
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/refindex"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// OperatorTrustReconciler mirrors into a reference-form NatsOperatorTrust's
// status the NATS operator and system account JWTs of the NatsOperator it names.
// Where the reference is not admitted,
// or the NatsOperator has not signed both JWTs, status carries neither.
type OperatorTrustReconciler struct {
	client.Client
}

var _ reconcile.Reconciler = (*OperatorTrustReconciler)(nil)

// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsoperatortrusts,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsoperatortrusts/status,verbs=update
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsoperators,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsreferencegrants,verbs=list;watch

// Reconcile mirrors into the status of the NatsOperatorTrust req names the
// JWTs of the NatsOperator it references.
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
	observe(&t.Status.Conditions, &t.Status.ObservedGeneration, t.Generation, err)
	return reconcile.Result{}, updateStatus(ctx, r.Client, &t, before, &t.Status, err)
}

func (r *OperatorTrustReconciler) reconcile(ctx context.Context, t *natsv1beta1.NatsOperatorTrust) error {
	st := &t.Status
	notReady := func(reason, msg string) {
		st.OperatorJWT, st.SystemAccountJWT = "", ""
		conditions.Set(&st.Conditions, t.Generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: msg})
	}
	key := t.Spec.OperatorRef.ObjectKey(t.Namespace)
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
	conditions.Set(&st.Conditions, t.Generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonMirrored})
	return nil
}

// SetupWithManager registers r with mgr, which must already hold Setup's indexes.
func (r *OperatorTrustReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	return ctrl.NewControllerManagedBy(mgr).
		Named("natsoperatortrust").
		For(&natsv1beta1.NatsOperatorTrust{}).
		Watches(&authv1beta1.NatsOperator{}, refindex.EnqueueByField(c, &natsv1beta1.NatsOperatorTrustList{}, operatorField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: natsGroup, Kind: "NatsOperatorTrust"}, &natsv1beta1.NatsOperatorTrustList{})).
		Complete(telemetry.Traced("NatsOperatorTrust", r))
}

// AccountTrustReconciler mirrors into a reference-form NatsAccountTrust's
// status the public key and JWT of the NatsAccount it names. Where the
// reference is not admitted, or the account
// has no JWT yet, status carries neither.
type AccountTrustReconciler struct {
	client.Client
}

var _ reconcile.Reconciler = (*AccountTrustReconciler)(nil)

// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsaccounttrusts,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsaccounttrusts/status,verbs=update
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsreferencegrants,verbs=list;watch

// Reconcile mirrors into the status of the NatsAccountTrust req names the
// public key and JWT of the NatsAccount it references.
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
	observe(&t.Status.Conditions, &t.Status.ObservedGeneration, t.Generation, err)
	return reconcile.Result{}, updateStatus(ctx, r.Client, &t, before, &t.Status, err)
}

func (r *AccountTrustReconciler) reconcile(ctx context.Context, t *natsv1beta1.NatsAccountTrust) error {
	st := &t.Status
	notReady := func(reason, msg string) {
		st.PublicKey, st.JWT = "", ""
		conditions.Set(&st.Conditions, t.Generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: msg})
	}
	key := t.Spec.AccountRef.ObjectKey(t.Namespace)
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
	conditions.Set(&st.Conditions, t.Generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonMirrored})
	return nil
}

// SetupWithManager registers r with mgr, which must already hold Setup's indexes.
func (r *AccountTrustReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	return ctrl.NewControllerManagedBy(mgr).
		Named("natsaccounttrust").
		For(&natsv1beta1.NatsAccountTrust{}).
		Watches(&authv1beta1.NatsAccount{}, refindex.EnqueueByField(c, &natsv1beta1.NatsAccountTrustList{}, accountField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: natsGroup, Kind: "NatsAccountTrust"}, &natsv1beta1.NatsAccountTrustList{})).
		Complete(telemetry.Traced("NatsAccountTrust", r))
}

// referenceAdmitted records on conds whether cond, from admit, admitted a
// reference, calling notReady where it did not.
func referenceAdmitted(conds *[]metav1.Condition, gen int64, cond *metav1.Condition, notReady func(reason, msg string)) bool {
	if cond != nil {
		conditions.Set(conds, gen, *cond)
		notReady(grant.ReasonReferenceNotPermitted, cond.Message)
		return false
	}
	conditions.Set(conds, gen, metav1.Condition{Type: grant.ConditionReferencesResolved, Status: metav1.ConditionTrue, Reason: ReasonResolved})
	return true
}

// result is res where err is nil, and err alone otherwise: controller-runtime
// ignores a requeue returned beside an error and retries with backoff.
func result(res reconcile.Result, err error) (reconcile.Result, error) {
	if err != nil {
		return reconcile.Result{}, err
	}
	return res, nil
}

// updateStatus writes obj's status where it differs from before, joining
// any failure to err.
func updateStatus(ctx context.Context, c client.Client, obj client.Object, before, after any, err error) error {
	if equality.Semantic.DeepEqual(before, after) {
		return err
	}
	return errors.Join(err, c.Status().Update(ctx, obj))
}
