package auth

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
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

// SystemAccountReconciler keeps a NatsSystemAccount's keys and reports
// whether it is the live system account. Its JWT is signed by the
// OperatorReconciler of the NatsOperator whose systemAccountRef names it,
// and lives in that operator's status; an unreferenced one is not signed.
// A newly signed JWT resets status.distribution to no server current.
type SystemAccountReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natssystemaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natssystemaccounts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsoperators,verbs=get;list;watch

// Reconcile implements reconcile.Reconciler.
func (r *SystemAccountReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var sys authv1beta1.NatsSystemAccount
	if err := r.Get(ctx, req.NamespacedName, &sys); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	before := sys.Status.DeepCopy()
	err := r.reconcile(ctx, &sys)
	sys.Status.ObservedGeneration = sys.Generation
	return reconcile.Result{}, updateStatus(ctx, r.Client, &sys, before, &sys.Status, err)
}

func (r *SystemAccountReconciler) reconcile(ctx context.Context, sys *authv1beta1.NatsSystemAccount) error {
	st := &sys.Status
	notReady := func(reason, msg string) {
		st.JWTHash = ""
		setCondition(&st.Conditions, sys.Generation, ConditionReady, metav1.ConditionFalse, reason, msg)
	}
	keys, err := resolveKeys(ctx, r.Client, systemAccountKeySource(sys), true)
	if err != nil {
		return keysFailed(err, notReady)
	}
	pub, err := keys.identityPublicKey()
	if err != nil {
		return err
	}
	st.PublicKey = pub

	key := refKey(sys.Spec.OperatorRef, sys.Namespace)
	cond, err := admit(ctx, r.Client, authGroup, "NatsSystemAccount", sys, "NatsOperator", key)
	if err != nil {
		return err
	}
	if !referenceAdmitted(&st.Conditions, sys.Generation, cond, notReady) {
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
	if refKey(op.Spec.SystemAccountRef, op.Namespace) != client.ObjectKeyFromObject(sys) {
		notReady(ReasonNotReferenced, fmt.Sprintf("NatsOperator %s names another system account", key))
		return nil
	}
	signed := op.Status.SystemAccount
	if signed == nil || signed.Name != sys.Name || signed.PublicKey != pub {
		notReady(ReasonPending, fmt.Sprintf("NatsOperator %s has not signed this account yet", key))
		return nil
	}
	if hash := JWTHash(signed.JWT); hash != st.JWTHash {
		st.JWTHash = hash
		st.Distribution = pushed(st.Distribution, time.Now())
	}
	setCondition(&st.Conditions, sys.Generation, ConditionReady, metav1.ConditionTrue, ReasonSigned, "")
	return nil
}

// SetupWithManager registers the reconciler with mgr. The indexes Setup
// registers must be in place.
func (r *SystemAccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	return ctrl.NewControllerManagedBy(mgr).
		Named("natssystemaccount").
		For(&authv1beta1.NatsSystemAccount{}).
		Owns(&corev1.Secret{}).
		Watches(&corev1.Secret{}, enqueueIndexed(c, &authv1beta1.NatsSystemAccountList{}, seedSecretField)).
		Watches(&authv1beta1.NatsOperator{}, enqueueIndexed(c, &authv1beta1.NatsSystemAccountList{}, operatorField)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: authGroup, Kind: "NatsSystemAccount"}, &authv1beta1.NatsSystemAccountList{})).
		Complete(r)
}
