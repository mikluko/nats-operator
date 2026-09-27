package authctl

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
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// SystemAccountReconciler keeps a NatsSystemAccount's keys and reports
// whether it is the live system account. Its JWT is signed by the
// OperatorReconciler of the NatsOperator whose systemAccountRef names it,
// and lives in that operator's status; an unreferenced one is not signed.
// status.revocations records what that JWT revokes, and what the next one
// is to revoke, as AccountReconciler records an account's.
// It is not pushed again while the operator's RevocationsUnrecovered is
// True, so the servers keep the JWT the revocations are recovered from.
// A newly signed JWT resets status.distribution to no server current; with
// a Distributor, status.distribution and the Distributed condition then
// follow the servers holding it.
type SystemAccountReconciler struct {
	client.Client
	// Distributor pushes the JWT again to servers without it; nil pushes
	// nothing.
	Distributor Distributor
	// RosterChanges receives a NatsOperator whose servers changed; the
	// system accounts naming it are reconciled. Nil receives nothing.
	RosterChanges <-chan event.GenericEvent
}

// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natssystemaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natssystemaccounts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsoperators,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsusers,verbs=get;list;watch

// Reconcile implements reconcile.Reconciler.
func (r *SystemAccountReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var sys authv1beta1.NatsSystemAccount
	if err := r.Get(ctx, req.NamespacedName, &sys); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	before := sys.Status.DeepCopy()
	again, err := r.reconcile(ctx, &sys)
	sys.Status.ObservedGeneration = sys.Generation
	return reconcile.Result{RequeueAfter: again}, updateStatus(ctx, r.Client, &sys, before, &sys.Status, err)
}

// reconcile returns how soon to look at sys again.
func (r *SystemAccountReconciler) reconcile(ctx context.Context, sys *authv1beta1.NatsSystemAccount) (time.Duration, error) {
	st := &sys.Status
	notReady := func(reason, msg string) {
		st.JWTHash = ""
		conditions.Set(&st.Conditions, sys.Generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: msg})
	}
	keys, err := resolveKeys(ctx, r.Client, systemAccountKeySource(sys), true)
	if err != nil {
		return 0, keysFailed(err, notReady)
	}
	pub, err := keys.identityPublicKey()
	if err != nil {
		return 0, err
	}
	st.PublicKey = pub

	key := refKey(sys.Spec.OperatorRef, sys.Namespace)
	cond, err := admit(ctx, r.Client, authGroup, "NatsSystemAccount", sys, "NatsOperator", key)
	if err != nil {
		return 0, err
	}
	if !referenceAdmitted(&st.Conditions, sys.Generation, cond, notReady) {
		return 0, nil
	}
	var op authv1beta1.NatsOperator
	if err := r.Get(ctx, key, &op); err != nil {
		if apierrors.IsNotFound(err) {
			notReady(ReasonNotFound, fmt.Sprintf("NatsOperator %s does not exist", key))
			return 0, nil
		}
		return 0, err
	}
	if refKey(op.Spec.SystemAccountRef, op.Namespace) != client.ObjectKeyFromObject(sys) {
		notReady(ReasonNotReferenced, fmt.Sprintf("NatsOperator %s names another system account", key))
		return 0, nil
	}
	signed := op.Status.SystemAccount
	if signed == nil || signed.Name != sys.Name || signed.PublicKey != pub {
		notReady(ReasonPending, fmt.Sprintf("NatsOperator %s has not signed this account yet", key))
		return 0, nil
	}
	users, err := listUsers(ctx, r.Client, authv1beta1.AccountKindSystemAccount, client.ObjectKeyFromObject(sys))
	if err != nil {
		return 0, err
	}
	signing, _, err := keys.signingPublicKeys()
	if err != nil {
		return 0, err
	}
	st.Revocations = accountRevocations(st.Revocations, signed.JWT, pub, signing, users)
	if hash := JWTHash(signed.JWT); hash != st.JWTHash {
		st.JWTHash = hash
		st.Distribution = pushed(st.Distribution, time.Now(), r.Distributor != nil)
	}
	conditions.Set(&st.Conditions, sys.Generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSigned})
	if unrecovered(op.Status.Conditions) {
		return 0, nil
	}
	dist, distributed, again, err := distribute(ctx, r.Distributor, key, signed.JWT, st.Distribution)
	st.Distribution = dist
	recordDistribution(&st.Conditions, sys.Generation, distributed)
	return again, err
}

// SetupWithManager registers the reconciler with mgr. The indexes Setup
// registers must be in place.
func (r *SystemAccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	b := ctrl.NewControllerManagedBy(mgr)
	if r.RosterChanges != nil {
		b = b.WatchesRawSource(source.Channel(r.RosterChanges, enqueueIndexed(c, &authv1beta1.NatsSystemAccountList{}, operatorField)))
	}
	return b.
		Named("natssystemaccount").
		For(&authv1beta1.NatsSystemAccount{}).
		Owns(&corev1.Secret{}).
		Watches(&corev1.Secret{}, enqueueIndexed(c, &authv1beta1.NatsSystemAccountList{}, seedSecretField)).
		Watches(&authv1beta1.NatsOperator{}, enqueueIndexed(c, &authv1beta1.NatsSystemAccountList{}, operatorField)).
		Watches(&authv1beta1.NatsUser{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			ref := obj.(*authv1beta1.NatsUser).Spec.AccountRef
			if ref.Kind != authv1beta1.AccountKindSystemAccount {
				return nil
			}
			return []reconcile.Request{{NamespacedName: refKey(ref.ObjectReference, obj.GetNamespace())}}
		})).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: authGroup, Kind: "NatsSystemAccount"}, &authv1beta1.NatsSystemAccountList{})).
		Complete(telemetry.Traced("NatsSystemAccount", r))
}
