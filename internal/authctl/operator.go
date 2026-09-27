package authctl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// OperatorReconciler signs a NatsOperator's JWT and the JWT of the
// NatsSystemAccount its systemAccountRef names, and writes both to its
// status. It generates the keys spec omits.
//
// The system account JWT imports the jetstream-stepdown exports of every
// NatsAccount the operator signs that carries the preset, and revokes the
// keys of its NatsUsers as AccountReconciler does an account's, the
// revocations SystemAccountReconciler records in the NatsSystemAccount's
// status among them. Where neither that record nor status.systemAccount
// holds anything to sign them from, they are recovered as AccountReconciler
// recovers an account's, the NatsSystemAccount's status.distribution
// saying whether it was distributed and the operator carrying
// RevocationsUnrecovered; an operator whose status holds no JWT is taken
// to have signed nothing yet. A system account JWT in status that the
// servers hold a newer one of, as after a push whose status write was
// lost, is signed and pushed afresh.
//
// status.deletedAccounts records each NatsAccount naming the operator that
// is being deleted and has a JWT, and keeps it until that JWT expires or
// its key is signed again; the Distributor is handed the request deleting
// them.
type OperatorReconciler struct {
	client.Client
	// Distributor receives the system account JWT whenever it is newly
	// signed; nil pushes nothing.
	Distributor Distributor
	// Recorder records system account JWTs pushed and held; nil records
	// none.
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsoperators,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsoperators/status,verbs=update
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsoperators/finalizers,verbs=update
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natssystemaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsaccounts;natsusers,verbs=list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsreferencegrants,verbs=list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create

// Reconcile implements reconcile.Reconciler.
func (r *OperatorReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var op authv1beta1.NatsOperator
	if err := r.Get(ctx, req.NamespacedName, &op); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	before := op.Status.DeepCopy()
	again, err := r.reconcile(ctx, &op)
	op.Status.ObservedGeneration = op.Generation
	return reconcile.Result{RequeueAfter: again}, updateStatus(ctx, r.Client, &op, before, &op.Status, err)
}

// reconcile returns how soon to look at op again.
func (r *OperatorReconciler) reconcile(ctx context.Context, op *authv1beta1.NatsOperator) (time.Duration, error) {
	st := &op.Status
	notReady := func(reason, msg string) {
		conditions.Set(&st.Conditions, op.Generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: msg})
	}
	var named authv1beta1.NatsAccountList
	if err := r.List(ctx, &named, client.MatchingFields{operatorField: keyValue(client.ObjectKeyFromObject(op))}); err != nil {
		return 0, fmt.Errorf("list NatsAccounts: %w", err)
	}
	st.DeletedAccounts = recordDeleting(st.DeletedAccounts, named.Items)
	signedBefore := st.JWT != ""
	src, err := operatorKeySource(op)
	if err != nil {
		notReady(ReasonInvalidJWT, err.Error())
		return 0, nil
	}
	keys, err := resolveKeys(ctx, r.Client, src, true)
	if err != nil {
		return 0, keysFailed(err, notReady)
	}
	pub, err := keys.identityPublicKey()
	if err != nil {
		return 0, err
	}
	signing, retiring, err := keys.signingPublicKeys()
	if err != nil {
		return 0, err
	}
	st.PublicKey = pub
	st.SigningKeys = signing
	st.SeedSecrets = nil
	if keys.Generated.Identity != "" || len(keys.Generated.Signing) > 0 {
		st.SeedSecrets = keys.Generated.DeepCopy()
	}

	sys, sysKeys, ok, err := r.systemAccount(ctx, op, notReady)
	if !ok || err != nil {
		return 0, err
	}
	sysPub, err := sysKeys.identityPublicKey()
	if err != nil {
		return 0, err
	}

	opJWT, err := jwtplane.SignOperator(jwtplane.Operator{Name: op.Name, Keys: keys.Keys, SystemAccount: sysPub, JWT: op.Spec.JWT})
	if err != nil {
		notReady(ReasonInvalidJWT, err.Error())
		return 0, nil
	}
	if !sameOperatorClaims(st.JWT, opJWT) {
		st.JWT = opJWT
	}

	accounts, err := r.signedAccounts(ctx, op, named.Items)
	if err != nil {
		return 0, err
	}
	users, err := listUsers(ctx, r.Client, authv1beta1.AccountKindSystemAccount, client.ObjectKeyFromObject(sys))
	if err != nil {
		return 0, err
	}
	prev := st.SystemAccount
	var prevJWT string
	if prev != nil && prev.Name == sys.Name {
		prevJWT = prev.JWT
	}
	sysSigning, _, err := sysKeys.signingPublicKeys()
	if err != nil {
		return 0, err
	}
	var lookup Distributor
	if signedBefore {
		lookup = r.Distributor
	}
	sd, err := recoverRevocations(ctx, lookup, client.ObjectKeyFromObject(op), sys.Status.Revocations, prevJWT, sysPub, sysSigning, users,
		unrecovered(st.Conditions), everDistributed(sys.Status.Distribution))
	if err != nil {
		recordHeld(r.Recorder, op, st.Conditions, err)
		return recoveryFailed(err, notReady)
	}
	recordRecovery(&st.Conditions, op.Generation, sd)
	sysJWT, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{
		Name:             sys.Name,
		Keys:             sysKeys.Keys,
		StepdownAccounts: stepdownAccounts(accounts),
		Revocations:      signedRevocations(sd.revocations),
	}, keys.Keys, time.Now())
	if err != nil {
		notReady(ReasonInvalidKeys, err.Error())
		return 0, nil
	}
	resign := prev == nil || prev.Name != sys.Name || !sameAccountClaims(prev.JWT, sysJWT)
	if !resign && !unrecovered(st.Conditions) {
		resign = repushStale(ctx, r.Distributor, client.ObjectKeyFromObject(op), prev.JWT)
	}
	if resign {
		err := push(ctx, r.Distributor, client.ObjectKeyFromObject(op), sysJWT)
		if err := ignoreUnreachable(err); err != nil {
			return 0, fmt.Errorf("push system account JWT: %w", err)
		}
		if r.Distributor != nil && err == nil {
			telemetry.Emit(r.Recorder, op, telemetry.JWTPushed, "system account JWT of %s pushed", sysPub)
		}
		st.SystemAccount = &authv1beta1.SystemAccountStatus{Name: sys.Name, PublicKey: sysPub, JWT: sysJWT}
	}

	setRetiringCondition(op, retiring, append([]string{st.SystemAccount.JWT}, accountJWTs(accounts)...))
	next, err := r.deletes(ctx, op, keys.Keys, named.Items)
	if err != nil {
		return 0, err
	}
	conditions.Set(&st.Conditions, op.Generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSigned})
	if next.IsZero() {
		return 0, nil
	}
	return max(time.Until(next), time.Second), nil
}

// deletes prunes op's deleted accounts of those whose JWTs have expired
// and those whose keys its system account, or an account in accounts not
// being deleted, holds again, and hands the Distributor the request
// deleting the rest. It returns when the next of them expires, the zero
// time for never.
func (r *OperatorReconciler) deletes(ctx context.Context, op *authv1beta1.NatsOperator, keys jwtplane.Keys, accounts []authv1beta1.NatsAccount) (time.Time, error) {
	live := liveKeys(op, accounts)
	pruned, next := pruneDeleted(op.Status.DeletedAccounts, live, time.Now())
	op.Status.DeletedAccounts = pruned
	if r.Distributor == nil {
		return next, nil
	}
	request, err := deleteRequest(keys, pruned)
	if err != nil {
		return time.Time{}, fmt.Errorf("sign delete request: %w", err)
	}
	if err := ignoreUnreachable(r.Distributor.Delete(ctx, client.ObjectKeyFromObject(op), request)); err != nil {
		return time.Time{}, fmt.Errorf("delete accounts: %w", err)
	}
	return next, nil
}

// keysFailed reports a key that cannot be read: a missing seed is waited
// for, a malformed one is a spec error, and anything else is retried.
func keysFailed(err error, notReady func(reason, msg string)) error {
	switch {
	case errors.Is(err, errKeysPending):
		notReady(ReasonKeysPending, err.Error())
		return nil
	case errors.Is(err, errInvalidSeed):
		notReady(ReasonInvalidKeys, err.Error())
		return nil
	}
	return err
}

// systemAccount returns the NatsSystemAccount op names and its keys. ok is
// false where the reference cannot be followed yet, with Ready set to say
// why.
func (r *OperatorReconciler) systemAccount(ctx context.Context, op *authv1beta1.NatsOperator, notReady func(reason, msg string)) (*authv1beta1.NatsSystemAccount, resolvedKeys, bool, error) {
	key := refKey(op.Spec.SystemAccountRef, op.Namespace)
	cond, err := admit(ctx, r.Client, authGroup, "NatsOperator", op, "NatsSystemAccount", key)
	if err != nil {
		return nil, resolvedKeys{}, false, err
	}
	if !referenceAdmitted(&op.Status.Conditions, op.Generation, cond, notReady) {
		return nil, resolvedKeys{}, false, nil
	}
	var sys authv1beta1.NatsSystemAccount
	if err := r.Get(ctx, key, &sys); err != nil {
		if apierrors.IsNotFound(err) {
			notReady(ReasonNotFound, fmt.Sprintf("NatsSystemAccount %s does not exist", key))
			return nil, resolvedKeys{}, false, nil
		}
		return nil, resolvedKeys{}, false, err
	}
	if refKey(sys.Spec.OperatorRef, sys.Namespace) != client.ObjectKeyFromObject(op) {
		notReady(ReasonOperatorMismatch, fmt.Sprintf("NatsSystemAccount %s names another operator", key))
		return nil, resolvedKeys{}, false, nil
	}
	keys, err := resolveKeys(ctx, r.Client, systemAccountKeySource(&sys), false)
	if err != nil {
		return nil, resolvedKeys{}, false, keysFailed(fmt.Errorf("NatsSystemAccount %s: %w", key, err), notReady)
	}
	return &sys, keys, true, nil
}

// signedAccounts returns the accounts in accounts, the NatsAccounts naming
// op, that are admitted to it.
func (r *OperatorReconciler) signedAccounts(ctx context.Context, op *authv1beta1.NatsOperator, accounts []authv1beta1.NatsAccount) ([]authv1beta1.NatsAccount, error) {
	var out []authv1beta1.NatsAccount
	for i := range accounts {
		acc := &accounts[i]
		cond, err := admit(ctx, r.Client, authGroup, "NatsAccount", acc, "NatsOperator", client.ObjectKeyFromObject(op))
		if err != nil {
			return nil, err
		}
		if cond == nil {
			out = append(out, *acc)
		}
	}
	return out, nil
}

// stepdownAccounts returns the sorted public keys of the accounts carrying
// the jetstream-stepdown export preset.
func stepdownAccounts(accounts []authv1beta1.NatsAccount) []string {
	var out []string
	for i := range accounts {
		acc := &accounts[i]
		if acc.Status.PublicKey == "" {
			continue
		}
		for _, e := range acc.Spec.Exports {
			if e.Preset == authv1beta1.ExportPresetJetStreamStepdown {
				out = append(out, acc.Status.PublicKey)
				break
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func accountJWTs(accounts []authv1beta1.NatsAccount) []string {
	out := make([]string, 0, len(accounts))
	for i := range accounts {
		out = append(out, accounts[i].Status.JWT)
	}
	return out
}

// setRetiringCondition sets RetiringKeysInUse from how many of jwts were
// issued by a key in retiring.
func setRetiringCondition(op *authv1beta1.NatsOperator, retiring, jwts []string) {
	var n int
	for _, token := range jwts {
		if token != "" && slices.Contains(retiring, issuer(token)) {
			n++
		}
	}
	if n == 0 {
		conditions.Set(&op.Status.Conditions, op.Generation, metav1.Condition{Type: ConditionRetiringKeysInUse, Status: metav1.ConditionFalse, Reason: ReasonNoneInUse})
		return
	}
	conditions.Set(&op.Status.Conditions, op.Generation, metav1.Condition{Type: ConditionRetiringKeysInUse, Status: metav1.ConditionTrue, Reason: ReasonInUse, Message: fmt.Sprintf("%d account JWTs are signed by a retiring key", n)})

}

// SetupWithManager registers the reconciler with mgr. The indexes Setup
// registers must be in place.
func (r *OperatorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	return ctrl.NewControllerManagedBy(mgr).
		Named("natsoperator").
		For(&authv1beta1.NatsOperator{}).
		Owns(&corev1.Secret{}).
		Watches(&corev1.Secret{}, enqueueIndexed(c, &authv1beta1.NatsOperatorList{}, seedSecretField)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			return systemAccountOperators(ctx, c, listIndexed(ctx, c, &authv1beta1.NatsSystemAccountList{}, seedSecretField, keyValue(client.ObjectKeyFromObject(obj))))
		})).
		Watches(&authv1beta1.NatsSystemAccount{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			sys := obj.(*authv1beta1.NatsSystemAccount)
			reqs := listIndexed(ctx, c, &authv1beta1.NatsOperatorList{}, systemAccountField, keyValue(client.ObjectKeyFromObject(sys)))
			return append(reqs, reconcile.Request{NamespacedName: refKey(sys.Spec.OperatorRef, sys.Namespace)})
		})).
		Watches(&authv1beta1.NatsAccount{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			acc := obj.(*authv1beta1.NatsAccount)
			return []reconcile.Request{{NamespacedName: refKey(acc.Spec.OperatorRef, acc.Namespace)}}
		})).
		Watches(&authv1beta1.NatsUser{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			ref := obj.(*authv1beta1.NatsUser).Spec.AccountRef
			if ref.Kind != authv1beta1.AccountKindSystemAccount {
				return nil
			}
			return systemAccountOperators(ctx, c, []reconcile.Request{{NamespacedName: refKey(ref.ObjectReference, obj.GetNamespace())}})
		})).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: authGroup, Kind: "NatsOperator"}, &authv1beta1.NatsOperatorList{})).
		Complete(telemetry.Traced("NatsOperator", r))
}

// systemAccountOperators maps requests for NatsSystemAccounts to requests
// for the operators they name.
func systemAccountOperators(ctx context.Context, c client.Reader, reqs []reconcile.Request) []reconcile.Request {
	var out []reconcile.Request
	for _, req := range reqs {
		var sys authv1beta1.NatsSystemAccount
		if err := c.Get(ctx, req.NamespacedName, &sys); err != nil {
			continue
		}
		out = append(out, reconcile.Request{NamespacedName: refKey(sys.Spec.OperatorRef, sys.Namespace)})
	}
	return out
}
