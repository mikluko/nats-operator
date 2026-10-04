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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/refindex"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// OperatorReconciler signs a NatsOperator's JWT and its live system
// account's JWT into its status; SystemAccountReconciler pushes the latter.
type OperatorReconciler struct {
	client.Client
	// Distributor is asked for the revocations and the newest JWT the
	// servers hold for the system account, and receives deletes; nil asks
	// and deletes nothing.
	Distributor Distributor
	// Recorder records system account JWTs held; nil records none.
	Recorder events.EventRecorder
}

var _ reconcile.Reconciler = (*OperatorReconciler)(nil)

// +kubebuilder:rbac:groups=auth.nats-operator.io,resources=natsoperators,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats-operator.io,resources=natsoperators/status,verbs=update
// +kubebuilder:rbac:groups=auth.nats-operator.io,resources=natssystemaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats-operator.io,resources=natsaccounts;natsusers,verbs=list;watch
// +kubebuilder:rbac:groups=nats-operator.io,resources=natsreferencegrants,verbs=list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create

// Reconcile implements reconcile.Reconciler.
func (r *OperatorReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var op authv1beta1.NatsOperator
	if err := r.Get(ctx, req.NamespacedName, &op); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	before := op.Status.DeepCopy()
	again, err := r.reconcile(ctx, &op)
	observe(&op.Status.Conditions, &op.Status.ObservedGeneration, op.Generation, err)
	return result(reconcile.Result{RequeueAfter: again}, updateStatus(ctx, r.Client, &op, before, &op.Status, err))
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
	refused, err := refusedAccounts(ctx, r.Client, client.ObjectKeyFromObject(op), named.Items)
	if err != nil {
		return 0, err
	}
	st.DeletedAccounts = recordDeleting(st.DeletedAccounts, named.Items, refused)
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

	accounts := slices.DeleteFunc(slices.Clone(named.Items), func(acc authv1beta1.NatsAccount) bool {
		return refused[client.ObjectKeyFromObject(&acc)]
	})
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
	sd, err := recoverRevocations(ctx, r.Distributor, client.ObjectKeyFromObject(op), sys.Status.Revocations, prevJWT, sysPub, sysSigning, users,
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
	}, keys.Keys)
	if err != nil {
		notReady(ReasonInvalidKeys, err.Error())
		return 0, nil
	}
	resign := prev == nil || prev.Name != sys.Name || !sameAccountClaims(prev.JWT, sysJWT)
	if !resign && !unrecovered(st.Conditions) {
		resign = superseded(ctx, r.Distributor, client.ObjectKeyFromObject(op), prev.JWT)
	}
	if resign {
		st.SystemAccount = &authv1beta1.SystemAccountStatus{Name: sys.Name, PublicKey: sysPub, JWT: sysJWT}
	}

	setRetiringCondition(op, retiring, append([]string{st.SystemAccount.JWT}, accountJWTs(accounts)...))
	next, err := r.deletes(ctx, op, keys.Keys, named.Items, refused)
	if err != nil {
		return 0, err
	}
	conditions.Set(&st.Conditions, op.Generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSigned})
	var again time.Duration
	if sd.unasked != nil {
		again = distributionRecheck
	}
	if next.IsZero() {
		return again, nil
	}
	return soonest(again, max(time.Until(next), time.Second)), nil
}

// deletes prunes op's deleted accounts and hands the Distributor the
// request deleting the rest, returning when the next of them expires, the
// zero time for never.
func (r *OperatorReconciler) deletes(ctx context.Context, op *authv1beta1.NatsOperator, keys jwtplane.Keys, accounts []authv1beta1.NatsAccount, refused map[types.NamespacedName]bool) (time.Time, error) {
	live := liveKeys(op, accounts, refused)
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

func keysFailed(err error, notReady func(reason, msg string)) error {
	switch {
	case errors.Is(err, errSeedLost):
		notReady(ReasonSeedLost, err.Error())
		return nil
	case errors.Is(err, errKeysPending):
		notReady(ReasonKeysPending, err.Error())
		return nil
	case errors.Is(err, errInvalidSeed):
		notReady(ReasonInvalidKeys, err.Error())
		return nil
	case errors.Is(err, errSeedNotOwned):
		notReady(ReasonSecretConflict, err.Error())
		return nil
	}
	return err
}

// systemAccount returns the NatsSystemAccount op names and its keys. ok is
// false where the reference cannot be followed yet, with Ready set to say
// why.
func (r *OperatorReconciler) systemAccount(ctx context.Context, op *authv1beta1.NatsOperator, notReady func(reason, msg string)) (*authv1beta1.NatsSystemAccount, resolvedKeys, bool, error) {
	key := op.Spec.SystemAccountRef.ObjectKey(op.Namespace)
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
	if sys.Spec.OperatorRef.ObjectKey(sys.Namespace) != client.ObjectKeyFromObject(op) {
		notReady(ReasonOperatorMismatch, fmt.Sprintf("NatsSystemAccount %s names another NatsOperator", key))
		return nil, resolvedKeys{}, false, nil
	}
	keys, err := resolveKeys(ctx, r.Client, systemAccountKeySource(&sys), false)
	if err != nil {
		return nil, resolvedKeys{}, false, keysFailed(fmt.Errorf("NatsSystemAccount %s: %w", key, err), notReady)
	}
	return &sys, keys, true, nil
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

// SetupWithManager registers r with mgr, which must already hold Setup's indexes.
func (r *OperatorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	return ctrl.NewControllerManagedBy(mgr).
		Named("natsoperator").
		For(&authv1beta1.NatsOperator{}).
		WatchesMetadata(&corev1.Secret{}, refindex.EnqueueByField(c, &authv1beta1.NatsOperatorList{}, seedSecretField)).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			return systemAccountOperators(ctx, c, refindex.Requests(ctx, c, &authv1beta1.NatsSystemAccountList{}, client.MatchingFields{seedSecretField: keyValue(client.ObjectKeyFromObject(obj))}))
		})).
		Watches(&authv1beta1.NatsSystemAccount{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			sys := obj.(*authv1beta1.NatsSystemAccount)
			reqs := refindex.Requests(ctx, c, &authv1beta1.NatsOperatorList{}, client.MatchingFields{systemAccountField: keyValue(client.ObjectKeyFromObject(sys))})
			return append(reqs, reconcile.Request{NamespacedName: sys.Spec.OperatorRef.ObjectKey(sys.Namespace)})
		})).
		Watches(&authv1beta1.NatsAccount{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			acc := obj.(*authv1beta1.NatsAccount)
			return []reconcile.Request{{NamespacedName: acc.Spec.OperatorRef.ObjectKey(acc.Namespace)}}
		}), builder.WithPredicates(accountSignedChange)).
		Watches(&authv1beta1.NatsUser{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			ref := obj.(*authv1beta1.NatsUser).Spec.AccountRef
			if ref.Kind != authv1beta1.AccountKindSystemAccount {
				return nil
			}
			return systemAccountOperators(ctx, c, []reconcile.Request{{NamespacedName: ref.ObjectKey(obj.GetNamespace())}})
		})).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: authGroup, Kind: "NatsOperator"}, &authv1beta1.NatsOperatorList{})).
		Watches(&natsv1beta1.NatsReferenceGrant{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			return grantOperators(ctx, c, obj)
		})).
		Complete(telemetry.Traced("NatsOperator", r))
}

// accountSignedChange passes a NatsAccount update only when its generation,
// its deletion, or its public key or JWT in status changed.
var accountSignedChange = predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
	o, okOld := e.ObjectOld.(*authv1beta1.NatsAccount)
	n, okNew := e.ObjectNew.(*authv1beta1.NatsAccount)
	if !okOld || !okNew {
		return true
	}
	return o.Generation != n.Generation ||
		!o.DeletionTimestamp.Equal(n.DeletionTimestamp) ||
		o.Status.PublicKey != n.Status.PublicKey ||
		o.Status.JWT != n.Status.JWT
}}

// systemAccountOperators maps requests for NatsSystemAccounts to requests
// for the NatsOperators they name.
func systemAccountOperators(ctx context.Context, c client.Reader, reqs []reconcile.Request) []reconcile.Request {
	var out []reconcile.Request
	for _, req := range reqs {
		var sys authv1beta1.NatsSystemAccount
		if err := c.Get(ctx, req.NamespacedName, &sys); err != nil {
			continue
		}
		out = append(out, reconcile.Request{NamespacedName: sys.Spec.OperatorRef.ObjectKey(sys.Namespace)})
	}
	return out
}

// grantOperators maps a NatsReferenceGrant that admits NatsAccounts to
// NatsOperators to requests for every NatsOperator in its namespace.
func grantOperators(ctx context.Context, c client.Reader, obj client.Object) []reconcile.Request {
	g, ok := obj.(*natsv1beta1.NatsReferenceGrant)
	if !ok {
		return nil
	}
	fromAccounts := slices.ContainsFunc(g.Spec.From, func(f natsv1beta1.ReferenceGrantFrom) bool {
		return f.Group == authGroup && f.Kind == "NatsAccount"
	})
	toOperators := slices.ContainsFunc(g.Spec.To, func(t natsv1beta1.ReferenceGrantTo) bool {
		return t.Group == authGroup && t.Kind == "NatsOperator"
	})
	if !fromAccounts || !toOperators {
		return nil
	}
	return refindex.Requests(ctx, c, &authv1beta1.NatsOperatorList{}, client.InNamespace(g.Namespace))
}
