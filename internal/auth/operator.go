package auth

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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// OperatorReconciler signs a NatsOperator's JWT and the JWT of the
// NatsSystemAccount its systemAccountRef names, and writes both to its
// status. It generates the keys spec omits.
//
// The system account JWT imports the jetstream-stepdown exports of every
// NatsAccount the operator signs that carries the preset.
type OperatorReconciler struct {
	client.Client
	// Distributor receives the system account JWT whenever it is newly
	// signed; nil pushes nothing.
	Distributor Distributor
}

// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsoperators,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsoperators/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natssystemaccounts;natsaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsreferencegrants,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create

// Reconcile implements reconcile.Reconciler.
func (r *OperatorReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var op authv1beta1.NatsOperator
	if err := r.Get(ctx, req.NamespacedName, &op); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	before := op.Status.DeepCopy()
	err := r.reconcile(ctx, &op)
	op.Status.ObservedGeneration = op.Generation
	return reconcile.Result{}, updateStatus(ctx, r.Client, &op, before, &op.Status, err)
}

func (r *OperatorReconciler) reconcile(ctx context.Context, op *authv1beta1.NatsOperator) error {
	st := &op.Status
	notReady := func(reason, msg string) {
		setCondition(&st.Conditions, op.Generation, ConditionReady, metav1.ConditionFalse, reason, msg)
	}
	src, err := operatorKeySource(op)
	if err != nil {
		notReady(ReasonInvalidJWT, err.Error())
		return nil
	}
	keys, err := resolveKeys(ctx, r.Client, src, true)
	if err != nil {
		return keysFailed(err, notReady)
	}
	pub, err := keys.identityPublicKey()
	if err != nil {
		return err
	}
	signing, retiring, err := keys.signingPublicKeys()
	if err != nil {
		return err
	}
	st.PublicKey = pub
	st.SigningKeys = signing
	st.SeedSecrets = nil
	if keys.Generated.Identity != "" || len(keys.Generated.Signing) > 0 {
		st.SeedSecrets = keys.Generated.DeepCopy()
	}

	sys, sysKeys, ok, err := r.systemAccount(ctx, op, notReady)
	if !ok || err != nil {
		return err
	}
	sysPub, err := sysKeys.identityPublicKey()
	if err != nil {
		return err
	}

	opJWT, err := jwtplane.SignOperator(jwtplane.Operator{Name: op.Name, Keys: keys.Keys, SystemAccount: sysPub, JWT: op.Spec.JWT})
	if err != nil {
		notReady(ReasonInvalidJWT, err.Error())
		return nil
	}
	if !sameOperatorClaims(st.JWT, opJWT) {
		st.JWT = opJWT
	}

	accounts, err := r.signedAccounts(ctx, op)
	if err != nil {
		return err
	}
	sysJWT, err := jwtplane.SignSystemAccount(jwtplane.SystemAccount{
		Name:             sys.Name,
		Keys:             sysKeys.Keys,
		StepdownAccounts: stepdownAccounts(accounts),
	}, keys.Keys, time.Now())
	if err != nil {
		notReady(ReasonInvalidKeys, err.Error())
		return nil
	}
	prev := st.SystemAccount
	if prev == nil || prev.Name != sys.Name || !sameAccountClaims(prev.JWT, sysJWT) {
		if err := push(ctx, r.Distributor, client.ObjectKeyFromObject(op), sysJWT); err != nil {
			return fmt.Errorf("push system account JWT: %w", err)
		}
		st.SystemAccount = &authv1beta1.SystemAccountStatus{Name: sys.Name, PublicKey: sysPub, JWT: sysJWT}
	}

	setRetiringCondition(op, retiring, append([]string{st.SystemAccount.JWT}, accountJWTs(accounts)...))
	setCondition(&st.Conditions, op.Generation, ConditionReady, metav1.ConditionTrue, ReasonSigned, "")
	return nil
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

// signedAccounts returns the NatsAccounts that name op and are admitted to.
func (r *OperatorReconciler) signedAccounts(ctx context.Context, op *authv1beta1.NatsOperator) ([]authv1beta1.NatsAccount, error) {
	var list authv1beta1.NatsAccountList
	if err := r.List(ctx, &list, client.MatchingFields{operatorField: keyValue(client.ObjectKeyFromObject(op))}); err != nil {
		return nil, fmt.Errorf("list NatsAccounts: %w", err)
	}
	var out []authv1beta1.NatsAccount
	for i := range list.Items {
		acc := &list.Items[i]
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
		setCondition(&op.Status.Conditions, op.Generation, ConditionRetiringKeysInUse, metav1.ConditionFalse, ReasonNoneInUse, "")
		return
	}
	setCondition(&op.Status.Conditions, op.Generation, ConditionRetiringKeysInUse, metav1.ConditionTrue, ReasonInUse,
		fmt.Sprintf("%d account JWTs are signed by a retiring key", n))
}

func setConditionFrom(conds *[]metav1.Condition, c metav1.Condition) {
	setCondition(conds, c.ObservedGeneration, c.Type, c.Status, c.Reason, c.Message)
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
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: authGroup, Kind: "NatsOperator"}, &authv1beta1.NatsOperatorList{})).
		Complete(r)
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
