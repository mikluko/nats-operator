package authctl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/refindex"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// AccountReconciler signs a NatsAccount's JWT with its NatsOperator's active
// signing key and writes it to status, generating the keys spec omits.
// A jwtTTL of zero signs a JWT that never expires.
type AccountReconciler struct {
	client.Client
	// Distributor receives every newly signed account JWT; nil pushes
	// nothing.
	Distributor Distributor
	// Recorder records JWTs pushed and held; nil records none.
	Recorder events.EventRecorder
	// RosterChanges receives a NatsOperator whose servers changed; the
	// accounts it signs are reconciled. Nil receives nothing.
	RosterChanges <-chan event.GenericEvent
}

var _ reconcile.Reconciler = (*AccountReconciler)(nil)

// AccountFinalizer holds a deleted NatsAccount until its NatsOperator
// records the deletion.
const AccountFinalizer = "auth.nats.mikluko.io/delete"

// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsaccounts,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsaccounts/status,verbs=update
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsoperators,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsusers,verbs=list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsreferencegrants,verbs=list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create

// Reconcile signs and distributes the JWT of the NatsAccount req names.
func (r *AccountReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var acc authv1beta1.NatsAccount
	if err := r.Get(ctx, req.NamespacedName, &acc); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if acc.DeletionTimestamp != nil {
		return reconcile.Result{}, r.finalize(ctx, &acc)
	}
	if err := patchFinalizer(ctx, r.Client, &acc, AccountFinalizer, true); err != nil {
		return reconcile.Result{}, err
	}
	before := acc.Status.DeepCopy()
	res, err := r.reconcile(ctx, &acc)
	observe(&acc.Status.Conditions, &acc.Status.ObservedGeneration, acc.Generation, err)
	return result(res, updateStatus(ctx, r.Client, &acc, before, &acc.Status, err))
}

func (r *AccountReconciler) reconcile(ctx context.Context, acc *authv1beta1.NatsAccount) (reconcile.Result, error) {
	st := &acc.Status
	notReady := func(reason, msg string) {
		conditions.Set(&st.Conditions, acc.Generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: msg})
	}
	keys, err := resolveKeys(ctx, r.Client, accountKeySource(acc), true)
	if err != nil {
		return reconcile.Result{}, keysFailed(err, notReady)
	}
	pub, err := keys.identityPublicKey()
	if err != nil {
		return reconcile.Result{}, err
	}
	opKey := refKey(acc.Spec.OperatorRef, acc.Namespace)
	refused, err := admit(ctx, r.Client, authGroup, "NatsAccount", acc, "NatsOperator", opKey)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !referenceAdmitted(&st.Conditions, acc.Generation, refused, notReady) {
		return reconcile.Result{}, nil
	}
	holder, err := accountKeyHolder(ctx, r.Client, acc, opKey, pub)
	if err != nil {
		return reconcile.Result{}, err
	}
	if holder != "" {
		if st.PublicKey == pub {
			st.PublicKey, st.JWT, st.JWTHash = "", "", ""
		}
		notReady(ReasonPublicKeyInUse, fmt.Sprintf("public key %s is held by %s under NatsOperator %s", pub, holder, opKey))
		return reconcile.Result{}, nil
	}
	st.PublicKey = pub

	opKeys, ok, err := r.operatorKeys(ctx, opKey, notReady)
	if !ok || err != nil {
		return reconcile.Result{}, err
	}

	exports, err := accountExports(acc)
	if err != nil {
		notReady(ReasonInvalidJWT, err.Error())
		return reconcile.Result{}, nil
	}
	imports, err := r.resolveImports(ctx, acc, pub)
	if err != nil {
		return reconcile.Result{}, err
	}
	st.Imports = imports.statuses

	users, err := listUsers(ctx, r.Client, authv1beta1.AccountKindAccount, client.ObjectKeyFromObject(acc))
	if err != nil {
		return reconcile.Result{}, err
	}
	signing, _, err := keys.signingPublicKeys()
	if err != nil {
		return reconcile.Result{}, err
	}
	sd, err := recoverRevocations(ctx, r.Distributor, opKey, st.Revocations, st.JWT, pub, signing, users,
		unrecovered(st.Conditions), everDistributed(st.Distribution))
	if err != nil {
		recordHeld(r.Recorder, acc, st.Conditions, err)
		again, err := recoveryFailed(err, notReady)
		return result(reconcile.Result{RequeueAfter: again}, err)
	}
	recordRecovery(&st.Conditions, acc.Generation, sd)
	st.Revocations = sd.revocations
	a := jwtplane.Account{
		Name:        acc.Name,
		Keys:        keys.Keys,
		Limits:      accountLimits(acc.Spec.Limits),
		Imports:     imports.imports,
		Revocations: signedRevocations(st.Revocations),
	}
	for _, e := range exports {
		a.Exports = append(a.Exports, e.Export)
	}
	if ttl := acc.Spec.JWTTTL; ttl != nil {
		a.TTL = ttl.Duration
		a.NoExpiry = ttl.Duration == 0
	}
	now := time.Now()
	token, err := jwtplane.SignAccount(a, opKeys.Keys, now)
	if err != nil {
		notReady(ReasonInvalidJWT, err.Error())
		return reconcile.Result{}, nil
	}
	adopt := func() error {
		err := push(ctx, r.Distributor, opKey, token)
		if err := ignoreUnreachable(err); err != nil {
			return fmt.Errorf("push account JWT: %w", err)
		}
		st.JWT = token
		st.JWTHash = JWTHash(token)
		st.Distribution = pushed(st.Distribution, now, r.Distributor != nil && err == nil)
		if r.Distributor != nil && err == nil {
			telemetry.Emit(r.Recorder, acc, telemetry.JWTPushed, "account JWT of %s pushed", pub)
		}
		return nil
	}
	if !sameAccountClaims(st.JWT, token) || due(st.JWT, now) || (!a.NoExpiry && lifetimeDiffers(st.JWT, accountTTL(a.TTL))) {
		if err := adopt(); err != nil {
			return reconcile.Result{}, err
		}
	}

	if len(imports.unresolved) > 0 {
		reason, readyReason := ReasonImportsUnresolved, ReasonImportsUnresolved
		if imports.notPermitted {
			reason, readyReason = grant.ReasonNoGrant, grant.ReasonReferenceNotPermitted
		}
		msg := strings.Join(imports.unresolved, "; ")
		conditions.Set(&st.Conditions, acc.Generation, metav1.Condition{Type: grant.ConditionReferencesResolved, Status: metav1.ConditionFalse, Reason: reason, Message: msg})
		notReady(readyReason, msg)
	} else {
		conditions.Set(&st.Conditions, acc.Generation, metav1.Condition{Type: grant.ConditionReferencesResolved, Status: metav1.ConditionTrue, Reason: ReasonAllImportsResolved})
		conditions.Set(&st.Conditions, acc.Generation, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSigned})
	}
	dist, cond, again, err := distribute(ctx, r.Distributor, opKey, st.JWT, st.Distribution)
	if errors.Is(err, ErrStaleJWT) && st.JWT != token {
		if err := adopt(); err != nil {
			return reconcile.Result{}, err
		}
		dist, cond, again, err = distribute(ctx, r.Distributor, opKey, st.JWT, st.Distribution)
	}
	st.Distribution = dist
	recordDistribution(&st.Conditions, acc.Generation, cond)
	res := requeueAtRenewal(st.JWT, now)
	res.RequeueAfter = soonest(res.RequeueAfter, again)
	return result(res, err)
}

// finalize removes AccountFinalizer once acc's NatsOperator records its
// deletion, where it has a JWT to delete.
func (r *AccountReconciler) finalize(ctx context.Context, acc *authv1beta1.NatsAccount) error {
	if !controllerutil.ContainsFinalizer(acc, AccountFinalizer) {
		return nil
	}
	if acc.Status.PublicKey != "" && acc.Status.JWT != "" {
		held, err := r.deletionPending(ctx, acc)
		if err != nil || held {
			return err
		}
	}
	return patchFinalizer(ctx, r.Client, acc, AccountFinalizer, false)
}

// deletionPending reports whether acc's NatsOperator exists and its status
// does not yet record acc's deletion.
func (r *AccountReconciler) deletionPending(ctx context.Context, acc *authv1beta1.NatsAccount) (bool, error) {
	d, err := deletedAccount(acc.Status.PublicKey, acc.Status.JWT)
	if err != nil {
		return false, err
	}
	key := refKey(acc.Spec.OperatorRef, acc.Namespace)
	var op authv1beta1.NatsOperator
	if err := r.Get(ctx, key, &op); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	var list authv1beta1.NatsAccountList
	if err := r.List(ctx, &list, client.MatchingFields{operatorField: keyValue(key)}); err != nil {
		return false, fmt.Errorf("list NatsAccounts: %w", err)
	}
	return !deletionRecorded(&op, list.Items, d, time.Now()), nil
}

func listUsers(ctx context.Context, c client.Reader, kind authv1beta1.AccountKind, key types.NamespacedName) ([]authv1beta1.NatsUser, error) {
	var list authv1beta1.NatsUserList
	if err := c.List(ctx, &list, client.MatchingFields{userAccountField: accountValue(kind, key)}); err != nil {
		return nil, fmt.Errorf("list NatsUsers: %w", err)
	}
	return list.Items, nil
}

func pushed(d *authv1beta1.Distribution, now time.Time, sent bool) *authv1beta1.Distribution {
	out := &authv1beta1.Distribution{}
	if sent {
		out.LastPushTime = &metav1.Time{Time: now}
	}
	if d != nil {
		out.Servers = d.Servers
	}
	return out
}

func accountTTL(ttl time.Duration) time.Duration {
	if ttl == 0 {
		return jwtplane.DefaultAccountTTL
	}
	return ttl
}

// due reports whether token has reached half its lifetime at now.
func due(token string, now time.Time) bool {
	at, err := jwtplane.RenewAt(token)
	return err == nil && !at.IsZero() && !now.Before(at)
}

func requeueAtRenewal(token string, now time.Time) reconcile.Result {
	at, err := jwtplane.RenewAt(token)
	if err != nil || at.IsZero() {
		return reconcile.Result{}
	}
	return reconcile.Result{RequeueAfter: max(at.Sub(now), time.Second)}
}

// operatorKeys returns the keys of the NatsOperator at key. ok is false
// where they cannot be read yet, with Ready set to say why.
func (r *AccountReconciler) operatorKeys(ctx context.Context, key types.NamespacedName, notReady func(reason, msg string)) (resolvedKeys, bool, error) {
	var op authv1beta1.NatsOperator
	if err := r.Get(ctx, key, &op); err != nil {
		if apierrors.IsNotFound(err) {
			notReady(ReasonNotFound, fmt.Sprintf("NatsOperator %s does not exist", key))
			return resolvedKeys{}, false, nil
		}
		return resolvedKeys{}, false, err
	}
	src, err := operatorKeySource(&op)
	if err != nil {
		notReady(ReasonInvalidJWT, fmt.Sprintf("NatsOperator %s: %v", key, err))
		return resolvedKeys{}, false, nil
	}
	keys, err := resolveKeys(ctx, r.Client, src, false)
	if err != nil {
		return resolvedKeys{}, false, keysFailed(fmt.Errorf("NatsOperator %s: %w", key, err), notReady)
	}
	return keys, true, nil
}

// specExport is one export as signed, with the importers spec lists for it.
type specExport struct {
	jwtplane.Export
	importers []authv1beta1.AccountReference
}

// accountExports expands acc's exports, presets included, in spec order.
func accountExports(acc *authv1beta1.NatsAccount) ([]specExport, error) {
	var out []specExport
	for _, e := range acc.Spec.Exports {
		if e.Preset != "" {
			expanded, err := jwtplane.ExportPreset(string(e.Preset))
			if err != nil {
				return nil, err
			}
			for _, x := range expanded {
				out = append(out, specExport{Export: x})
			}
			continue
		}
		x := jwtplane.Export{
			Name:         e.Name,
			Type:         exportType(e.Type),
			Subject:      e.Subject,
			ResponseType: jwt.ResponseType(e.ResponseType),
			Private:      e.Access == authv1beta1.ExportAccessPrivate,
		}
		out = append(out, specExport{Export: x, importers: e.Importers})
	}
	return out, nil
}

func exportType(t authv1beta1.ExportType) jwt.ExportType {
	if t == authv1beta1.ExportTypeService {
		return jwt.Service
	}
	return jwt.Stream
}

// exportTypeOf is the API's name for a jwt export type.
func exportTypeOf(t jwt.ExportType) authv1beta1.ExportType {
	if t == jwt.Service {
		return authv1beta1.ExportTypeService
	}
	return authv1beta1.ExportTypeStream
}

func accountLimits(l *authv1beta1.AccountLimits) jwtplane.Limits {
	if l == nil {
		return jwtplane.Limits{}
	}
	out := jwtplane.Limits{
		Connections:   deref(l.Connections),
		Subscriptions: deref(l.Subscriptions),
		Payload:       quantity(l.Payload),
	}
	if js := l.JetStream; js != nil {
		out.JetStream = &jwtplane.JetStreamLimits{
			MemoryStorage: quantity(js.MemoryStorage),
			DiskStorage:   quantity(js.DiskStorage),
			Streams:       deref(js.Streams),
			Consumers:     deref(js.Consumers),
		}
	}
	return out
}

func deref(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func quantity(q *resource.Quantity) int64 {
	if q == nil {
		return 0
	}
	return q.Value()
}

// resolvedImports are an account's imports as far as they resolve.
type resolvedImports struct {
	imports  []jwtplane.Import
	statuses []authv1beta1.ImportStatus
	// unresolved says, per import left out, why.
	unresolved []string
	// notPermitted is set when a missing NatsReferenceGrant left one out.
	notPermitted bool
}

// resolveImports resolves acc's imports; pub is acc's public key. An error
// is a failure to read, never an import that does not resolve.
func (r *AccountReconciler) resolveImports(ctx context.Context, acc *authv1beta1.NatsAccount, pub string) (resolvedImports, error) {
	var out resolvedImports
	for _, imp := range acc.Spec.Imports {
		exKey := refKey(imp.AccountRef.ObjectReference, acc.Namespace)
		label := importLabel(acc.Namespace, exKey, imp.Export)
		skip := func(format string, args ...any) {
			out.unresolved = append(out.unresolved, label+": "+fmt.Sprintf(format, args...))
		}
		cond, err := admit(ctx, r.Client, authGroup, "NatsAccount", acc, string(imp.AccountRef.Kind), exKey)
		if err != nil {
			return out, err
		}
		if cond != nil {
			out.notPermitted = true
			skip("%s", cond.Message)
			continue
		}
		if imp.AccountRef.Kind != authv1beta1.AccountKindAccount {
			skip("a %s has no exports", imp.AccountRef.Kind)
			continue
		}
		var exporter authv1beta1.NatsAccount
		if err := r.Get(ctx, exKey, &exporter); err != nil {
			if apierrors.IsNotFound(err) {
				skip("NatsAccount %s does not exist", exKey)
				continue
			}
			return out, err
		}
		if exOp, op := refKey(exporter.Spec.OperatorRef, exporter.Namespace), refKey(acc.Spec.OperatorRef, acc.Namespace); exOp != op {
			skip("NatsAccount %s is signed by NatsOperator %s, not %s", exKey, exOp, op)
			continue
		}
		if exporter.Status.PublicKey == "" {
			skip("NatsAccount %s has no public key yet", exKey)
			continue
		}
		exports, err := accountExports(&exporter)
		if err != nil {
			skip("%v", err)
			continue
		}
		e, found := findExport(exports, imp.Export)
		if !found {
			skip("NatsAccount %s has no export %q", exKey, imp.Export)
			continue
		}
		ji := jwtplane.Import{Account: exporter.Status.PublicKey, Export: e.Export, LocalSubject: imp.LocalSubject}
		status := authv1beta1.ImportStatus{
			Export:       label,
			Subject:      e.Subject,
			LocalSubject: imp.LocalSubject,
			Type:         exportTypeOf(e.Type),
		}
		if e.Private {
			if !listsImporter(e.importers, exporter.Namespace, acc) {
				skip("the export does not list this account among its importers")
				continue
			}
			token, err := r.activation(ctx, &exporter, e.Export, pub)
			if err != nil {
				if errors.Is(err, errKeysPending) || errors.Is(err, errInvalidSeed) {
					skip("%v", err)
					continue
				}
				return out, err
			}
			ji.Token = token
			status.Activation = authv1beta1.ActivationSigned
		}
		out.imports = append(out.imports, ji)
		out.statuses = append(out.statuses, status)
	}
	return out, nil
}

// activation mints the token admitting importer to exporter's private
// export e, signed by exporter's active signing key.
func (r *AccountReconciler) activation(ctx context.Context, exporter *authv1beta1.NatsAccount, e jwtplane.Export, importer string) (string, error) {
	keys, err := resolveKeys(ctx, r.Client, accountKeySource(exporter), false)
	if err != nil {
		return "", fmt.Errorf("NatsAccount %s: %w", client.ObjectKeyFromObject(exporter), err)
	}
	e.Importers = []string{importer}
	return jwtplane.SignActivation(keys.Keys, e, importer)
}

// importLabel is an ImportStatus's export: account/export, prefixed by the
// exporter's namespace where it is not the importer's.
func importLabel(namespace string, exporter types.NamespacedName, export string) string {
	if exporter.Namespace == namespace {
		return exporter.Name + "/" + export
	}
	return exporter.Namespace + "/" + exporter.Name + "/" + export
}

func findExport(exports []specExport, name string) (specExport, bool) {
	for _, e := range exports {
		if e.Name == name {
			return e, true
		}
	}
	return specExport{}, false
}

// listsImporter reports whether importers, listed in namespace, name acc.
func listsImporter(importers []authv1beta1.AccountReference, namespace string, acc *authv1beta1.NatsAccount) bool {
	for _, ref := range importers {
		if ref.Kind == authv1beta1.AccountKindAccount && refKey(ref.ObjectReference, namespace) == client.ObjectKeyFromObject(acc) {
			return true
		}
	}
	return false
}

// SetupWithManager registers r with mgr, which must already hold Setup's indexes.
func (r *AccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	b := ctrl.NewControllerManagedBy(mgr)
	if r.RosterChanges != nil {
		b = b.WatchesRawSource(source.Channel(r.RosterChanges, refindex.EnqueueByField(c, &authv1beta1.NatsAccountList{}, operatorField)))
	}
	return b.
		Named("natsaccount").
		For(&authv1beta1.NatsAccount{}).
		WatchesMetadata(&corev1.Secret{}, refindex.EnqueueByField(c, &authv1beta1.NatsAccountList{}, seedSecretField)).
		Watches(&authv1beta1.NatsOperator{}, refindex.EnqueueByField(c, &authv1beta1.NatsAccountList{}, operatorField)).
		Watches(&authv1beta1.NatsAccount{}, refindex.EnqueueByField(c, &authv1beta1.NatsAccountList{}, exporterField)).
		Watches(&authv1beta1.NatsAccount{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			return sameKeyAccounts(ctx, c, obj)
		})).
		Watches(&authv1beta1.NatsUser{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			ref := obj.(*authv1beta1.NatsUser).Spec.AccountRef
			if ref.Kind != authv1beta1.AccountKindAccount {
				return nil
			}
			return []reconcile.Request{{NamespacedName: refKey(ref.ObjectReference, obj.GetNamespace())}}
		})).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: authGroup, Kind: "NatsAccount"}, &authv1beta1.NatsAccountList{})).
		Complete(telemetry.Traced("NatsAccount", r))
}
