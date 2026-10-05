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
	"k8s.io/apimachinery/pkg/api/meta"
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
const AccountFinalizer = "auth.nats-operator.io/delete"

// +kubebuilder:rbac:groups=auth.nats-operator.io,resources=natsaccounts,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=auth.nats-operator.io,resources=natsaccounts/status,verbs=update
// +kubebuilder:rbac:groups=auth.nats-operator.io,resources=natsoperators,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats-operator.io,resources=natssystemaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups=auth.nats-operator.io,resources=natsusers,verbs=list;watch
// +kubebuilder:rbac:groups=nats-operator.io,resources=natsreferencegrants,verbs=list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create

// Reconcile implements reconcile.Reconciler.
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
	opKey := acc.Spec.OperatorRef.ObjectKey(acc.Namespace)
	refused, err := admitAccount(ctx, r.Client, acc.Namespace, opKey)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !referenceAdmitted(&st.Conditions, acc.Generation, refused, notReady) {
		return reconcile.Result{}, r.withdraw(ctx, acc)
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
	if len(imports.pending) > 0 {
		msg := strings.Join(imports.pending, "; ")
		conditions.Set(&st.Conditions, acc.Generation, metav1.Condition{Type: grant.ConditionReferencesResolved, Status: metav1.ConditionFalse, Reason: ReasonExporterPending, Message: msg})
		notReady(ReasonExporterPending, msg)
		return reconcile.Result{}, nil
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
		unrecovered(st.Conditions) || takeoverRefused(st.Conditions), everDistributed(st.Distribution))
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
	if sd.held != "" {
		refused, err := refuseTakeover(sd.held, token, acc.Spec.Takeover, notReady)
		if err != nil {
			return reconcile.Result{}, err
		}
		if refused {
			st.JWT, st.JWTHash, st.Distribution = "", "", nil
			meta.RemoveStatusCondition(&st.Conditions, ConditionDistributed)
			return reconcile.Result{}, nil
		}
	}
	held := sd.unasked != nil
	adopt := func() error {
		var err error
		if !held {
			err = push(ctx, r.Distributor, opKey, token)
			if err := ignoreUndelivered(err); err != nil {
				return fmt.Errorf("push account JWT: %w", err)
			}
		}
		sent := !held && r.Distributor != nil && err == nil
		st.JWT = token
		st.JWTHash = JWTHash(token)
		st.Distribution = pushed(st.Distribution, now, sent)
		if sent {
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
	if held {
		recordDistribution(&st.Conditions, acc.Generation, pushHeld(sd.unasked))
		res := requeueAtRenewal(st.JWT, now)
		res.RequeueAfter = soonest(res.RequeueAfter, distributionRecheck)
		return res, nil
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

// withdraw drops the JWT of acc, which is not admitted to its NatsOperator,
// from status once that NatsOperator records its deletion.
func (r *AccountReconciler) withdraw(ctx context.Context, acc *authv1beta1.NatsAccount) error {
	st := &acc.Status
	if st.PublicKey == "" || st.JWT == "" {
		return nil
	}
	held, err := r.deletionPending(ctx, acc)
	if err != nil || held {
		return err
	}
	st.JWT, st.JWTHash, st.Distribution = "", "", nil
	meta.RemoveStatusCondition(&st.Conditions, ConditionDistributed)
	return nil
}

// deletionPending reports whether acc's NatsOperator exists and its status
// does not yet record acc's deletion.
func (r *AccountReconciler) deletionPending(ctx context.Context, acc *authv1beta1.NatsAccount) (bool, error) {
	d, err := deletedAccount(acc.Status.PublicKey, acc.Status.JWT)
	if err != nil {
		return false, err
	}
	key := acc.Spec.OperatorRef.ObjectKey(acc.Namespace)
	var op authv1beta1.NatsOperator
	if err := r.Get(ctx, key, &op); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	var list authv1beta1.NatsAccountList
	if err := r.List(ctx, &list, client.MatchingFields{operatorField: keyValue(key)}); err != nil {
		return false, fmt.Errorf("list NatsAccounts: %w", err)
	}
	refused, err := refusedAccounts(ctx, r.Client, key, list.Items)
	if err != nil {
		return false, err
	}
	return !deletionRecorded(&op, list.Items, refused, d, time.Now()), nil
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
	js := l.JetStream
	if js == nil {
		return out
	}
	global := jwtplane.JetStreamLimits{
		MemoryStorage:        quantity(js.MemoryStorage),
		DiskStorage:          quantity(js.DiskStorage),
		Streams:              deref(js.Streams),
		Consumers:            deref(js.Consumers),
		MaxAckPending:        deref(js.MaxAckPending),
		MemoryMaxStreamBytes: quantity(js.MemoryMaxStreamBytes),
		DiskMaxStreamBytes:   quantity(js.DiskMaxStreamBytes),
		MaxBytesRequired:     js.MaxBytesRequired,
	}
	if len(js.Tiers) == 0 || global != (jwtplane.JetStreamLimits{}) {
		out.JetStream = &global
	}
	for _, t := range js.Tiers {
		if out.JetStreamTiers == nil {
			out.JetStreamTiers = map[string]jwtplane.JetStreamLimits{}
		}
		out.JetStreamTiers[string(t.Name)] = jwtplane.JetStreamLimits{
			MemoryStorage:        quantity(t.MemoryStorage),
			DiskStorage:          quantity(t.DiskStorage),
			Streams:              deref(t.Streams),
			Consumers:            deref(t.Consumers),
			MaxAckPending:        deref(t.MaxAckPending),
			MemoryMaxStreamBytes: quantity(t.MemoryMaxStreamBytes),
			DiskMaxStreamBytes:   quantity(t.DiskMaxStreamBytes),
			MaxBytesRequired:     t.MaxBytesRequired,
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
	// pending names, per import whose exporter has no public key yet, that
	// exporter; an account with one is not signed.
	pending []string
	// notPermitted is set when a missing NatsReferenceGrant left one out.
	notPermitted bool
}

// resolveImports resolves acc's imports; pub is acc's public key. An error
// is a failure to read, never an import that does not resolve.
func (r *AccountReconciler) resolveImports(ctx context.Context, acc *authv1beta1.NatsAccount, pub string) (resolvedImports, error) {
	var out resolvedImports
	for i := range acc.Spec.Imports {
		imp := &acc.Spec.Imports[i]
		var res importResolution
		var err error
		switch {
		case imp.AccountRef == nil:
			res, err = r.resolveKeyImport(ctx, acc, imp, pub)
		case imp.AccountRef.Kind == authv1beta1.AccountKindSystemAccount:
			res, err = r.resolveSystemImport(ctx, acc, imp, pub)
		default:
			res, err = r.resolveAccountImport(ctx, acc, imp, pub)
		}
		if err != nil {
			return out, err
		}
		if res.reason == "" {
			if err := importFlagsFit(imp, res.imp.Export.Type); err != nil {
				res.reason = err.Error()
			}
		}
		switch {
		case res.pending:
			out.pending = append(out.pending, res.label+": "+res.reason)
		case res.reason != "":
			out.unresolved = append(out.unresolved, res.label+": "+res.reason)
			out.notPermitted = out.notPermitted || res.notPermitted
		default:
			res.imp.LocalSubject, res.imp.Share, res.imp.AllowTrace = imp.LocalSubject, imp.Share, imp.AllowTrace
			res.status.Export, res.status.LocalSubject = res.label, imp.LocalSubject
			res.status.Subject, res.status.Type = res.imp.Export.Subject, exportTypeOf(res.imp.Export.Type)
			out.imports = append(out.imports, res.imp)
			out.statuses = append(out.statuses, res.status)
		}
	}
	return out, nil
}

// importResolution is one import resolved, or the reason it is not. A
// pending one waits for its exporter; any other with a reason is left out.
type importResolution struct {
	label        string
	imp          jwtplane.Import
	status       authv1beta1.ImportStatus
	reason       string
	pending      bool
	notPermitted bool
}

// importFlagsFit returns why imp's share or allowTrace does not fit an
// import of type t, or nil.
func importFlagsFit(imp *authv1beta1.Import, t jwt.ExportType) error {
	if imp.Share && t != jwt.Service {
		return errors.New("share is set on a Stream import")
	}
	if imp.AllowTrace && t != jwt.Stream {
		return errors.New("allowTrace is set on a Service import")
	}
	return nil
}

// admitImport asks whether acc may reference the exporter of imp, filling
// res with the refusal where it may not.
func (r *AccountReconciler) admitImport(ctx context.Context, acc *authv1beta1.NatsAccount, imp *authv1beta1.Import, exKey types.NamespacedName, res *importResolution) error {
	cond, err := admit(ctx, r.Client, authGroup, "NatsAccount", acc, string(imp.AccountRef.Kind), exKey)
	if err != nil {
		return err
	}
	if cond != nil {
		res.reason, res.notPermitted = cond.Message, true
	}
	return nil
}

// resolveAccountImport resolves an import from a NatsAccount under acc's
// NatsOperator, minting the activation token of a private export.
func (r *AccountReconciler) resolveAccountImport(ctx context.Context, acc *authv1beta1.NatsAccount, imp *authv1beta1.Import, pub string) (importResolution, error) {
	exKey := imp.AccountRef.ObjectKey(acc.Namespace)
	res := importResolution{label: importLabel(acc.Namespace, exKey, imp.Export)}
	if err := r.admitImport(ctx, acc, imp, exKey, &res); err != nil || res.reason != "" {
		return res, err
	}
	var exporter authv1beta1.NatsAccount
	if err := r.Get(ctx, exKey, &exporter); err != nil {
		if apierrors.IsNotFound(err) {
			res.reason = fmt.Sprintf("NatsAccount %s does not exist", exKey)
			return res, nil
		}
		return res, err
	}
	if exOp, op := exporter.Spec.OperatorRef.ObjectKey(exporter.Namespace), acc.Spec.OperatorRef.ObjectKey(acc.Namespace); exOp != op {
		res.reason = fmt.Sprintf("NatsAccount %s is signed by NatsOperator %s, not %s", exKey, exOp, op)
		return res, nil
	}
	if exporter.Status.PublicKey == "" {
		res.reason, res.pending = fmt.Sprintf("NatsAccount %s has no public key yet", exKey), true
		return res, nil
	}
	exports, err := accountExports(&exporter)
	if err != nil {
		res.reason = err.Error()
		return res, nil
	}
	e, found := findExport(exports, imp.Export)
	if !found {
		res.reason = fmt.Sprintf("NatsAccount %s has no export %q", exKey, imp.Export)
		return res, nil
	}
	res.imp = jwtplane.Import{Account: exporter.Status.PublicKey, Export: e.Export}
	if !e.Private {
		return res, nil
	}
	if !listsImporter(e.importers, exporter.Namespace, acc) {
		res.reason = "the export does not list this account among its importers"
		return res, nil
	}
	token, err := r.activation(ctx, &exporter, e.Export, pub)
	if err != nil {
		if errors.Is(err, errKeysPending) || errors.Is(err, errInvalidSeed) {
			res.reason = err.Error()
			return res, nil
		}
		return res, err
	}
	res.imp.Token, res.status.Activation = token, authv1beta1.ActivationSigned
	return res, nil
}

// resolveSystemImport resolves an import of one of the monitoring exports
// of the NatsSystemAccount under acc's NatsOperator.
func (r *AccountReconciler) resolveSystemImport(ctx context.Context, acc *authv1beta1.NatsAccount, imp *authv1beta1.Import, pub string) (importResolution, error) {
	exKey := imp.AccountRef.ObjectKey(acc.Namespace)
	res := importResolution{label: importLabel(acc.Namespace, exKey, imp.Export)}
	if err := r.admitImport(ctx, acc, imp, exKey, &res); err != nil || res.reason != "" {
		return res, err
	}
	var sys authv1beta1.NatsSystemAccount
	if err := r.Get(ctx, exKey, &sys); err != nil {
		if apierrors.IsNotFound(err) {
			res.reason = fmt.Sprintf("NatsSystemAccount %s does not exist", exKey)
			return res, nil
		}
		return res, err
	}
	opKey := acc.Spec.OperatorRef.ObjectKey(acc.Namespace)
	if exOp := sys.Spec.OperatorRef.ObjectKey(sys.Namespace); exOp != opKey {
		res.reason = fmt.Sprintf("NatsSystemAccount %s is signed by NatsOperator %s, not %s", exKey, exOp, opKey)
		return res, nil
	}
	var op authv1beta1.NatsOperator
	if err := r.Get(ctx, opKey, &op); err != nil {
		if apierrors.IsNotFound(err) {
			res.reason = fmt.Sprintf("NatsOperator %s does not exist", opKey)
			return res, nil
		}
		return res, err
	}
	if op.Spec.SystemAccountRef.ObjectKey(op.Namespace) != exKey {
		res.reason = fmt.Sprintf("NatsSystemAccount %s is not the system account of NatsOperator %s", exKey, opKey)
		return res, nil
	}
	if sys.Status.PublicKey == "" {
		res.reason, res.pending = fmt.Sprintf("NatsSystemAccount %s has no public key yet", exKey), true
		return res, nil
	}
	ji, ok := jwtplane.MonitoringImport(imp.Export, sys.Status.PublicKey, pub)
	if !ok {
		res.reason = fmt.Sprintf("NatsSystemAccount %s has no export %q", exKey, imp.Export)
		return res, nil
	}
	res.imp = ji
	return res, nil
}

// resolveKeyImport resolves an import from the account imp names by public
// key, reading the activation token of a private export from the Secret
// imp names in acc's namespace and checking it fits the import.
func (r *AccountReconciler) resolveKeyImport(ctx context.Context, acc *authv1beta1.NatsAccount, imp *authv1beta1.Import, pub string) (importResolution, error) {
	res := importResolution{label: imp.PublicKey + "/" + imp.Export}
	res.imp = jwtplane.Import{
		Account: imp.PublicKey,
		Export:  jwtplane.Export{Name: imp.Export, Type: exportType(imp.Type), Subject: imp.Subject, Private: imp.Activation != nil},
	}
	if imp.Activation != nil {
		ref := imp.Activation.SecretKeyRef
		var secret corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: acc.Namespace, Name: ref.Name}, &secret); err != nil {
			if apierrors.IsNotFound(err) {
				res.reason = fmt.Sprintf("Secret %s/%s does not exist", acc.Namespace, ref.Name)
				return res, nil
			}
			return res, err
		}
		token, ok := secret.Data[ref.Key]
		if !ok {
			res.reason = fmt.Sprintf("Secret %s/%s has no key %q", acc.Namespace, ref.Name, ref.Key)
			return res, nil
		}
		res.imp.Token = strings.TrimSpace(string(token))
		res.status.Activation = authv1beta1.ActivationSupplied
	}
	if err := jwtplane.ValidateImport(res.imp, pub); err != nil {
		res.reason = err.Error()
	}
	return res, nil
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

// activationSecretNames are the Secrets acc's imports read activation
// tokens from.
func activationSecretNames(acc *authv1beta1.NatsAccount) []string {
	var out []string
	for _, imp := range acc.Spec.Imports {
		if imp.Activation != nil {
			out = append(out, imp.Activation.SecretKeyRef.Name)
		}
	}
	return out
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
		if ref.Kind == authv1beta1.AccountKindAccount && ref.ObjectKey(namespace) == client.ObjectKeyFromObject(acc) {
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
		Watches(&authv1beta1.NatsSystemAccount{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			return systemAccountAccounts(ctx, c, obj)
		})).
		Watches(&authv1beta1.NatsAccount{}, refindex.EnqueueByField(c, &authv1beta1.NatsAccountList{}, exporterField)).
		Watches(&authv1beta1.NatsAccount{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			return sameKeyAccounts(ctx, c, obj)
		})).
		Watches(&authv1beta1.NatsUser{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			ref := obj.(*authv1beta1.NatsUser).Spec.AccountRef
			if ref.Kind != authv1beta1.AccountKindAccount {
				return nil
			}
			return []reconcile.Request{{NamespacedName: ref.ObjectKey(obj.GetNamespace())}}
		})).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: authGroup, Kind: "NatsAccount"}, &authv1beta1.NatsAccountList{})).
		Complete(telemetry.Traced("NatsAccount", r))
}
