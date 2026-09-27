package authctl

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// UserFinalizer holds a deleted NatsUser until its key is revoked, the
// revocation has reached every server, its connections are closed and its
// creds Secret is removed.
const UserFinalizer = "auth.nats.mikluko.io/revoke"

// kickInterval is how long a deleted user waits between kick passes that
// still found connections.
const kickInterval = time.Second

// UserReconciler signs a NatsUser's JWT with its account's active signing
// key. With publicKey the JWT goes to status; otherwise a key generated for
// the user is kept in a creds file written to the Secret credentials names,
// or to "<name>-creds" when it names none, owned by the user. A Secret of
// that name the user does not own is never written.
//
// A user whose reference to its account is no longer admitted keeps its
// public key in status, and the account revokes it; admitted again, it is
// re-signed. Deletion is held by UserFinalizer until the account JWT revokes
// the user's key, then, with Sessions set, until the account's distribution
// shows every server current and a kick pass finds no connection; the creds
// Secret goes last.
type UserReconciler struct {
	client.Client
	// Sessions closes a deleted user's connections; nil reaches no NATS
	// server, and deletion then waits only for the revocation to be signed.
	Sessions Sessions
	// Recorder records users kicked; nil records none.
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsusers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsusers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=auth.nats.mikluko.io,resources=natsusers/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;delete

// Reconcile implements reconcile.Reconciler.
func (r *UserReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var u authv1beta1.NatsUser
	if err := r.Get(ctx, req.NamespacedName, &u); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if u.DeletionTimestamp != nil {
		return r.finalize(ctx, &u)
	}
	if controllerutil.AddFinalizer(&u, UserFinalizer) {
		if err := r.Update(ctx, &u); err != nil {
			return reconcile.Result{}, err
		}
	}
	before := u.Status.DeepCopy()
	res, err := r.reconcile(ctx, &u)
	u.Status.ObservedGeneration = u.Generation
	return res, updateStatus(ctx, r.Client, &u, before, &u.Status, err)
}

// reconcile signs u. A JWT signed within the second its key was revoked is
// revoked with it, and is re-signed a second later.
func (r *UserReconciler) reconcile(ctx context.Context, u *authv1beta1.NatsUser) (reconcile.Result, error) {
	st := &u.Status
	notReady := func(reason, msg string) {
		setCondition(&st.Conditions, u.Generation, ConditionReady, metav1.ConditionFalse, reason, msg)
	}
	acc, ok, err := r.account(ctx, u, notReady)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !ok {
		st.JWT = ""
		return reconcile.Result{}, nil
	}
	keys, err := resolveKeys(ctx, r.Client, acc.keys, false)
	if err != nil {
		return reconcile.Result{}, keysFailed(fmt.Errorf("%s %s: %w", u.Spec.AccountRef.Kind, acc.key, err), notReady)
	}
	var token string

	if u.Spec.PublicKey != "" {
		if !nkeys.IsValidPublicUserKey(u.Spec.PublicKey) {
			notReady(ReasonInvalidKeys, fmt.Sprintf("publicKey %q is not a user public key", u.Spec.PublicKey))
			return reconcile.Result{}, nil
		}
		token, err = r.sign(u, u.Spec.PublicKey, keys, acc.jwt, st.JWT)
		if err != nil {
			notReady(ReasonInvalidJWT, err.Error())
			return reconcile.Result{}, nil
		}
		st.PublicKey, st.JWT = u.Spec.PublicKey, token
	} else {
		var pub string
		pub, token, err = r.writeCreds(ctx, u, keys, acc.jwt)
		switch {
		case errors.Is(err, errSecretNotOwned):
			notReady(ReasonSecretConflict, err.Error())
			return reconcile.Result{}, nil
		case errors.Is(err, errInvalidClaims):
			notReady(ReasonInvalidJWT, err.Error())
			return reconcile.Result{}, nil
		case err != nil:
			return reconcile.Result{}, err
		}
		st.PublicKey, st.JWT = pub, ""
	}
	setCondition(&st.Conditions, u.Generation, ConditionReady, metav1.ConditionTrue, ReasonSigned, "")
	if userRevoked(acc.jwt, token) {
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}
	return reconcile.Result{}, nil
}

// errInvalidClaims wraps a user spec jwtplane refuses to sign.
var errInvalidClaims = errors.New("invalid user claims")

// errSecretNotOwned is returned for a creds Secret the user does not own.
var errSecretNotOwned = errors.New("creds Secret not owned by this NatsUser")

// sign returns the JWT of u for pub. current, u's JWT as last signed, is
// returned unchanged while it carries the same claims from the same
// signing key and accountJWT does not revoke it.
func (r *UserReconciler) sign(u *authv1beta1.NatsUser, pub string, keys resolvedKeys, accountJWT, current string) (string, error) {
	token, err := jwtplane.SignUser(userClaims(u, pub), keys.Keys)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errInvalidClaims, err)
	}
	if sameUserClaims(current, token) && !userRevoked(accountJWT, current) {
		return current, nil
	}
	return token, nil
}

// userClaims maps u's spec to the claims jwtplane signs for pub.
func userClaims(u *authv1beta1.NatsUser, pub string) jwtplane.User {
	out := jwtplane.User{
		Name:          u.Name,
		PublicKey:     pub,
		SystemAccount: u.Spec.AccountRef.Kind == authv1beta1.AccountKindSystemAccount,
		Preset:        jwtplane.UserPreset(u.Spec.Preset),
	}
	for _, t := range u.Spec.ConnectionTypes {
		out.AllowedConnectionTypes = append(out.AllowedConnectionTypes, string(t))
	}
	if p := u.Spec.Permissions; p != nil {
		out.Permissions = &jwtplane.Permissions{}
		if p.Publish != nil {
			out.Permissions.Publish = jwtplane.SubjectPermissions{Allow: p.Publish.Allow, Deny: p.Publish.Deny}
		}
		if p.Subscribe != nil {
			out.Permissions.Subscribe = jwtplane.SubjectPermissions{Allow: p.Subscribe.Allow, Deny: p.Subscribe.Deny}
		}
	}
	return out
}

// credsSecret returns the key of u's creds Secret and the key within it.
func credsSecret(u *authv1beta1.NatsUser) (types.NamespacedName, string) {
	name, key := u.Name+"-creds", natsconn.DefaultCredentialsKey
	if c := u.Spec.Credentials; c != nil {
		name = c.SecretKeyRef.Name
		if c.SecretKeyRef.Key != "" {
			key = c.SecretKeyRef.Key
		}
	}
	return types.NamespacedName{Namespace: u.Namespace, Name: name}, key
}

// writeCreds keeps u's creds Secret holding a creds file signed for u and
// returns the user's public key and JWT. The key pair of a creds file already there
// is kept; a Secret the user does not own wraps errSecretNotOwned.
func (r *UserReconciler) writeCreds(ctx context.Context, u *authv1beta1.NatsUser, keys resolvedKeys, accountJWT string) (string, string, error) {
	key, field := credsSecret(u)
	var s corev1.Secret
	err := r.Get(ctx, key, &s)
	exists := err == nil
	if err != nil && !apierrors.IsNotFound(err) {
		return "", "", err
	}
	if exists && !metav1.IsControlledBy(&s, u) {
		return "", "", fmt.Errorf("%w: %s", errSecretNotOwned, key)
	}
	var kp nkeys.KeyPair
	var current string
	if exists {
		kp, current = parseCreds(s.Data[field])
	}
	if kp == nil {
		seed, err := jwtplane.GenerateSeed(nkeys.PrefixByteUser)
		if err != nil {
			return "", "", err
		}
		if kp, err = nkeys.FromSeed(seed); err != nil {
			return "", "", err
		}
		current = ""
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return "", "", err
	}
	token, err := r.sign(u, pub, keys, accountJWT, current)
	if err != nil {
		return "", "", err
	}
	if token == current {
		return pub, token, nil
	}
	seed, err := kp.Seed()
	if err != nil {
		return "", "", err
	}
	creds, err := jwt.FormatUserConfig(token, seed)
	if err != nil {
		return "", "", err
	}
	if !exists {
		s = corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}, Type: corev1.SecretTypeOpaque}
		if err := controllerutil.SetControllerReference(u, &s, r.Scheme()); err != nil {
			return "", "", err
		}
	}
	if s.Data == nil {
		s.Data = map[string][]byte{}
	}
	s.Data[field] = creds
	if exists {
		err = r.Update(ctx, &s)
	} else {
		err = r.Create(ctx, &s)
	}
	if err != nil {
		return "", "", fmt.Errorf("write Secret %s: %w", key, err)
	}
	return pub, token, nil
}

// parseCreds returns the user key pair and JWT of a creds file; kp is nil
// where either does not parse or the key is not a user key.
func parseCreds(creds []byte) (kp nkeys.KeyPair, token string) {
	if len(creds) == 0 {
		return nil, ""
	}
	token, err := jwt.ParseDecoratedJWT(creds)
	if err != nil {
		return nil, ""
	}
	kp, err = jwt.ParseDecoratedNKey(creds)
	if err != nil {
		return nil, ""
	}
	pub, err := kp.PublicKey()
	if err != nil || !nkeys.IsValidPublicUserKey(pub) {
		return nil, ""
	}
	return kp, token
}

// sameUserClaims reports whether two user JWTs carry the same claims,
// signed by the same key, whenever they were signed.
func sameUserClaims(a, b string) bool {
	return sameClaims(a, b, func(token string) (any, error) {
		c, err := jwt.DecodeUserClaims(token)
		if err != nil {
			return nil, err
		}
		normalizeClaimsData(&c.ClaimsData)
		return c, nil
	})
}

// userAccount is the account a NatsUser belongs to, as far as signing and
// revoking its users needs it.
type userAccount struct {
	key  types.NamespacedName
	keys keySource
	// operator is the NatsOperator signing the account.
	operator types.NamespacedName
	// publicKey is the account's identity; empty until it has one.
	publicKey string
	// jwt is the account JWT as signed; empty where it is not signed, as
	// for a system account no operator names.
	jwt string
	// distribution is how many servers hold jwt; nil where unknown, as for
	// a system account whose status counts servers for another JWT.
	distribution *authv1beta1.Distribution
}

// account returns the account u belongs to. ok is false where u may not
// use it yet, with Ready and ReferencesResolved set to say why.
func (r *UserReconciler) account(ctx context.Context, u *authv1beta1.NatsUser, notReady func(reason, msg string)) (userAccount, bool, error) {
	ref := u.Spec.AccountRef
	key := refKey(ref.ObjectReference, u.Namespace)
	cond, err := admit(ctx, r.Client, authGroup, "NatsUser", u, string(ref.Kind), key)
	if err != nil {
		return userAccount{}, false, err
	}
	if !referenceAdmitted(&u.Status.Conditions, u.Generation, cond, notReady) {
		return userAccount{}, false, nil
	}
	acc, found, err := r.lookupAccount(ctx, ref.Kind, key)
	if err != nil {
		return userAccount{}, false, err
	}
	if !found {
		notReady(ReasonNotFound, fmt.Sprintf("%s %s does not exist", ref.Kind, key))
		return userAccount{}, false, nil
	}
	return acc, true, nil
}

// lookupAccount reads the account of kind at key.
func (r *UserReconciler) lookupAccount(ctx context.Context, kind authv1beta1.AccountKind, key types.NamespacedName) (userAccount, bool, error) {
	out := userAccount{key: key}
	switch kind {
	case authv1beta1.AccountKindSystemAccount:
		var sys authv1beta1.NatsSystemAccount
		if err := r.Get(ctx, key, &sys); err != nil {
			return out, false, client.IgnoreNotFound(err)
		}
		out.keys = systemAccountKeySource(&sys)
		out.operator = refKey(sys.Spec.OperatorRef, sys.Namespace)
		out.publicKey = sys.Status.PublicKey
		var op authv1beta1.NatsOperator
		if err := r.Get(ctx, out.operator, &op); client.IgnoreNotFound(err) != nil {
			return out, false, err
		}
		if s := op.Status.SystemAccount; s != nil && s.Name == sys.Name && s.PublicKey == sys.Status.PublicKey && refKey(op.Spec.SystemAccountRef, op.Namespace) == key {
			out.jwt = s.JWT
		}
		if out.jwt != "" && sys.Status.JWTHash == JWTHash(out.jwt) {
			out.distribution = sys.Status.Distribution
		}
	default:
		var acc authv1beta1.NatsAccount
		if err := r.Get(ctx, key, &acc); err != nil {
			return out, false, client.IgnoreNotFound(err)
		}
		out.keys = accountKeySource(&acc)
		out.operator = refKey(acc.Spec.OperatorRef, acc.Namespace)
		out.publicKey = acc.Status.PublicKey
		out.jwt = acc.Status.JWT
		out.distribution = acc.Status.Distribution
	}
	return out, true, nil
}

// finalize takes a deleted user through revocation, distribution and the
// kick, and removes its creds Secret and then UserFinalizer.
func (r *UserReconciler) finalize(ctx context.Context, u *authv1beta1.NatsUser) (reconcile.Result, error) {
	if !controllerutil.ContainsFinalizer(u, UserFinalizer) {
		return reconcile.Result{}, nil
	}
	before := u.Status.DeepCopy()
	done, res, err := r.drain(ctx, u)
	if !done || err != nil {
		return res, updateStatus(ctx, r.Client, u, before, &u.Status, err)
	}
	if err := r.deleteCreds(ctx, u); err != nil {
		return reconcile.Result{}, err
	}
	controllerutil.RemoveFinalizer(u, UserFinalizer)
	return reconcile.Result{}, r.Update(ctx, u)
}

// drain reports whether a deleted user is revoked everywhere and has no
// connection left, with Ready set to the step it waits on otherwise. A user
// never signed, or whose account is gone or unsigned, has nothing to drain.
func (r *UserReconciler) drain(ctx context.Context, u *authv1beta1.NatsUser) (bool, reconcile.Result, error) {
	pub := u.Status.PublicKey
	if pub == "" {
		return true, reconcile.Result{}, nil
	}
	waiting := func(reason, msg string) {
		setCondition(&u.Status.Conditions, u.Generation, ConditionReady, metav1.ConditionFalse, reason, msg)
	}
	acc, found, err := r.lookupAccount(ctx, u.Spec.AccountRef.Kind, refKey(u.Spec.AccountRef.ObjectReference, u.Namespace))
	if err != nil {
		return false, reconcile.Result{}, err
	}
	if !found || acc.jwt == "" {
		return true, reconcile.Result{}, nil
	}
	if !revokedSince(acc.jwt, pub, u.DeletionTimestamp.Time) {
		waiting(ReasonRevoking, fmt.Sprintf("waiting for %s %s to revoke %s", u.Spec.AccountRef.Kind, acc.key, pub))
		return false, reconcile.Result{}, nil
	}
	if r.Sessions == nil {
		return true, reconcile.Result{}, nil
	}
	if d := acc.distribution; d == nil || d.Servers == 0 || d.Current < d.Servers {
		waiting(ReasonDistributing, fmt.Sprintf("waiting for every server to hold the JWT of %s %s", u.Spec.AccountRef.Kind, acc.key))
		return false, reconcile.Result{}, nil
	}
	n, err := r.Sessions.Kick(ctx, acc.operator, acc.publicKey, pub)
	if err != nil {
		waiting(ReasonKicking, err.Error())
		return false, reconcile.Result{}, fmt.Errorf("kick %s: %w", pub, err)
	}
	if n > 0 {
		telemetry.Emit(r.Recorder, u, telemetry.UserKicked, "closed %d connections of %s", n, pub)
		waiting(ReasonKicking, fmt.Sprintf("closed %d connections; checking none remain", n))
		return false, reconcile.Result{RequeueAfter: kickInterval}, nil
	}
	return true, reconcile.Result{}, nil
}

// deleteCreds deletes u's creds Secret if u owns it.
func (r *UserReconciler) deleteCreds(ctx context.Context, u *authv1beta1.NatsUser) error {
	if u.Spec.PublicKey != "" {
		return nil
	}
	key, _ := credsSecret(u)
	var s corev1.Secret
	if err := r.Get(ctx, key, &s); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(&s, u) {
		return nil
	}
	return client.IgnoreNotFound(r.Delete(ctx, &s, client.Preconditions{UID: &s.UID}))
}

// SetupWithManager registers the reconciler with mgr. The indexes Setup
// registers must be in place.
func (r *UserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	return ctrl.NewControllerManagedBy(mgr).
		Named("natsuser").
		For(&authv1beta1.NatsUser{}).
		Owns(&corev1.Secret{}).
		Watches(&authv1beta1.NatsAccount{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			return listIndexed(ctx, c, &authv1beta1.NatsUserList{}, userAccountField, accountValue(authv1beta1.AccountKindAccount, client.ObjectKeyFromObject(obj)))
		})).
		Watches(&authv1beta1.NatsSystemAccount{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			return listIndexed(ctx, c, &authv1beta1.NatsUserList{}, userAccountField, accountValue(authv1beta1.AccountKindSystemAccount, client.ObjectKeyFromObject(obj)))
		})).
		Watches(&authv1beta1.NatsOperator{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			op := obj.(*authv1beta1.NatsOperator)
			sys := refKey(op.Spec.SystemAccountRef, op.Namespace)
			return listIndexed(ctx, c, &authv1beta1.NatsUserList{}, userAccountField, accountValue(authv1beta1.AccountKindSystemAccount, sys))
		})).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(c, schema.GroupKind{Group: authGroup, Kind: "NatsUser"}, &authv1beta1.NatsUserList{})).
		Complete(telemetry.Traced("NatsUser", r))
}
