package authctl

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
)

// keyClaim is an object's claim to a public key: the key its status
// records.
type keyClaim struct {
	kind     string
	key      types.NamespacedName
	uid      types.UID
	created  metav1.Time
	recorded string
}

func (c keyClaim) String() string { return c.kind + " " + c.key.String() }

func claimOf(kind string, obj client.Object, recorded string) keyClaim {
	return keyClaim{kind: kind, key: client.ObjectKeyFromObject(obj), uid: obj.GetUID(), created: obj.GetCreationTimestamp(), recorded: recorded}
}

// keyHolder returns the claim among others that holds pub against self: one
// recording pub where self does not, or, where both do, the one created
// first, by name within a second.
func keyHolder(self keyClaim, pub string, others []keyClaim) (keyClaim, bool) {
	for _, o := range others {
		if o.uid == self.uid || o.recorded != pub {
			continue
		}
		if self.recorded != pub || precedes(o, self) {
			return o, true
		}
	}
	return keyClaim{}, false
}

func precedes(a, b keyClaim) bool {
	if !a.created.Equal(&b.created) {
		return a.created.Before(&b.created)
	}
	return a.key.String() < b.key.String()
}

// accountKeyHolder returns who holds pub against acc under the NatsOperator
// at operator: the NatsSystemAccount it references, where that names it
// back and its spec or status carries pub, or another NatsAccount per
// keyHolder.
func accountKeyHolder(ctx context.Context, c client.Reader, acc *authv1beta1.NatsAccount, operator types.NamespacedName, pub string) (string, error) {
	var op authv1beta1.NatsOperator
	switch err := c.Get(ctx, operator, &op); {
	case apierrors.IsNotFound(err):
	case err != nil:
		return "", fmt.Errorf("get NatsOperator %s: %w", operator, err)
	default:
		var sys authv1beta1.NatsSystemAccount
		err := c.Get(ctx, refKey(op.Spec.SystemAccountRef, op.Namespace), &sys)
		if client.IgnoreNotFound(err) != nil {
			return "", fmt.Errorf("get NatsSystemAccount: %w", err)
		}
		if err == nil && refKey(sys.Spec.OperatorRef, sys.Namespace) == operator && (sys.Status.PublicKey == pub || sys.Spec.PublicKey == pub) {
			return claimOf("NatsSystemAccount", &sys, pub).String(), nil
		}
	}
	var accounts authv1beta1.NatsAccountList
	if err := c.List(ctx, &accounts, client.MatchingFields{operatorField: keyValue(operator)}); err != nil {
		return "", fmt.Errorf("list NatsAccounts: %w", err)
	}
	others := make([]keyClaim, 0, len(accounts.Items))
	for i := range accounts.Items {
		others = append(others, claimOf("NatsAccount", &accounts.Items[i], accounts.Items[i].Status.PublicKey))
	}
	if h, held := keyHolder(claimOf("NatsAccount", acc, acc.Status.PublicKey), pub, others); held {
		return h.String(), nil
	}
	return "", nil
}

// sameKeyAccounts returns a request for every other NatsAccount under the
// NatsOperator obj names whose status records obj's public key.
func sameKeyAccounts(ctx context.Context, c client.Reader, obj client.Object) []reconcile.Request {
	acc, ok := obj.(*authv1beta1.NatsAccount)
	if !ok || acc.Status.PublicKey == "" {
		return nil
	}
	var list authv1beta1.NatsAccountList
	if err := c.List(ctx, &list, client.MatchingFields{operatorField: keyValue(refKey(acc.Spec.OperatorRef, acc.Namespace))}); err != nil {
		log.FromContext(ctx).Error(err, "list NatsAccounts by operator")
		return nil
	}
	var out []reconcile.Request
	for i := range list.Items {
		o := &list.Items[i]
		if o.UID != acc.UID && o.Status.PublicKey == acc.Status.PublicKey {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(o)})
		}
	}
	return out
}

// userKeyHolder returns the other NatsUser of u's account holding pub
// against u, per keyHolder.
func userKeyHolder(ctx context.Context, c client.Reader, u *authv1beta1.NatsUser, pub string) (string, error) {
	users, err := listUsers(ctx, c, u.Spec.AccountRef.Kind, refKey(u.Spec.AccountRef.ObjectReference, u.Namespace))
	if err != nil {
		return "", err
	}
	others := make([]keyClaim, 0, len(users))
	for i := range users {
		others = append(others, claimOf("NatsUser", &users[i], users[i].Status.PublicKey))
	}
	if h, held := keyHolder(claimOf("NatsUser", u, u.Status.PublicKey), pub, others); held {
		return h.String(), nil
	}
	return "", nil
}

// sameKeyUsers returns a request for every other NatsUser of obj's account
// whose status records obj's public key.
func sameKeyUsers(ctx context.Context, c client.Reader, obj client.Object) []reconcile.Request {
	u, ok := obj.(*authv1beta1.NatsUser)
	if !ok || u.Status.PublicKey == "" {
		return nil
	}
	users, err := listUsers(ctx, c, u.Spec.AccountRef.Kind, refKey(u.Spec.AccountRef.ObjectReference, u.Namespace))
	if err != nil {
		log.FromContext(ctx).Error(err, "list NatsUsers by account")
		return nil
	}
	var out []reconcile.Request
	for i := range users {
		o := &users[i]
		if o.UID != u.UID && o.Status.PublicKey == u.Status.PublicKey {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(o)})
		}
	}
	return out
}
