package authctl

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
)

var (
	authGroup = authv1beta1.GroupVersion.Group
	natsGroup = natsv1beta1.GroupVersion.Group
)

// admit asks internal/grant whether from, of group and kind, may reference
// the object of toKind in the auth group at to. A nil condition admits.
func admit(ctx context.Context, r client.Reader, group, kind string, from client.Object, toKind string, to types.NamespacedName) (*metav1.Condition, error) {
	return grant.Admit(ctx, r,
		grant.Referrer{Group: group, Kind: kind, Namespace: from.GetNamespace()},
		grant.Target{Group: authGroup, Kind: toKind, Namespace: to.Namespace, Name: to.Name})
}

// admitAccount asks internal/grant whether a NatsAccount in namespace may
// reference the NatsOperator at op. A nil condition admits.
func admitAccount(ctx context.Context, r client.Reader, namespace string, op types.NamespacedName) (*metav1.Condition, error) {
	return grant.Admit(ctx, r,
		grant.Referrer{Group: authGroup, Kind: "NatsAccount", Namespace: namespace},
		grant.Target{Group: authGroup, Kind: "NatsOperator", Namespace: op.Namespace, Name: op.Name})
}

// refusedAccounts returns the keys of the accounts in accounts, the
// NatsAccounts naming the NatsOperator at op, that are not admitted to it.
func refusedAccounts(ctx context.Context, r client.Reader, op types.NamespacedName, accounts []authv1beta1.NatsAccount) (map[types.NamespacedName]bool, error) {
	out := map[types.NamespacedName]bool{}
	for i := range accounts {
		cond, err := admitAccount(ctx, r, accounts[i].Namespace, op)
		if err != nil {
			return nil, err
		}
		if cond != nil {
			out[client.ObjectKeyFromObject(&accounts[i])] = true
		}
	}
	return out, nil
}
