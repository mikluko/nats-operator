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
