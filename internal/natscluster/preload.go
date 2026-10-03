package natscluster

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
)

// Reasons an auth.accountTrustRefs entry is refused with.
const (
	ReasonAccountTrustNotFound = "AccountTrustNotFound"
	ReasonAccountTrustNotReady = "AccountTrustNotReady"
	ReasonAccountTrustInvalid  = "AccountTrustInvalid"
)

var preloadTrustReasons = trustReasons{ReasonAccountTrustNotFound, ReasonAccountTrustNotReady, ReasonAccountTrustInvalid}

// AccountPreload is an account every server preloads, read from one of
// auth.accountTrustRefs.
type AccountPreload struct {
	PublicKey string
	JWT       string
}

// readAccountPreloads resolves nc's auth.accountTrustRefs under trust, one
// per entry; an entry that does not resolve, or whose trust carries no JWT,
// returns nil and the Progressing condition saying why.
func readAccountPreloads(ctx context.Context, r client.Reader, nc *clusterv1beta1.NatsCluster, trust *Trust) ([]AccountPreload, *metav1.Condition, error) {
	if nc.Spec.Auth == nil {
		return nil, nil, nil
	}
	from := grant.Referrer{Group: clusterv1beta1.GroupVersion.Group, Kind: "NatsCluster", Namespace: nc.Namespace}
	var out []AccountPreload
	for i, ref := range nc.Spec.Auth.AccountTrustRefs {
		key := ref.ObjectKey(nc.Namespace)
		acc, cond, err := readAccountTrust(ctx, r, from, nc, trust, ref, preloadTrustReasons)
		if err != nil {
			return nil, nil, err
		}
		if cond == nil {
			cond = noJWT(key, acc)
		}
		if cond != nil {
			cond.Message = fmt.Sprintf("auth.accountTrustRefs[%d]: %s", i, cond.Message)
			return nil, cond, nil
		}
		out = append(out, AccountPreload{PublicKey: acc.PublicKey, JWT: acc.JWT})
	}
	return out, nil, nil
}

// noJWT is the Progressing condition refusing acc, read from the trust key,
// for carrying no JWT to preload, or nil when it carries one.
func noJWT(key types.NamespacedName, acc trustedAccount) *metav1.Condition {
	c := &metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse}
	switch {
	case acc.JWT != "":
		return nil
	case acc.Reported:
		c.Reason, c.Message = ReasonAccountTrustNotReady, fmt.Sprintf("NatsAccountTrust %s has no JWT in its status yet", key)
	default:
		c.Reason, c.Message = ReasonAccountTrustInvalid, fmt.Sprintf("NatsAccountTrust %s carries no jwt to preload", key)
	}
	return c
}

// checkPreloads returns the Progressing condition refusing what nc
// preloads under trust through remotes and accounts, or nil: two JWTs for
// one account, or a leaf preloading an account into a Full resolver with no
// jetstream.volumeClaimTemplate.
func checkPreloads(nc *clusterv1beta1.NatsCluster, trust *Trust, remotes []LeafRemote, accounts []AccountPreload) *metav1.Condition {
	if trust == nil {
		return nil
	}
	if c := preloadConflict(trust, remotes, accounts); c != nil {
		return c
	}
	if len(nc.Spec.LeafRemotes) > 0 && preloads(remotes, accounts) && resolverType(nc, remotes, accounts) == clusterv1beta1.ResolverFull &&
		(nc.Spec.JetStream == nil || nc.Spec.JetStream.VolumeClaimTemplate == nil) {
		return unsupportedSpec("a leaf preloading accounts keeps its Full resolver on jetstream.volumeClaimTemplate, which is not set; set it, or set auth.resolver: Cache")
	}
	return nil
}

// preloadConflict returns the Progressing condition refusing two preloads
// of one account with different JWTs among the system account, remotes and
// accounts, or nil.
func preloadConflict(trust *Trust, remotes []LeafRemote, accounts []AccountPreload) *metav1.Condition {
	type preload struct{ source, jwt string }
	seen := map[string]preload{trust.SystemAccount: {"auth.trustRef", trust.SystemAccountJWT}}
	check := func(source, pub, jwt string) *metav1.Condition {
		p, ok := seen[pub]
		if !ok {
			seen[pub] = preload{source, jwt}
			return nil
		}
		if p.jwt == jwt {
			return nil
		}
		return &metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: ReasonAccountTrustInvalid,
			Message: fmt.Sprintf("%s and %s give account %s different JWTs", p.source, source, pub)}
	}
	for i, r := range remotes {
		if r.PreloadJWT == "" {
			continue
		}
		if c := check(fmt.Sprintf("leafRemotes[%d]", i), r.LocalAccount, r.PreloadJWT); c != nil {
			return c
		}
	}
	for i, a := range accounts {
		if c := check(fmt.Sprintf("auth.accountTrustRefs[%d]", i), a.PublicKey, a.JWT); c != nil {
			return c
		}
	}
	return nil
}
