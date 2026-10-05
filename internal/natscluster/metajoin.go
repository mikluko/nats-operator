package natscluster

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// ReasonOwnMetaGroup is the Progressing reason of a NatsCluster whose
// gateway remotes are not rendered because its servers may hold a JetStream
// meta group of their own.
const ReasonOwnMetaGroup = "OwnMetaGroup"

const ownMetaGroupWayOut = "two meta groups meeting over a gateway do not merge, and the one that loses deletes every stream the other does not list; delete the claims of every server and let them start empty, or set jetstream.domain"

// joinsMetaGroup reports whether nc's servers run JetStream in the meta
// group of the supercluster its gateway remotes form: JetStream with no
// domain, and a remote other than nc's own entry.
func joinsMetaGroup(nc *clusterv1beta1.NatsCluster) bool {
	js, g := nc.Spec.JetStream, nc.Spec.Gateway
	if js == nil || js.Domain != "" || g == nil {
		return false
	}
	return slices.ContainsFunc(g.Remotes, func(r clusterv1beta1.GatewayRemote) bool { return r.Name != nc.Name })
}

// metaShape is what of a server's config decides which JetStream meta group
// it is in.
type metaShape struct {
	jetStream bool
	domain    string
	remotes   []string
}

func (a metaShape) equal(b metaShape) bool {
	return a.jetStream == b.jetStream && a.domain == b.domain && slices.Equal(a.remotes, b.remotes)
}

// shapeOf decodes the metaShape of a rendered config file.
func shapeOf(data string) (metaShape, error) {
	var c Config
	if err := json.Unmarshal([]byte(data), &c); err != nil {
		return metaShape{}, fmt.Errorf("decode config: %w", err)
	}
	var s metaShape
	if c.JetStream != nil {
		s.jetStream, s.domain = true, c.JetStream.Domain
	}
	if c.Gateway != nil {
		for _, r := range c.Gateway.Gateways {
			s.remotes = append(s.remotes, r.Name)
		}
		slices.Sort(s.remotes)
	}
	return s, nil
}

// metaJoinHold returns the Progressing condition that holds nc at its last
// render, or nil when nothing does. It holds a NatsCluster that joins a
// supercluster's meta group while a server it would create has a data
// volume claim older than nc, or while a server it would move into that
// group by a config change holds a meta group of the NATS cluster's own or
// cannot be observed.
func (r *Reconciler) metaJoinHold(ctx context.Context, nc *clusterv1beta1.NatsCluster, plan *Plan, sts map[string]*appsv1.StatefulSet, snap *sysobs.Snapshot, observeErr error) (*metav1.Condition, error) {
	if !joinsMetaGroup(nc) {
		return nil, nil
	}
	var stale, moving []string
	for _, s := range plan.Servers {
		if sts[s.Name] == nil {
			pvc, err := r.claim(ctx, nc, s.Name)
			if err != nil {
				return nil, err
			}
			if pvc != nil && pvc.DeletionTimestamp.IsZero() && pvc.CreationTimestamp.Before(&nc.CreationTimestamp) {
				stale = append(stale, pvc.Name)
			}
			continue
		}
		changes, err := r.changesMetaShape(ctx, s)
		if err != nil {
			return nil, err
		}
		if changes {
			moving = append(moving, s.Name)
		}
	}
	if len(stale) > 0 {
		return ownMetaGroup(fmt.Sprintf("the data volume claims %s are older than the NatsCluster and may hold a JetStream meta group of their own, so their servers are not created: %s",
			strings.Join(stale, ", "), ownMetaGroupWayOut)), nil
	}
	if len(moving) == 0 {
		return nil, nil
	}
	switch own, known, why := ownMeta(snap, moving); {
	case !known:
		msg := fmt.Sprintf("the JetStream meta group of %s is not known, so their gateway remotes are not rendered: %s", strings.Join(moving, ", "), why)
		if snap == nil && observeErr != nil {
			msg += ": " + observeErr.Error()
		}
		return &metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: ReasonObservationFailed, Message: msg}, nil
	case own:
		return ownMetaGroup(fmt.Sprintf("%s hold a JetStream meta group of the NATS cluster's own, led by %s, so their gateway remotes are not rendered: %s",
			strings.Join(moving, ", "), snap.MetaLeader(), ownMetaGroupWayOut)), nil
	}
	return nil, nil
}

func ownMetaGroup(msg string) *metav1.Condition {
	return &metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: ReasonOwnMetaGroup, Message: msg}
}

// changesMetaShape reports whether server s's ConfigMap renders a metaShape
// other than the plan's; a missing ConfigMap changes nothing, since the
// server then restarts onto the plan's config from no running one.
func (r *Reconciler) changesMetaShape(ctx context.Context, s Server) (bool, error) {
	cm := &corev1.ConfigMap{}
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(s.ConfigMap), cm); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get configmap %s: %w", s.ConfigMap.Name, err)
	}
	running, err := shapeOf(cm.Data[configFile])
	if err != nil {
		return true, nil
	}
	want, err := shapeOf(s.ConfigMap.Data[configFile])
	if err != nil {
		return false, err
	}
	return !running.equal(want), nil
}

// ownMeta judges from snap whether servers hold a meta group of their NATS
// cluster's own: one whose leader is a server of the NATS cluster and lists
// no peer outside it. known is false, with why, when snap cannot tell: it is
// nil, one of servers did not answer, or the leader is a server of the NATS
// cluster that did not report its peers. A meta group without a leader, or
// none at all, is not judged own.
func ownMeta(snap *sysobs.Snapshot, servers []string) (own, known bool, why string) {
	if snap == nil {
		return false, false, "the NATS cluster is not observed"
	}
	if silent := slices.DeleteFunc(slices.Clone(servers), func(s string) bool { return !slices.Contains(snap.Silent, s) }); len(silent) > 0 {
		return false, false, strings.Join(silent, ", ") + " did not answer"
	}
	i := slices.IndexFunc(snap.Groups, func(g sysobs.Group) bool { return g.Kind == sysobs.KindMeta })
	if i < 0 || snap.Groups[i].Leader == "" {
		return false, true, ""
	}
	g := snap.Groups[i]
	if !slices.ContainsFunc(snap.Servers, func(s sysobs.Server) bool { return s.Name == g.Leader }) {
		return false, true, ""
	}
	if g.FromFollowers {
		return false, false, "its leader " + g.Leader + " did not report its peers"
	}
	return len(g.Outside) == 0, true, ""
}
