package natscluster

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8slabels "k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// ServerAdmin evacuates servers of one NATS cluster and removes them from
// its meta group, over the system account.
type ServerAdmin interface {
	Evacuate(ctx context.Context, server string) error
	RemovePeer(ctx context.Context, server string) error
	StepDownMeta(ctx context.Context) error
}

var _ ServerAdmin = (*sysobs.SystemClient)(nil)

// AdminFunc returns the ServerAdmin of the NATS cluster nc deployed, or an
// error saying why it has none.
type AdminFunc func(ctx context.Context, nc *clusterv1beta1.NatsCluster) (ServerAdmin, error)

// removalAction is one thing done to the server being removed.
type removalAction string

const (
	actionEvacuate   removalAction = "Evacuate"
	actionStepDown   removalAction = "StepDownMeta"
	actionRemovePeer removalAction = "RemovePeer"
	actionDelete     removalAction = "Delete"
)

// removalStep is an action on one server.
type removalStep struct {
	Server string
	Action removalAction
}

// removalState is what a rollout decision reads about removing servers.
// Surplus and Replace are in ordinal order.
type removalState struct {
	// Removing is the server whose removal has begun, "" when none has,
	// Phase how far it has gone, and Since when it entered Phase, zero
	// when unrecorded.
	Removing string
	Phase    clusterv1beta1.RemovalPhase
	Since    time.Time
	// Surplus are the servers beyond spec.replicas.
	Surplus []string
	// Replace are the servers of the plan waiting to be replaced: their
	// volume claim templates differ from the plan's, or replace-server
	// named them.
	Replace []string
	// Rejoining are the servers a replacement recreated that the gate has
	// not yet passed.
	Rejoining []string
	Replicas  int
	JetStream bool
	// NoAdmin says why no server can be evacuated or removed, "" when one
	// can.
	NoAdmin  string
	Snapshot *sysobs.Snapshot
}

// stepKind is what a rollout step does to a server.
type stepKind int

const (
	kindRestart stepKind = iota
	kindScaleDown
	kindReplace
)

func (st rolloutState) kindOf(server string) stepKind {
	switch {
	case slices.Contains(st.Removal.Surplus, server):
		return kindScaleDown
	case server == st.Removal.Removing || slices.Contains(st.Removal.Replace, server) || slices.Contains(st.Removal.Rejoining, server):
		return kindReplace
	}
	return kindRestart
}

// startRemoval is the first action on server: its evacuation, or its
// deletion when the servers run no JetStream.
func startRemoval(rm removalState, server string) removalStep {
	if !rm.JetStream {
		return removalStep{Server: server, Action: actionDelete}
	}
	return removalStep{Server: server, Action: actionEvacuate}
}

// continueRemoval is the next action on the server being removed, or nil
// while it waits, with what it waits for.
func continueRemoval(rm removalState, metaLeader string) (*removalStep, gateState) {
	x, snap := rm.Removing, rm.Snapshot
	if snap == nil {
		return nil, gateState{GateSettled, "the NATS cluster is not observed"}
	}
	if rm.Phase == clusterv1beta1.RemovalRemoved {
		if member, known := metaMember(snap, x); member || !known {
			return &removalStep{Server: x, Action: actionRemovePeer}, gateState{GateSettled, "removing " + x + " from the meta group"}
		}
		return &removalStep{Server: x, Action: actionDelete}, gateState{GateSettled, "deleting " + x}
	}
	if n := groupsHeld(snap, x); n > 0 {
		return nil, gateState{GateEvacuated, fmt.Sprintf("%s still holds %d Raft groups", x, n)}
	}
	if why := unsettledWithout(snap.Verdict(), x); why != "" {
		return nil, gateState{GateSettled, why}
	}
	if metaLeader == x {
		return &removalStep{Server: x, Action: actionStepDown}, gateState{GateSettled, x + " is stepping down as meta leader"}
	}
	return &removalStep{Server: x, Action: actionRemovePeer}, gateState{GateSettled, "removing " + x + " from the meta group"}
}

// removalGate is what the rollout waits for once startRemoval's step is
// taken.
func removalGate(step removalStep) gateState {
	if step.Action == actionEvacuate {
		return gateState{GateEvacuated, "evacuating " + step.Server}
	}
	return gateState{GateSettled, "deleting " + step.Server}
}

// scaleDownBlocked says why the servers beyond spec.replicas cannot be
// removed, "" when they can.
func scaleDownBlocked(rm removalState) string {
	if !rm.JetStream {
		return ""
	}
	if rm.NoAdmin != "" {
		return "cannot evacuate servers: " + rm.NoAdmin
	}
	if rm.Snapshot == nil {
		return ""
	}
	var wide []string
	for _, g := range rm.Snapshot.Groups {
		if g.Kind == sysobs.KindStream && len(g.Members) > rm.Replicas {
			wide = append(wide, fmt.Sprintf("%s has %d replicas", streamLabel(g), len(g.Members)))
		}
	}
	if len(wide) == 0 {
		return ""
	}
	return fmt.Sprintf("%s; %d servers cannot hold them", namedList(wide), rm.Replicas)
}

// namedList joins up to maxNamedGroups of names, counting the rest.
func namedList(names []string) string {
	out := slices.Clone(names[:min(len(names), maxNamedGroups)])
	if n := len(names) - maxNamedGroups; n > 0 {
		out = append(out, fmt.Sprintf("and %d more", n))
	}
	return strings.Join(out, ", ")
}

// groupsHeld counts the stream and consumer groups listing server as a
// member.
func groupsHeld(snap *sysobs.Snapshot, server string) int {
	n := 0
	for _, g := range snap.Groups {
		if g.Kind != sysobs.KindMeta && slices.ContainsFunc(g.Members, func(m sysobs.Member) bool { return m.Server == server }) {
			n++
		}
	}
	return n
}

// metaMember reports whether snap's meta group lists server as a member,
// with known false when the meta group is FromFollowers.
func metaMember(snap *sysobs.Snapshot, server string) (member, known bool) {
	for _, g := range snap.Groups {
		if g.Kind == sysobs.KindMeta {
			return slices.ContainsFunc(g.Members, func(m sysobs.Member) bool { return m.Server == server }), !g.FromFollowers
		}
	}
	return false, true
}

// unsettledWithout describes what keeps v from Settled once server's own
// silence and its own offline or lagging memberships are set aside, ""
// when nothing does.
func unsettledWithout(v sysobs.Verdict, server string) string {
	v.Silent = slices.DeleteFunc(slices.Clone(v.Silent), func(s string) bool { return s == server })
	v.Unsettled = slices.DeleteFunc(slices.Clone(v.Unsettled), func(u sysobs.Unsettled) bool {
		return u.Reason != sysobs.ReasonNoLeader && len(u.Servers) == 1 && u.Servers[0] == server
	})
	if v.Settled() {
		return ""
	}
	return describeUnsettled(v)
}

// removalOf collects the removal state of nc from status.removals and its
// StatefulSets.
func removalOf(nc *clusterv1beta1.NatsCluster, plan *Plan, sets map[string]*appsv1.StatefulSet, snap *sysobs.Snapshot) removalState {
	rm := removalState{Replicas: int(nc.Spec.Replicas), JetStream: nc.Spec.JetStream != nil, Snapshot: snap}
	byOrdinal := func(a, b string) int { return ordinal(nc, a) - ordinal(nc, b) }
	removals := slices.Clone(nc.Status.Removals)
	slices.SortFunc(removals, func(a, b clusterv1beta1.ServerRemoval) int { return byOrdinal(a.Name, b.Name) })
	for _, rv := range removals {
		switch rv.Phase {
		case clusterv1beta1.RemovalEvacuating, clusterv1beta1.RemovalRemoved:
			if rm.Removing == "" {
				rm.Removing, rm.Phase = rv.Name, rv.Phase
				if rv.Since != nil {
					rm.Since = rv.Since.Time
				}
			}
		case clusterv1beta1.RemovalRejoining:
			rm.Rejoining = append(rm.Rejoining, rv.Name)
		}
	}
	want := map[string]string{}
	for _, s := range plan.Servers {
		want[s.Name] = s.StatefulSet.Annotations[AnnotationVolumeDigest]
	}
	names := slices.SortedFunc(maps.Keys(sets), byOrdinal)
	for _, name := range names {
		digest, inPlan := want[name]
		switch {
		case !inPlan:
			rm.Surplus = append(rm.Surplus, name)
		case removalPhaseOf(nc, name) == clusterv1beta1.RemovalRequested || sets[name].Annotations[AnnotationVolumeDigest] != digest:
			rm.Replace = append(rm.Replace, name)
		}
	}
	return rm
}

// removalPhaseOf is server's phase in nc's status.removals, "" when its
// removal has not begun.
func removalPhaseOf(nc *clusterv1beta1.NatsCluster, server string) clusterv1beta1.RemovalPhase {
	for _, rv := range nc.Status.Removals {
		if rv.Name == server {
			return rv.Phase
		}
	}
	return ""
}

// withRemoval is removals with server at phase, entered at now unless it
// was there already, or without server where phase is "".
func withRemoval(removals []clusterv1beta1.ServerRemoval, server string, phase clusterv1beta1.RemovalPhase, now time.Time) []clusterv1beta1.ServerRemoval {
	i := slices.IndexFunc(removals, func(rv clusterv1beta1.ServerRemoval) bool { return rv.Name == server })
	switch {
	case phase == "" && i < 0:
		return removals
	case phase == "":
		return slices.Delete(slices.Clone(removals), i, i+1)
	case i >= 0 && removals[i].Phase == phase:
		return removals
	}
	rv := clusterv1beta1.ServerRemoval{Name: server, Phase: phase, Since: &metav1.Time{Time: now}}
	if i < 0 {
		return append(slices.Clone(removals), rv)
	}
	out := slices.Clone(removals)
	out[i] = rv
	return out
}

// ordinal is the index a server's name carries, -1 for a name not of nc's
// form.
func ordinal(nc *clusterv1beta1.NatsCluster, server string) int {
	i, err := strconv.Atoi(strings.TrimPrefix(server, nc.Name+"-"))
	if err != nil {
		return -1
	}
	return i
}

// admin returns nc's ServerAdmin, or nil and why there is none.
func (r *Reconciler) admin(ctx context.Context, nc *clusterv1beta1.NatsCluster) (ServerAdmin, string) {
	if r.Admin == nil {
		return nil, "no system account connection"
	}
	a, err := r.Admin(ctx, nc)
	if err != nil {
		return nil, err.Error()
	}
	return a, ""
}

// requestReplacement marks the server the replace-server annotation names
// for replacement, unless its removal has begun, and clears the
// annotation, whether or not it named a server of the plan.
func (r *Reconciler) requestReplacement(ctx context.Context, nc *clusterv1beta1.NatsCluster, plan *Plan, sets map[string]*appsv1.StatefulSet) error {
	name, ok := nc.Annotations[clusterv1beta1.AnnotationReplaceServer]
	if !ok {
		return nil
	}
	if sets[name] != nil && slices.ContainsFunc(plan.Servers, func(s Server) bool { return s.Name == name }) && removalPhaseOf(nc, name) == "" {
		if err := r.setRemoval(ctx, nc, name, clusterv1beta1.RemovalRequested); err != nil {
			return err
		}
	}
	return r.clearAnnotation(ctx, nc, clusterv1beta1.AnnotationReplaceServer, name)
}

// remove carries out step. An evacuation or removal the meta leader
// answers with ErrNotMember has nothing left to do and counts as done; a
// removal refused while another membership change is in flight is retried
// on a later reconcile.
func (r *Reconciler) remove(ctx context.Context, nc *clusterv1beta1.NatsCluster, admin ServerAdmin, noAdmin string, step removalStep, sets map[string]*appsv1.StatefulSet, surplus bool) error {
	if step.Action == actionDelete {
		if err := r.setRemoval(ctx, nc, step.Server, clusterv1beta1.RemovalDeleting); err != nil {
			return err
		}
		return r.deleteServer(ctx, nc, step.Server, sets[step.Server], surplus)
	}
	if admin == nil {
		return fmt.Errorf("%s %s: %s", step.Action, step.Server, noAdmin)
	}
	switch step.Action {
	case actionEvacuate:
		if err := admin.Evacuate(ctx, step.Server); err != nil && !errors.Is(err, sysobs.ErrNotMember) {
			return fmt.Errorf("evacuate %s: %w", step.Server, err)
		}
		return r.setRemoval(ctx, nc, step.Server, clusterv1beta1.RemovalEvacuating)
	case actionStepDown:
		if err := admin.StepDownMeta(ctx); err != nil {
			return fmt.Errorf("step down meta leader %s: %w", step.Server, err)
		}
		return nil
	case actionRemovePeer:
		err := admin.RemovePeer(ctx, step.Server)
		switch {
		case err == nil, errors.Is(err, sysobs.ErrNotMember):
			return r.setRemoval(ctx, nc, step.Server, clusterv1beta1.RemovalRemoved)
		case errors.Is(err, sysobs.ErrChangeInflight):
			return nil
		default:
			return fmt.Errorf("remove %s from the meta group: %w", step.Server, err)
		}
	}
	return fmt.Errorf("unknown removal action %q", step.Action)
}

// rejoined forgets the removal of every server in rejoining.
func (r *Reconciler) rejoined(ctx context.Context, nc *clusterv1beta1.NatsCluster, rejoining []string) error {
	for _, name := range rejoining {
		if err := r.setRemoval(ctx, nc, name, ""); err != nil {
			return err
		}
	}
	return nil
}

// setRemoval patches server's removal phase into nc's status.removals at
// once, so that it outlives the objects the next action deletes, or drops
// server from it where phase is "".
func (r *Reconciler) setRemoval(ctx context.Context, nc *clusterv1beta1.NatsCluster, server string, phase clusterv1beta1.RemovalPhase) error {
	orig := nc.DeepCopy()
	nc.Status.Removals = withRemoval(nc.Status.Removals, server, phase, r.now())
	if equality.Semantic.DeepEqual(orig.Status.Removals, nc.Status.Removals) {
		return nil
	}
	if err := r.Client.Status().Patch(ctx, nc, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("record removal of %s as %q: %w", server, phase, err)
	}
	return nil
}

// finishDeletions carries through every server whose removal reached
// Deleting and drops each deleted StatefulSet from sets. It returns the
// servers of plan still waiting for their data volume claim to go.
func (r *Reconciler) finishDeletions(ctx context.Context, nc *clusterv1beta1.NatsCluster, plan *Plan, sets map[string]*appsv1.StatefulSet) ([]string, error) {
	var waiting []string
	for _, rv := range slices.Clone(nc.Status.Removals) {
		if rv.Phase != clusterv1beta1.RemovalDeleting {
			continue
		}
		inPlan := slices.ContainsFunc(plan.Servers, func(s Server) bool { return s.Name == rv.Name })
		if err := r.deleteServer(ctx, nc, rv.Name, sets[rv.Name], !inPlan); err != nil {
			return nil, err
		}
		delete(sets, rv.Name)
		next := clusterv1beta1.RemovalPhase("")
		if inPlan {
			gone, err := r.claimGone(ctx, nc, rv.Name)
			if err != nil {
				return nil, err
			}
			if !gone {
				waiting = append(waiting, rv.Name)
				continue
			}
			next = clusterv1beta1.RemovalRejoining
		}
		if err := r.setRemoval(ctx, nc, rv.Name, next); err != nil {
			return nil, err
		}
	}
	return waiting, nil
}

// deleteServer deletes server's StatefulSet, its data volume claim when the
// claim carries server's selector labels, and for a surplus server its
// ConfigMap when nc controls it. It returns a *notControlledError naming
// every claim or ConfigMap it left.
func (r *Reconciler) deleteServer(ctx context.Context, nc *clusterv1beta1.NatsCluster, server string, sts *appsv1.StatefulSet, surplus bool) error {
	if sts != nil {
		if err := r.Client.Delete(ctx, sts, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete statefulset %s: %w", server, err)
		}
	}
	var refused refusals
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: nc.Namespace, Name: dataClaimName(server)}}
	if err := refused.add(r.deleteIf(ctx, claim, func() bool {
		return k8slabels.SelectorFromSet(serverSelector(nc, server)).Matches(k8slabels.Set(claim.Labels))
	})); err != nil {
		return err
	}
	if surplus {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: nc.Namespace, Name: configMapName(server)}}
		if err := refused.add(r.deleteIf(ctx, cm, func() bool { return metav1.IsControlledBy(cm, nc) })); err != nil {
			return err
		}
	}
	return refused.err()
}

// deleteIf deletes obj, as read, when it exists and ours reports it as
// nc's, and returns a *notControlledError when it exists and is not.
func (r *Reconciler) deleteIf(ctx context.Context, obj client.Object, ours func() bool) error {
	if err := r.uncached().Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get %s: %w", obj.GetName(), err)
	}
	if !ours() {
		return r.notControlled(obj)
	}
	uid, rv := obj.GetUID(), obj.GetResourceVersion()
	if err := r.Client.Delete(ctx, obj, client.Preconditions{UID: &uid, ResourceVersion: &rv}); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete %s: %w", obj.GetName(), err)
	}
	return nil
}

// claimTerminating reports whether server's data volume claim is being
// deleted.
func (r *Reconciler) claimTerminating(ctx context.Context, nc *clusterv1beta1.NatsCluster, server string) (bool, error) {
	pvc, err := r.claim(ctx, nc, server)
	return pvc != nil && !pvc.DeletionTimestamp.IsZero(), err
}

// claimGone reports whether server's data volume claim does not exist.
func (r *Reconciler) claimGone(ctx context.Context, nc *clusterv1beta1.NatsCluster, server string) (bool, error) {
	pvc, err := r.claim(ctx, nc, server)
	return pvc == nil && err == nil, err
}

// claim is server's data volume claim, nil when there is none.
func (r *Reconciler) claim(ctx context.Context, nc *clusterv1beta1.NatsCluster, server string) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.uncached().Get(ctx, client.ObjectKey{Namespace: nc.Namespace, Name: dataClaimName(server)}, pvc)
	switch {
	case apierrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("get pvc %s: %w", dataClaimName(server), err)
	}
	return pvc, nil
}

// streamLabel names a stream group as account/stream, by the account's
// name where it has one.
func streamLabel(g sysobs.Group) string {
	account := g.AccountName
	if account == "" {
		account = g.Account
	}
	return account + "/" + g.Stream
}
