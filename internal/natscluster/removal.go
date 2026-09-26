package natscluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// ServerAdmin evacuates servers of one NATS cluster and removes them from
// its meta group, over the system account; *sysobs.Observer is one.
type ServerAdmin interface {
	Evacuate(ctx context.Context, server string) error
	RemovePeer(ctx context.Context, server string) error
	StepDownMeta(ctx context.Context) error
}

// AdminFunc returns the ServerAdmin of the NATS cluster nc deployed, or an
// error saying why it has none.
type AdminFunc func(ctx context.Context, nc *clusterv1beta1.NatsCluster) (ServerAdmin, error)

// removalPhase is how far a server's removal has gone, as its
// StatefulSet's AnnotationRemoval records it.
type removalPhase string

const (
	// phaseRequested is a server the replace-server annotation named,
	// waiting its turn to be replaced.
	phaseRequested removalPhase = "Requested"
	// phaseEvacuating is a server whose evacuation the meta leader
	// accepted.
	phaseEvacuating removalPhase = "Evacuating"
	// phaseRemoved is a server whose removal from the meta group was
	// committed.
	phaseRemoved removalPhase = "Removed"
	// phaseRejoining is a server recreated by its replacement, until the
	// rollout gate next opens.
	phaseRejoining removalPhase = "Rejoining"
)

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
	// and Phase how far it has gone.
	Removing string
	Phase    removalPhase
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
// while it waits, with what it waits for. An evacuated server is removed
// from the meta group once it holds no Raft group and the NATS cluster is
// Settled apart from it; a meta leader steps down first. A removed server
// is deleted once the meta group no longer lists it, and is removed again
// while it does.
func continueRemoval(rm removalState, metaLeader string) (*removalStep, gateState) {
	x, snap := rm.Removing, rm.Snapshot
	if snap == nil {
		return nil, gateState{GateSettled, "the NATS cluster is not observed"}
	}
	if rm.Phase == phaseRemoved {
		if inMetaGroup(snap, x) {
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
// removed, "" when they can: no system user to evacuate them over, or a
// stream with more replicas than spec.replicas.
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

// inMetaGroup reports whether snap's meta group lists server as a member.
func inMetaGroup(snap *sysobs.Snapshot, server string) bool {
	for _, g := range snap.Groups {
		if g.Kind == sysobs.KindMeta {
			return slices.ContainsFunc(g.Members, func(m sysobs.Member) bool { return m.Server == server })
		}
	}
	return false
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

// removalOf collects the removal state of nc from its StatefulSets.
func removalOf(nc *clusterv1beta1.NatsCluster, plan *Plan, sets map[string]*appsv1.StatefulSet, snap *sysobs.Snapshot) removalState {
	rm := removalState{Replicas: int(nc.Spec.Replicas), JetStream: nc.Spec.JetStream != nil, Snapshot: snap}
	want := map[string]string{}
	for _, s := range plan.Servers {
		want[s.Name] = s.StatefulSet.Annotations[AnnotationVolumeDigest]
	}
	names := make([]string, 0, len(sets))
	for name := range sets {
		names = append(names, name)
	}
	slices.SortFunc(names, func(a, b string) int { return ordinal(nc, a) - ordinal(nc, b) })
	for _, name := range names {
		sts := sets[name]
		phase := removalPhase(sts.Annotations[AnnotationRemoval])
		if (phase == phaseEvacuating || phase == phaseRemoved) && rm.Removing == "" {
			rm.Removing, rm.Phase = name, phase
		}
		if phase == phaseRejoining {
			rm.Rejoining = append(rm.Rejoining, name)
		}
		digest, inPlan := want[name]
		switch {
		case !inPlan:
			rm.Surplus = append(rm.Surplus, name)
		case phase == phaseRequested || sts.Annotations[AnnotationVolumeDigest] != digest:
			rm.Replace = append(rm.Replace, name)
		}
	}
	return rm
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
	sts := sets[name]
	if sts != nil && slices.ContainsFunc(plan.Servers, func(s Server) bool { return s.Name == name }) && sts.Annotations[AnnotationRemoval] == "" {
		if err := r.markRemoval(ctx, sts, phaseRequested); err != nil {
			return err
		}
	}
	orig := nc.DeepCopy()
	delete(nc.Annotations, clusterv1beta1.AnnotationReplaceServer)
	if err := r.Client.Patch(ctx, nc, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("clear %s: %w", clusterv1beta1.AnnotationReplaceServer, err)
	}
	return nil
}

// remove carries out step. An evacuation or removal the meta leader
// answers with ErrNotMember has nothing left to do and counts as done; a
// removal refused while another membership change is in flight is retried
// on a later reconcile.
func (r *Reconciler) remove(ctx context.Context, nc *clusterv1beta1.NatsCluster, admin ServerAdmin, noAdmin string, step removalStep, sets map[string]*appsv1.StatefulSet, surplus bool) error {
	sts := sets[step.Server]
	if step.Action == actionDelete {
		return r.deleteServer(ctx, nc, step.Server, sts, surplus)
	}
	if admin == nil {
		return fmt.Errorf("%s %s: %s", step.Action, step.Server, noAdmin)
	}
	switch step.Action {
	case actionEvacuate:
		if err := admin.Evacuate(ctx, step.Server); err != nil && !errors.Is(err, sysobs.ErrNotMember) {
			return fmt.Errorf("evacuate %s: %w", step.Server, err)
		}
		return r.markRemoval(ctx, sts, phaseEvacuating)
	case actionStepDown:
		if err := admin.StepDownMeta(ctx); err != nil {
			return fmt.Errorf("step down meta leader %s: %w", step.Server, err)
		}
		return nil
	case actionRemovePeer:
		err := admin.RemovePeer(ctx, step.Server)
		switch {
		case err == nil, errors.Is(err, sysobs.ErrNotMember):
			return r.markRemoval(ctx, sts, phaseRemoved)
		case errors.Is(err, sysobs.ErrChangeInflight):
			return nil
		default:
			return fmt.Errorf("remove %s from the meta group: %w", step.Server, err)
		}
	}
	return fmt.Errorf("unknown removal action %q", step.Action)
}

// rejoined clears the mark on every server in rejoining.
func (r *Reconciler) rejoined(ctx context.Context, rejoining []string, sets map[string]*appsv1.StatefulSet) error {
	for _, name := range rejoining {
		sts := sets[name]
		orig := sts.DeepCopy()
		delete(sts.Annotations, AnnotationRemoval)
		if err := r.Client.Patch(ctx, sts, client.MergeFrom(orig)); err != nil {
			return fmt.Errorf("clear %s on statefulset %s: %w", AnnotationRemoval, name, err)
		}
	}
	return nil
}

func (r *Reconciler) markRemoval(ctx context.Context, sts *appsv1.StatefulSet, phase removalPhase) error {
	orig := sts.DeepCopy()
	sts.Annotations = merged(sts.Annotations, map[string]string{AnnotationRemoval: string(phase)})
	if err := r.Client.Patch(ctx, sts, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("mark statefulset %s %s: %w", sts.Name, phase, err)
	}
	return nil
}

// deleteServer deletes server's StatefulSet and its data volume claim, and
// for a surplus server its ConfigMap.
func (r *Reconciler) deleteServer(ctx context.Context, nc *clusterv1beta1.NatsCluster, server string, sts *appsv1.StatefulSet, surplus bool) error {
	if sts != nil {
		if err := r.Client.Delete(ctx, sts, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete statefulset %s: %w", server, err)
		}
	}
	objs := []client.Object{&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: nc.Namespace, Name: dataClaimName(server)}}}
	if surplus {
		objs = append(objs, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: nc.Namespace, Name: configMapName(server)}})
	}
	for _, o := range objs {
		if err := r.Client.Delete(ctx, o); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete %s: %w", o.GetName(), err)
		}
	}
	return nil
}

// dataReleased reports whether server's data volume claim from an earlier
// StatefulSet is gone, so that a StatefulSet created now gets a new one.
func (r *Reconciler) dataReleased(ctx context.Context, nc *clusterv1beta1.NatsCluster, server string) (bool, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: nc.Namespace, Name: dataClaimName(server)}, pvc)
	switch {
	case apierrors.IsNotFound(err):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("get pvc %s: %w", dataClaimName(server), err)
	}
	return pvc.DeletionTimestamp.IsZero(), nil
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
