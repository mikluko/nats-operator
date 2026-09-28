package natscluster

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// What a rollout's gate waits for, as status.rollout.gate.waitingFor names
// it.
const (
	GateReady          = "Ready"
	GateTargetRevision = "TargetRevision"
	GateSettled        = "Settled"
	GateEvacuated      = "Evacuated"
)

// gateBlockedAfter is how long a closed gate reads as RollingRestart before
// it reads as GateBlocked. It only relabels: a closed gate holds whatever
// its age.
const gateBlockedAfter = 10 * time.Minute

// maxNamedGroups bounds how many unsettled groups a GateBlocked message
// names.
const maxNamedGroups = 5

// rolloutServer is one server as the rollout sees it.
type rolloutServer struct {
	Name string
	// OnTarget is true when its StatefulSet is on the target revision.
	OnTarget bool
	// Restart is true when its revision waits for a restart.
	Restart bool
	// Ready is true when its pod is Ready at its StatefulSet's template.
	Ready bool
	// Missing is true when it has no StatefulSet, as a replaced server
	// has between its deletion and its recreation.
	Missing bool
	// Revision is the config revision the server reports, "" when it did
	// not answer.
	Revision string
}

// rolloutState is everything one rollout decision reads. Servers are in
// ordinal order; Verdict is nil when the NATS cluster was not observed.
// NotInMeta are the servers that answered but are not members of the meta
// group, as a server readmitted after a removal is not until its tombstone
// lapses; none are named while the meta group is FromFollowers.
type rolloutState struct {
	Target     string
	Servers    []rolloutServer
	MetaLeader string
	Verdict    *sysobs.Verdict
	NotInMeta  []string
	Removal    removalState
	Paused     bool
	ForceStep  string
	Prev       *clusterv1beta1.RolloutStatus
	Now        time.Time
}

// rolloutDecision is what one reconcile does to a rollout.
type rolloutDecision struct {
	// Step is the server to restart now, or "".
	Step string
	// Remove is the removal action to take now, or nil.
	Remove *removalStep
	// Blocked is the Progressing condition of a scale-down or replacement
	// that cannot start, nil when none is blocked.
	Blocked *metav1.Condition
	// ClearForceStep is true when a force-step annotation was read; it is
	// cleared whether or not it named a server that could be stepped.
	ClearForceStep bool
	// Status is status.rollout, nil when no server waits for a restart.
	Status *clusterv1beta1.RolloutStatus
	// Progressing is the Progressing condition while Status is not nil.
	Progressing metav1.Condition
}

// gateState is a closed gate's condition and what holds it, or the zero
// value for an open gate.
type gateState struct {
	waitingFor string
	detail     string
}

func (g gateState) open() bool { return g.waitingFor == "" }

// decide takes one rollout decision, of at most one step.
func decide(st rolloutState) rolloutDecision {
	d := rolloutDecision{ClearForceStep: st.ForceStep != ""}
	rm := st.Removal
	var restarts, onTarget, missing []string
	for _, s := range st.Servers {
		switch {
		case s.Name == rm.Removing || slices.Contains(rm.Replace, s.Name):
		case s.Restart:
			restarts = append(restarts, s.Name)
		case s.OnTarget:
			onTarget = append(onTarget, s.Name)
		case s.Missing:
			missing = append(missing, s.Name)
		}
	}
	notRemoving := func(s string) bool { return s == rm.Removing }
	surplus := slices.DeleteFunc(slices.Clone(rm.Surplus), notRemoving)
	slices.Reverse(surplus)
	replace := rolloutOrder(slices.DeleteFunc(slices.Clone(rm.Replace), notRemoving), st.MetaLeader)
	if why := scaleDownBlocked(rm); why != "" && len(surplus) > 0 {
		d.Blocked = &metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: ReasonScaleDownBlocked,
			Message: fmt.Sprintf("cannot remove %s: %s", strings.Join(surplus, ", "), why)}
		surplus = nil
	}
	if rm.JetStream && rm.NoAdmin != "" && len(replace) > 0 {
		d.Blocked = &metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: ReasonReplacementBlocked,
			Message: fmt.Sprintf("cannot replace %s: cannot evacuate servers: %s", strings.Join(replace, ", "), rm.NoAdmin)}
		replace = nil
	}
	pending := slices.Concat(surplus, rolloutOrder(restarts, st.MetaLeader), replace)

	gate := judgeGate(st)
	current := ""
	prev := st.Prev
	if prev != nil && prev.TargetRevision != st.Target {
		prev = nil
	}
	switch {
	case rm.Removing != "":
		current = rm.Removing
	case prev != nil && !gate.open() && (slices.Contains(onTarget, prev.Current) || slices.Contains(missing, prev.Current)):
		current = prev.Current
	}
	if len(pending) == 0 && current == "" {
		return d
	}

	since := st.Now
	if prev != nil && prev.Gate != nil && prev.Gate.Since != nil {
		since = prev.Gate.Since.Time
	}
	switch {
	case rm.Removing != "":
		d.Remove, gate = continueRemoval(rm, st.MetaLeader)
		switch {
		case !rm.Since.IsZero():
			since = rm.Since
		case d.Remove != nil:
			since = st.Now
		}
	case st.ForceStep != "" && slices.Contains(restarts, st.ForceStep):
		d.Step = st.ForceStep
	case gate.open() && !st.Paused && len(pending) > 0:
		if next := pending[0]; st.kindOf(next) == kindRestart {
			d.Step = next
		} else {
			step := startRemoval(rm, next)
			d.Remove = &step
			current, gate, since = next, removalGate(step), st.Now
		}
	}
	if d.Step != "" {
		current = d.Step
		gate = gateState{waitingFor: GateSettled, detail: d.Step + " is restarting"}
		since = st.Now
	}
	pending = slices.DeleteFunc(pending, func(s string) bool { return s == current })

	updated := slices.DeleteFunc(slices.Clone(onTarget), func(s string) bool { return s == current })
	d.Status = &clusterv1beta1.RolloutStatus{
		TargetRevision: st.Target,
		Updated:        updated,
		Current:        current,
		Pending:        pending,
	}
	if !gate.open() {
		d.Status.Gate = &clusterv1beta1.RolloutGate{WaitingFor: gate.waitingFor, Since: &metav1.Time{Time: since}}
	}
	d.Progressing = rolloutCondition(d.Status, gate, st.Paused, st.Now.Sub(since), st.kindOf)
	return d
}

// rolloutCondition is the Progressing condition of rollout rs: while a
// server is being worked on or the gate is closed, RollingRestart,
// ScalingDown or ReplacingServer after what that server's step does;
// GateBlocked once the gate has been closed for gateBlockedAfter;
// RolloutPaused while paused between steps.
func rolloutCondition(rs *clusterv1beta1.RolloutStatus, gate gateState, paused bool, closedFor time.Duration, kindOf func(string) stepKind) metav1.Condition {
	c := metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionTrue}
	total := len(rs.Updated) + len(rs.Pending)
	if rs.Current != "" {
		total++
	}
	step := fmt.Sprintf("(%d of %d)", len(rs.Updated)+1, total)
	server := rs.Current
	if server == "" {
		server = rs.Pending[0]
	}
	var verb string
	switch kindOf(server) {
	case kindScaleDown:
		c.Reason, verb = ReasonScalingDown, "removing"
	case kindReplace:
		c.Reason, verb = ReasonReplacingServer, "replacing"
	default:
		c.Reason, verb = ReasonRollingRestart, "restarting"
	}
	switch {
	case rs.Current != "":
		c.Message = fmt.Sprintf("%s %s %s", verb, server, step)
	case paused:
		c.Reason = ReasonRolloutPaused
		c.Message = fmt.Sprintf("paused before %s %s %s", verb, server, step)
		return c
	default:
		c.Message = fmt.Sprintf("before %s %s %s", verb, server, step)
	}
	if gate.open() {
		return c
	}
	c.Message += "; waiting for " + gate.waitingFor
	if closedFor >= gateBlockedAfter {
		c.Reason = ReasonGateBlocked
		c.Message += fmt.Sprintf(" for %s: %s", closedFor.Round(time.Second), gate.detail)
	}
	return c
}

// judgeGate reports what holds the gate to the next step, in order: the
// NATS cluster not Settled, a server outside the meta group, a server not
// Ready, a server on the target revision not reporting it.
func judgeGate(st rolloutState) gateState {
	var notReady, behind []string
	for _, s := range st.Servers {
		if !s.Ready {
			notReady = append(notReady, s.Name)
		}
		if s.OnTarget && s.Revision != st.Target {
			behind = append(behind, s.Name)
		}
	}
	switch {
	case st.Verdict == nil:
		return gateState{GateSettled, "the NATS cluster is not observed"}
	case !st.Verdict.Settled():
		return gateState{GateSettled, describeUnsettled(*st.Verdict)}
	case len(st.NotInMeta) > 0:
		return gateState{GateSettled, strings.Join(st.NotInMeta, ", ") + " not in the meta group"}
	case len(notReady) > 0:
		return gateState{GateReady, strings.Join(notReady, ", ") + " not ready"}
	case len(behind) > 0:
		return gateState{GateTargetRevision, fmt.Sprintf("%s not reporting revision %s", strings.Join(behind, ", "), st.Target)}
	}
	return gateState{}
}

// describeUnsettled names the servers that did not answer, or else up to
// maxNamedGroups unsettled groups with why and on which servers.
func describeUnsettled(v sysobs.Verdict) string {
	if len(v.Silent) > 0 {
		return strings.Join(v.Silent, ", ") + " did not answer"
	}
	var out []string
	for _, u := range v.Unsettled[:min(len(v.Unsettled), maxNamedGroups)] {
		s := groupName(u.Group) + " " + string(u.Reason)
		if len(u.Servers) > 0 {
			s += " on " + strings.Join(u.Servers, ", ")
		}
		out = append(out, s)
	}
	if n := len(v.Unsettled) - maxNamedGroups; n > 0 {
		out = append(out, fmt.Sprintf("and %d more", n))
	}
	return strings.Join(out, "; ")
}

func groupName(g sysobs.Group) string {
	switch g.Kind {
	case sysobs.KindMeta:
		return "meta"
	case sysobs.KindConsumer:
		return "consumer " + g.Stream + "/" + g.Consumer
	default:
		return "stream " + g.Stream
	}
}

// rolloutOrder orders servers, given in ordinal order, highest ordinal
// first with metaLeader last.
func rolloutOrder(servers []string, metaLeader string) []string {
	out := slices.Clone(servers)
	slices.Reverse(out)
	if i := slices.Index(out, metaLeader); i >= 0 {
		out = append(slices.Delete(out, i, i+1), metaLeader)
	}
	return out
}

// podReady reports whether sts's pod is Ready at sts's current template.
func podReady(sts *appsv1.StatefulSet) bool {
	return sts != nil && sts.Status.ObservedGeneration >= sts.Generation &&
		sts.Status.UpdatedReplicas > 0 && sts.Status.ReadyReplicas > 0
}

// rolloutState collects what decide reads from one reconcile.
func (r *Reconciler) rolloutState(nc *clusterv1beta1.NatsCluster, plan *Plan, o Observed) rolloutState {
	st := rolloutState{
		Target:    plan.Revision,
		ForceStep: nc.Annotations[clusterv1beta1.AnnotationForceStep],
		Prev:      nc.Status.Rollout,
		Now:       r.now(),
	}
	if ro := nc.Spec.Rollout; ro != nil {
		st.Paused = ro.Paused
	}
	reported := map[string]string{}
	if snap := o.Snapshot; snap != nil {
		v := snap.Verdict()
		st.Verdict = &v
		st.MetaLeader = metaLeader(snap)
		for _, s := range snap.Servers {
			if !slices.Contains(snap.Silent, s.Name) {
				reported[s.Name] = s.Metadata[MetadataConfigRevision]
			}
		}
	}
	st.Removal = removalOf(nc, plan, o.StatefulSets, o.Snapshot)
	if snap := o.Snapshot; snap != nil && st.Removal.JetStream && st.MetaLeader != "" {
		for _, s := range plan.Servers {
			if _, ok := reported[s.Name]; !ok {
				continue
			}
			if member, known := metaMember(snap, s.Name); known && !member {
				st.NotInMeta = append(st.NotInMeta, s.Name)
			}
		}
	}
	for _, s := range plan.Servers {
		sts := o.StatefulSets[s.Name]
		_, restart := o.Apply.Restart[s.Name]
		st.Servers = append(st.Servers, rolloutServer{
			Name:     s.Name,
			OnTarget: sts != nil && sts.Annotations[AnnotationConfigRevision] == plan.Revision,
			Restart:  restart,
			Ready:    podReady(sts),
			Missing:  sts == nil,
			Revision: reported[s.Name],
		})
	}
	return st
}

// rollout takes one rollout decision and carries it out, clearing the
// force-step and replace-server annotations it read.
func (r *Reconciler) rollout(ctx context.Context, nc *clusterv1beta1.NatsCluster, plan *Plan, o Observed) (rolloutDecision, error) {
	if err := r.requestReplacement(ctx, nc, plan, o.StatefulSets); err != nil {
		return rolloutDecision{}, err
	}
	admin, noAdmin := r.admin(ctx, nc)
	st := r.rolloutState(nc, plan, o)
	st.Removal.NoAdmin = noAdmin
	d := decide(st)
	if judgeGate(st).open() {
		if err := r.rejoined(ctx, nc, st.Removal.Rejoining); err != nil {
			return d, err
		}
	}
	if d.Remove != nil {
		surplus := st.kindOf(d.Remove.Server) == kindScaleDown
		if st.Removal.Removing == "" {
			verb := "replacing"
			if surplus {
				verb = "removing"
			}
			telemetry.Emit(r.Recorder, nc, telemetry.RolloutStep, "%s %s", verb, d.Remove.Server)
		}
		if err := r.remove(ctx, nc, admin, noAdmin, *d.Remove, o.StatefulSets, surplus); err != nil {
			return d, err
		}
		if d.Remove.Action == actionDelete {
			delete(o.StatefulSets, d.Remove.Server)
		}
	}
	if d.Step != "" {
		i := slices.IndexFunc(plan.Servers, func(s Server) bool { return s.Name == d.Step })
		sts, err := r.restartServer(ctx, nc, plan.Servers[i], o.StatefulSets[d.Step], o.Apply.Restart[d.Step])
		if err != nil {
			return d, err
		}
		o.StatefulSets[d.Step] = sts
		telemetry.Emit(r.Recorder, nc, telemetry.RolloutStep, "restarting %s", d.Step)
	}
	if d.ClearForceStep {
		orig := nc.DeepCopy()
		delete(nc.Annotations, clusterv1beta1.AnnotationForceStep)
		if err := r.Client.Patch(ctx, nc, client.MergeFrom(orig)); err != nil {
			return d, fmt.Errorf("clear %s: %w", clusterv1beta1.AnnotationForceStep, err)
		}
	}
	return d, nil
}

// restartServer writes server s's rendered ConfigMap, marked for a restart
// for reason, then its rendered pod template into cur. The template names
// the revision, so writing it restarts the pod.
func (r *Reconciler) restartServer(ctx context.Context, nc *clusterv1beta1.NatsCluster, s Server, cur *appsv1.StatefulSet, reason string) (*appsv1.StatefulSet, error) {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: s.ConfigMap.Name, Namespace: s.ConfigMap.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = merged(cm.Labels, s.ConfigMap.Labels)
		cm.Data = s.ConfigMap.Data
		cm.Annotations = merged(cm.Annotations, s.ConfigMap.Annotations)
		cm.Annotations[AnnotationConfigApply] = string(clusterv1beta1.ConfigAppliedByRestart)
		cm.Annotations[AnnotationRestartReason] = reason
		delete(cm.Annotations, AnnotationReloadSince)
		return controllerutil.SetControllerReference(nc, cm, r.Client.Scheme())
	}); err != nil {
		return nil, fmt.Errorf("write configmap %s for restart: %w", cm.Name, err)
	}
	sts := cur.DeepCopy()
	sts.Labels = merged(sts.Labels, s.StatefulSet.Labels)
	sts.Annotations = merged(sts.Annotations, s.StatefulSet.Annotations)
	sts.Annotations[AnnotationVolumeDigest] = cur.Annotations[AnnotationVolumeDigest]
	sts.Spec.Template = *s.StatefulSet.Spec.Template.DeepCopy()
	if err := r.Client.Update(ctx, sts); err != nil {
		return nil, fmt.Errorf("restart statefulset %s: %w", sts.Name, err)
	}
	return sts, nil
}

// recordGateBlocked records GateBlocked on nc when its Progressing reads
// GateBlocked and before, the conditions it had, did not.
func recordGateBlocked(rec events.EventRecorder, nc *clusterv1beta1.NatsCluster, before []metav1.Condition) {
	now := meta.FindStatusCondition(nc.Status.Conditions, ConditionProgressing)
	if now == nil || now.Reason != ReasonGateBlocked {
		return
	}
	if was := meta.FindStatusCondition(before, ConditionProgressing); was != nil && was.Reason == ReasonGateBlocked {
		return
	}
	telemetry.Emit(rec, nc, telemetry.GateBlocked, "%s", now.Message)
}
