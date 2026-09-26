package natscluster

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// What a rollout's gate waits for, as status.rollout.gate.waitingFor names
// it.
const (
	GateReady          = "Ready"
	GateTargetRevision = "TargetRevision"
	GateSettled        = "Settled"
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
	// Revision is the config revision the server reports, "" when it did
	// not answer.
	Revision string
}

// rolloutState is everything one rollout decision reads. Servers are in
// ordinal order; Verdict is nil when the NATS cluster was not observed.
type rolloutState struct {
	Target     string
	Servers    []rolloutServer
	MetaLeader string
	Verdict    *sysobs.Verdict
	Paused     bool
	ForceStep  string
	Prev       *clusterv1beta1.RolloutStatus
	Now        time.Time
}

// rolloutDecision is what one reconcile does to a rollout.
type rolloutDecision struct {
	// Step is the server to restart now, or "".
	Step string
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

// decide takes one rollout decision. It restarts at most one server: the
// highest ordinal waiting for a restart, the meta leader's server last, and
// only once every server is Ready, every server on the target revision
// reports it, and the NATS cluster is Settled. A closed gate holds with no
// timeout. Paused holds before the next step; a force-step naming a server
// waiting for a restart steps it through a closed gate and through Paused.
func decide(st rolloutState) rolloutDecision {
	d := rolloutDecision{ClearForceStep: st.ForceStep != ""}
	var pending, onTarget []string
	for _, s := range st.Servers {
		switch {
		case s.Restart:
			pending = append(pending, s.Name)
		case s.OnTarget:
			onTarget = append(onTarget, s.Name)
		}
	}
	pending = rolloutOrder(pending, st.MetaLeader)

	gate := judgeGate(st)
	current := ""
	prev := st.Prev
	if prev != nil && prev.TargetRevision != st.Target {
		prev = nil
	}
	if prev != nil && !gate.open() && slices.Contains(onTarget, prev.Current) {
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
	case st.ForceStep != "" && slices.Contains(pending, st.ForceStep):
		d.Step = st.ForceStep
	case gate.open() && !st.Paused && len(pending) > 0:
		d.Step = pending[0]
	}
	if d.Step != "" {
		current = d.Step
		pending = slices.DeleteFunc(pending, func(s string) bool { return s == d.Step })
		gate = gateState{waitingFor: GateSettled, detail: d.Step + " is restarting"}
		since = st.Now
	}

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
	d.Progressing = rolloutCondition(d.Status, gate, st.Paused, st.Now.Sub(since))
	return d
}

// rolloutCondition is the Progressing condition of rollout rs: RollingRestart
// while a server restarts or the gate is closed, GateBlocked once it has
// been closed for gateBlockedAfter, RolloutPaused while paused between
// steps.
func rolloutCondition(rs *clusterv1beta1.RolloutStatus, gate gateState, paused bool, closedFor time.Duration) metav1.Condition {
	c := metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionTrue, Reason: ReasonRollingRestart}
	total := len(rs.Updated) + len(rs.Pending)
	if rs.Current != "" {
		total++
	}
	step := fmt.Sprintf("(%d of %d)", len(rs.Updated)+1, total)
	switch {
	case rs.Current != "":
		c.Message = fmt.Sprintf("restarting %s %s", rs.Current, step)
	case paused:
		c.Reason = ReasonRolloutPaused
		c.Message = fmt.Sprintf("paused before restarting %s %s", rs.Pending[0], step)
		return c
	default:
		c.Message = fmt.Sprintf("before restarting %s %s", rs.Pending[0], step)
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
// NATS cluster not Settled, a server not Ready, a server on the target
// revision not reporting it.
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
	for _, s := range plan.Servers {
		sts := o.StatefulSets[s.Name]
		_, restart := o.Apply.Restart[s.Name]
		st.Servers = append(st.Servers, rolloutServer{
			Name:     s.Name,
			OnTarget: sts != nil && sts.Annotations[AnnotationConfigRevision] == plan.Revision,
			Restart:  restart,
			Ready:    podReady(sts),
			Revision: reported[s.Name],
		})
	}
	return st
}

// rollout takes one rollout decision and carries it out: it restarts the
// server decided on and clears a force-step annotation it read.
func (r *Reconciler) rollout(ctx context.Context, nc *clusterv1beta1.NatsCluster, plan *Plan, o Observed) (rolloutDecision, error) {
	d := decide(r.rolloutState(nc, plan, o))
	if d.Step != "" {
		i := slices.IndexFunc(plan.Servers, func(s Server) bool { return s.Name == d.Step })
		sts, err := r.restartServer(ctx, nc, plan.Servers[i], o.StatefulSets[d.Step], o.Apply.Restart[d.Step])
		if err != nil {
			return d, err
		}
		o.StatefulSets[d.Step] = sts
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
// the revision, so writing it restarts the pod, whose preStop enters lame
// duck mode.
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
	sts.Spec.Template = *s.StatefulSet.Spec.Template.DeepCopy()
	if err := r.Client.Update(ctx, sts); err != nil {
		return nil, fmt.Errorf("restart statefulset %s: %w", sts.Name, err)
	}
	return sts, nil
}
