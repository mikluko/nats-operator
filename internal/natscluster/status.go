package natscluster

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// Condition types a NatsCluster's status reports.
const (
	ConditionReady       = "Ready"
	ConditionSettled     = "Settled"
	ConditionProgressing = "Progressing"
	// ConditionGatewaysConnected is set only on a NatsCluster with a
	// gateway.
	ConditionGatewaysConnected = "GatewaysConnected"
)

// Reasons of the Ready, Settled, GatewaysConnected and Progressing
// conditions.
const (
	ReasonAllServersReady   = "AllServersReady"
	ReasonQuorumAvailable   = "QuorumAvailable"
	ReasonQuorumUnavailable = "QuorumUnavailable"

	ReasonAllGroupsCurrent  = "AllGroupsCurrent"
	ReasonServersSilent     = "ServersSilent"
	ReasonGroupsLeaderless  = "GroupsLeaderless"
	ReasonMembersOffline    = "MembersOffline"
	ReasonGroupsCatchingUp  = "GroupsCatchingUp"
	ReasonObservationFailed = "ObservationFailed"

	ReasonAllMembersReachable = "AllMembersReachable"
	ReasonMembersUnreachable  = "MembersUnreachable"

	ReasonUpToDate            = "UpToDate"
	ReasonCreating            = "Creating"
	ReasonClaimTerminating    = "ClaimTerminating"
	ReasonRollingRestart      = "RollingRestart"
	ReasonGateBlocked         = "GateBlocked"
	ReasonRolloutPaused       = "RolloutPaused"
	ReasonReloadPending       = "ReloadPending"
	ReasonScaleDownPending    = "ScaleDownPending"
	ReasonScalingDown         = "ScalingDown"
	ReasonReplacingServer     = "ReplacingServer"
	ReasonScaleDownBlocked    = "ScaleDownBlocked"
	ReasonReplacementBlocked  = "ReplacementBlocked"
	ReasonUnsupportedSpec     = "UnsupportedSpec"
	ReasonReconcileFailed     = "ReconcileFailed"
	ReasonClientCertNotReady  = "ClientCertificateNotReady"
	ReasonRouteCertNotReady   = "RouteCertificateNotReady"
	ReasonGatewayCertNotReady = "GatewayCertificateNotReady"
	ReasonTrustNotFound       = "TrustNotFound"
	ReasonTrustNotReady       = "TrustNotReady"
	ReasonTrustInvalid        = "TrustInvalid"
)

// Observed is what one reconcile saw and did.
type Observed struct {
	// Held is the Progressing condition of a spec, trust or leaf remote
	// that nothing is rendered past, nil when none holds the reconcile.
	Held *metav1.Condition
	// CertWait says which certificate the servers wait for, with
	// CertReason, "" when none.
	CertWait, CertReason string
	StatefulSets         map[string]*appsv1.StatefulSet
	Created              []string
	// ClaimTerminating are the servers not created because their data
	// volume claim is being deleted.
	ClaimTerminating []string
	Snapshot         *sysobs.Snapshot
	ObserveErr       error
	Apply            configApply
	Rollout          rolloutDecision
}

// computeStatus returns nc's status from plan and what was observed,
// starting from nc's current status so that what was not observed this
// time keeps its last value.
func computeStatus(nc *clusterv1beta1.NatsCluster, plan *Plan, o Observed) clusterv1beta1.NatsClusterStatus {
	st := *nc.Status.DeepCopy()
	gen := nc.Generation
	st.ObservedGeneration = gen
	st.Replicas = nc.Spec.Replicas
	st.Endpoints = &clusterv1beta1.Endpoints{
		Client:  clientURL(nc),
		Monitor: fmt.Sprintf("http://%s.%s.svc:%d", headlessServiceName(nc), nc.Namespace, PortMonitor),
	}
	st.Config = configStatus(nc.Status.Config, plan, o.Apply)
	st.Rollout = o.Rollout.Status

	reported := map[string]sysobs.Server{}
	if o.Snapshot != nil {
		silent := map[string]bool{}
		for _, s := range o.Snapshot.Silent {
			silent[s] = true
		}
		for _, s := range o.Snapshot.Servers {
			if !silent[s.Name] {
				reported[s.Name] = s
			}
		}
	}

	st.Servers = nil
	st.ReadyReplicas = 0
	versions := map[string]bool{}
	for _, s := range plan.Servers {
		ss := clusterv1beta1.ServerStatus{Name: s.Name}
		if sts := o.StatefulSets[s.Name]; sts != nil && sts.Status.ReadyReplicas > 0 {
			ss.Ready = true
			st.ReadyReplicas++
		}
		if r, ok := reported[s.Name]; ok {
			ss.Version = r.Version
			ss.ConfigRevision = r.Metadata[MetadataConfigRevision]
			versions[r.Version] = true
		}
		st.Servers = append(st.Servers, ss)
	}
	if len(versions) == 1 && len(reported) >= len(plan.Servers) {
		for v := range versions {
			st.Version = v
		}
	}

	if nc.Spec.JetStream != nil {
		if st.JetStream == nil {
			st.JetStream = &clusterv1beta1.JetStreamStatus{}
		}
		limits := plan.Limits.JetStream
		st.JetStream.Limits = &limits
		if o.Snapshot != nil {
			st.JetStream.MetaLeader = metaLeader(o.Snapshot)
		}
	} else {
		st.JetStream = nil
	}

	if g := nc.Spec.Gateway; g != nil {
		st.Endpoints.Gateway = g.Advertise
		if o.Snapshot != nil {
			st.Gateways = gatewayStatus(nc, o.Snapshot)
		}
		conditions.Set(&st.Conditions, gen, gatewaysCondition(st.Gateways, o))
	} else {
		st.Gateways = nil
		meta.RemoveStatusCondition(&st.Conditions, ConditionGatewaysConnected)
	}

	conditions.Set(&st.Conditions, gen, readyCondition(st.ReadyReplicas, nc.Spec.Replicas))
	conditions.Set(&st.Conditions, gen, settledCondition(o))
	conditions.Set(&st.Conditions, gen, progressingCondition(nc, plan, o))
	return st
}

// gatewayStatus reports every other member of the supercluster: those
// gateway.remotes names and those a server holds a connection with, sorted
// by name. Outbound counts the servers holding an outbound connection to a
// member, Inbound the inbound connections from it across servers; a member
// is Connected when every server that answered GATEWAYZ dials it.
func gatewayStatus(nc *clusterv1beta1.NatsCluster, snap *sysobs.Snapshot) []clusterv1beta1.GatewayStatus {
	byName := map[string]*clusterv1beta1.GatewayStatus{}
	member := func(name string) *clusterv1beta1.GatewayStatus {
		if byName[name] == nil {
			byName[name] = &clusterv1beta1.GatewayStatus{Name: name}
		}
		return byName[name]
	}
	for _, r := range nc.Spec.Gateway.Remotes {
		if r.Name != nc.Name {
			member(r.Name)
		}
	}
	reporting := int32(0)
	for _, s := range snap.Servers {
		if s.Gateways == nil {
			continue
		}
		reporting++
		for _, name := range s.Gateways.Outbound {
			if name != nc.Name {
				member(name).Outbound++
			}
		}
		for name, n := range s.Gateways.Inbound {
			if name != nc.Name {
				member(name).Inbound += int32(n)
			}
		}
	}
	out := make([]clusterv1beta1.GatewayStatus, 0, len(byName))
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		g := byName[name]
		g.Connected = reporting > 0 && g.Outbound == reporting
		out = append(out, *g)
	}
	return out
}

// gatewaysCondition is True when every member in gateways is connected,
// and Unknown when the NATS cluster was not observed.
func gatewaysCondition(gateways []clusterv1beta1.GatewayStatus, o Observed) metav1.Condition {
	c := metav1.Condition{Type: ConditionGatewaysConnected}
	if o.Snapshot == nil {
		c.Status, c.Reason = metav1.ConditionUnknown, ReasonObservationFailed
		if o.ObserveErr != nil {
			c.Message = o.ObserveErr.Error()
		}
		return c
	}
	var down []string
	for _, g := range gateways {
		if !g.Connected {
			down = append(down, g.Name)
		}
	}
	c.Message = fmt.Sprintf("%d of %d remote members connected", len(gateways)-len(down), len(gateways))
	if len(down) > 0 {
		c.Status, c.Reason = metav1.ConditionFalse, ReasonMembersUnreachable
		c.Message += ": " + strings.Join(down, ", ") + " unreachable"
		return c
	}
	c.Status, c.Reason = metav1.ConditionTrue, ReasonAllMembersReachable
	return c
}

// configStatus reports how plan's revision is applied: by restart when any
// server needs one, naming each distinct reason; by reload while servers
// reload to it; otherwise as prev reported that revision, or by restart.
func configStatus(prev *clusterv1beta1.ConfigStatus, plan *Plan, a configApply) *clusterv1beta1.ConfigStatus {
	cs := &clusterv1beta1.ConfigStatus{Revision: plan.Revision, AppliedBy: clusterv1beta1.ConfigAppliedByRestart}
	switch {
	case len(a.Restart) > 0:
		var reasons []string
		for _, s := range plan.Servers {
			if r, ok := a.Restart[s.Name]; ok && r != "" && !slices.Contains(reasons, r) {
				reasons = append(reasons, r)
			}
		}
		cs.RestartReason = strings.Join(reasons, "; ")
	case len(a.Reloading) > 0 || len(a.Reloaded) > 0:
		cs.AppliedBy = clusterv1beta1.ConfigAppliedByReload
	case prev != nil && prev.Revision == plan.Revision:
		cs.AppliedBy, cs.RestartReason = prev.AppliedBy, prev.RestartReason
	}
	return cs
}

func metaLeader(s *sysobs.Snapshot) string {
	for _, g := range s.Groups {
		if g.Kind == sysobs.KindMeta {
			return g.Leader
		}
	}
	return ""
}

// readyCondition is True while a majority of the servers are Ready, with
// reason AllServersReady once all are.
func readyCondition(ready, want int32) metav1.Condition {
	msg := fmt.Sprintf("%d of %d servers ready", ready, want)
	switch {
	case ready >= want:
		return metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonAllServersReady, Message: msg}
	case ready > want/2:
		return metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonQuorumAvailable, Message: msg}
	default:
		return metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonQuorumUnavailable, Message: msg}
	}
}

// settledCondition judges the observation: Unknown when there is none,
// False with the most severe reason when anything is unsettled, a silent
// server first.
func settledCondition(o Observed) metav1.Condition {
	c := metav1.Condition{Type: ConditionSettled}
	if o.Snapshot == nil {
		c.Status, c.Reason = metav1.ConditionUnknown, ReasonObservationFailed
		if o.ObserveErr != nil {
			c.Message = o.ObserveErr.Error()
		}
		return c
	}
	v := o.Snapshot.Verdict()
	if v.Settled() {
		c.Status, c.Reason = metav1.ConditionTrue, ReasonAllGroupsCurrent
		c.Message = "every Raft group has a leader and every member is current"
		return c
	}
	c.Status = metav1.ConditionFalse
	if len(v.Silent) > 0 {
		c.Reason = ReasonServersSilent
		c.Message = fmt.Sprintf("%s did not answer", strings.Join(v.Silent, ", "))
		return c
	}
	byReason := map[sysobs.Reason][]sysobs.Unsettled{}
	for _, u := range v.Unsettled {
		byReason[u.Reason] = append(byReason[u.Reason], u)
	}
	switch {
	case len(byReason[sysobs.ReasonNoLeader]) > 0:
		c.Reason = ReasonGroupsLeaderless
		c.Message = fmt.Sprintf("%d Raft groups have no leader", len(byReason[sysobs.ReasonNoLeader]))
	case len(byReason[sysobs.ReasonMemberOffline]) > 0:
		us := byReason[sysobs.ReasonMemberOffline]
		c.Reason = ReasonMembersOffline
		c.Message = fmt.Sprintf("%d Raft groups have a member offline on %s", len(us), strings.Join(serversOf(us), ", "))
	default:
		us := byReason[sysobs.ReasonMemberBehind]
		c.Reason = ReasonGroupsCatchingUp
		c.Message = fmt.Sprintf("%d Raft groups have a member on %s that is not current", len(us), strings.Join(serversOf(us), ", "))
	}
	return c
}

func serversOf(us []sysobs.Unsettled) []string {
	var out []string
	for _, u := range us {
		out = append(out, u.Servers...)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// progressingCondition is the highest-ranked Progressing reason; plan is read
// only when o.Held is nil.
func progressingCondition(nc *clusterv1beta1.NatsCluster, plan *Plan, o Observed) metav1.Condition {
	if o.Held != nil {
		return *o.Held
	}
	c := metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionTrue}
	if o.CertWait != "" {
		c.Reason, c.Message = o.CertReason, o.CertWait
		return c
	}
	if len(o.Created) > 0 {
		c.Reason = ReasonCreating
		c.Message = "creating " + strings.Join(o.Created, ", ")
		return c
	}
	if len(o.ClaimTerminating) > 0 {
		var claims []string
		for _, s := range o.ClaimTerminating {
			claims = append(claims, dataClaimName(s))
		}
		c.Reason = ReasonClaimTerminating
		c.Message = "waiting for the deletion of " + strings.Join(claims, ", ")
		return c
	}
	if o.Rollout.Status != nil {
		return o.Rollout.Progressing
	}
	var stale []string
	for _, s := range plan.Servers {
		if sts := o.StatefulSets[s.Name]; sts != nil && sts.Annotations[AnnotationConfigRevision] != plan.Revision {
			stale = append(stale, s.Name)
		}
	}
	if len(stale) > 0 {
		c.Reason = ReasonReloadPending
		c.Message = fmt.Sprintf("reloading %s to revision %s", strings.Join(stale, ", "), plan.Revision)
		return c
	}
	if o.Rollout.Blocked != nil {
		return *o.Rollout.Blocked
	}
	var extra []string
	for name := range o.StatefulSets {
		if !slices.ContainsFunc(plan.Servers, func(s Server) bool { return s.Name == name }) {
			extra = append(extra, name)
		}
	}
	if len(extra) > 0 {
		slices.Sort(extra)
		c.Reason = ReasonScaleDownPending
		c.Message = fmt.Sprintf("%s beyond %d replicas", strings.Join(extra, ", "), nc.Spec.Replicas)
		return c
	}
	c.Status, c.Reason = metav1.ConditionFalse, ReasonUpToDate
	return c
}
