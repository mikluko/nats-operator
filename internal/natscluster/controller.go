// Package natscluster reconciles NatsCluster.
package natscluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/sysobs"
	"github.com/mikluko/nats-operator/internal/telemetry"
)

// Observer observes the NATS cluster a NatsCluster deployed: its Raft
// groups, and its leafnode connections by server name.
type Observer interface {
	Observe(ctx context.Context, nc *clusterv1beta1.NatsCluster) (*sysobs.Snapshot, error)
	ObserveLeafs(ctx context.Context, nc *clusterv1beta1.NatsCluster) (map[string][]sysobs.Leaf, error)
}

// PodMonitor is the Observer of a NatsCluster through the HTTP monitoring
// port of each pod spec.replicas names, addressed under the headless
// Service. It is how a NATS cluster without an auth plane is observed: its
// system account has no user to connect as.
type PodMonitor struct {
	Monitor *sysobs.MonitorObserver
}

// Observe reads every server's /varz and /jsz.
func (m PodMonitor) Observe(ctx context.Context, nc *clusterv1beta1.NatsCluster) (*sysobs.Snapshot, error) {
	return m.Monitor.Observe(ctx, monitorEndpoints(nc))
}

// ObserveLeafs reads every server's /leafz.
func (m PodMonitor) ObserveLeafs(ctx context.Context, nc *clusterv1beta1.NatsCluster) (map[string][]sysobs.Leaf, error) {
	return m.Monitor.Leafz(ctx, monitorEndpoints(nc))
}

func monitorEndpoints(nc *clusterv1beta1.NatsCluster) []sysobs.Endpoint {
	var eps []sysobs.Endpoint
	for _, s := range serverNames(nc) {
		eps = append(eps, sysobs.Endpoint{Name: s, URL: fmt.Sprintf("http://%s:%d", podHost(nc, s), PortMonitor)})
	}
	return eps
}

// Requeue intervals: a NATS cluster's state is observed rather than
// watched, so every reconcile schedules the next.
const (
	resyncSettled   = time.Minute
	resyncUnsettled = 10 * time.Second
)

// Reconciler reconciles NatsCluster objects.
type Reconciler struct {
	Client   client.Client
	Observer Observer
	// Reloader reaches a NATS cluster's system account to reload its
	// servers; nil restarts every config change.
	Reloader ReloaderFunc
	// Admin reaches a NATS cluster's system account to evacuate and
	// remove servers; nil blocks scale-down and replacement of servers
	// running JetStream.
	Admin AdminFunc
	// Forget, when set, is called with the key of a NatsCluster that is
	// gone or being deleted.
	Forget func(types.NamespacedName)
	Now    func() time.Time
	// Recorder records the rollout's events; nil records none.
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=cluster.nats.mikluko.io,resources=natsclusters,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=cluster.nats.mikluko.io,resources=natsclusters/status,verbs=patch
// +kubebuilder:rbac:groups=cluster.nats.mikluko.io,resources=natsclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;secrets,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;create;update;delete
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsoperatortrusts,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsreferencegrants,verbs=list;watch

// TrustField is the field index SetupWithManager registers on NatsClusters:
// the namespace/name of the NatsOperatorTrust auth.trustRef names.
const TrustField = "cluster.nats.mikluko.io/trust"

// SetupWithManager registers r and its field indexes with mgr.
func (r *Reconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	idx := mgr.GetFieldIndexer()
	if err := idx.IndexField(ctx, &clusterv1beta1.NatsCluster{}, TrustField, func(o client.Object) []string {
		if key := trustKey(o.(*clusterv1beta1.NatsCluster)); key != "" {
			return []string{key}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("index NatsCluster trust: %w", err)
	}
	if err := grant.IndexReferrers(ctx, idx, &clusterv1beta1.NatsCluster{}, func(o client.Object) []string {
		nc := o.(*clusterv1beta1.NatsCluster)
		out := leafRefNamespaces(nc)
		if a := nc.Spec.Auth; a != nil {
			out = append(out, a.TrustRef.Namespace)
		}
		return out
	}); err != nil {
		return fmt.Errorf("index NatsCluster grants: %w", err)
	}
	if err := indexLeafRefs(ctx, idx); err != nil {
		return err
	}
	return r.watchLeafRefs(ctrl.NewControllerManagedBy(mgr)).
		For(&clusterv1beta1.NatsCluster{}).
		Watches(&natsv1beta1.NatsOperatorTrust{}, handler.EnqueueRequestsFromMapFunc(r.clustersTrusting)).
		Watches(&natsv1beta1.NatsReferenceGrant{}, grant.EnqueueReferrers(mgr.GetClient(),
			schema.GroupKind{Group: clusterv1beta1.GroupVersion.Group, Kind: "NatsCluster"}, &clusterv1beta1.NatsClusterList{})).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("natscluster").
		Complete(telemetry.Traced("NatsCluster", r))
}

// trustKey is the namespace/name of the NatsOperatorTrust nc reads, or "".
func trustKey(nc *clusterv1beta1.NatsCluster) string {
	if nc.Spec.Auth == nil {
		return ""
	}
	ns := nc.Spec.Auth.TrustRef.Namespace
	if ns == "" {
		ns = nc.Namespace
	}
	return ns + "/" + nc.Spec.Auth.TrustRef.Name
}

// clustersTrusting maps a NatsOperatorTrust to the NatsClusters that read
// it.
func (r *Reconciler) clustersTrusting(ctx context.Context, t client.Object) []reconcile.Request {
	return r.clustersByField(ctx, TrustField, t.GetNamespace()+"/"+t.GetName())
}

// Reconcile creates what a NatsCluster renders and reports its status.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	nc := &clusterv1beta1.NatsCluster{}
	if err := r.Client.Get(ctx, req.NamespacedName, nc); err != nil {
		if apierrors.IsNotFound(err) && r.Forget != nil {
			r.Forget(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !nc.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, nc)
	}
	if err := r.guardDeletion(ctx, nc); err != nil {
		return ctrl.Result{}, err
	}
	orig := nc.DeepCopy()

	if fields := unsupportedFields(&nc.Spec); len(fields) > 0 {
		return ctrl.Result{}, r.hold(ctx, orig, nc, unsupportedSpec("not rendered by this cluster controller: "+strings.Join(fields, ", ")))
	}
	trust, cond, err := readTrust(ctx, r.Client, nc)
	if err != nil {
		return ctrl.Result{}, err
	}
	if cond != nil {
		return ctrl.Result{RequeueAfter: resyncUnsettled}, r.hold(ctx, orig, nc, cond)
	}
	remotes, cond, err := readLeafRemotes(ctx, r.Client, nc, trust)
	if err != nil {
		return ctrl.Result{}, err
	}
	if cond != nil {
		return ctrl.Result{RequeueAfter: resyncUnsettled}, r.hold(ctx, orig, nc, cond)
	}
	certs, certWait, certReason, err := r.ensureCerts(ctx, nc)
	if err != nil {
		return ctrl.Result{}, err
	}
	plan, err := Render(nc, Inputs{Trust: trust, GatewayCA: certs.Gateway.CA, Certs: certs}, remotes...)
	if err != nil {
		return ctrl.Result{}, r.hold(ctx, orig, nc, unsupportedSpec(err.Error()))
	}

	if err := r.applyShared(ctx, nc, plan); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.applyLeafnodes(ctx, nc, plan); err != nil {
		return ctrl.Result{}, err
	}

	stsByName, err := r.statefulSets(ctx, nc)
	if err != nil {
		return ctrl.Result{}, err
	}
	obs := Observed{StatefulSets: stsByName, CertWait: certWait, CertReason: certReason}
	if certWait == "" {
		for _, s := range plan.Servers {
			if stsByName[s.Name] != nil {
				continue
			}
			terminating, err := r.claimTerminating(ctx, nc, s.Name)
			if err != nil {
				return ctrl.Result{}, err
			}
			if terminating {
				obs.ClaimTerminating = append(obs.ClaimTerminating, s.Name)
				continue
			}
			if ro := nc.Status.Rollout; ro != nil && ro.Current == s.Name {
				s.StatefulSet = s.StatefulSet.DeepCopy()
				s.StatefulSet.Annotations[AnnotationRemoval] = string(phaseRejoining)
			}
			sts, err := r.createServer(ctx, nc, s)
			if err != nil {
				return ctrl.Result{}, err
			}
			stsByName[s.Name] = sts
			obs.Created = append(obs.Created, s.Name)
		}
	}
	obs.Snapshot, obs.ObserveErr = r.Observer.Observe(ctx, nc)
	if certWait == "" {
		if obs.Apply, err = r.applyConfig(ctx, nc, plan, stsByName, obs.Snapshot); err != nil {
			return ctrl.Result{}, err
		}
		if obs.Rollout, err = r.rollout(ctx, nc, plan, obs); err != nil {
			return ctrl.Result{}, err
		}
	}

	nc.Status = computeStatus(nc, plan, obs)
	r.observeLeafs(ctx, nc, plan, &nc.Status)
	recordGateBlocked(r.Recorder, nc, orig.Status.Conditions)
	if err := r.patchStatus(ctx, orig, nc); err != nil {
		return ctrl.Result{}, err
	}
	if meta.IsStatusConditionTrue(nc.Status.Conditions, ConditionSettled) && nc.Status.ReadyReplicas == nc.Spec.Replicas && len(obs.Apply.Reloading) == 0 && obs.Rollout.Status == nil {
		return ctrl.Result{RequeueAfter: resyncSettled}, nil
	}
	return ctrl.Result{RequeueAfter: resyncUnsettled}, nil
}

// unsupportedFields names the spec fields this controller does not render.
func unsupportedFields(spec *clusterv1beta1.NatsClusterSpec) []string {
	return unsupportedLeafFields(spec)
}

// hold reports held as nc's Progressing condition and patches the status:
// a spec, trust or leaf remote nothing is rendered past.
func (r *Reconciler) hold(ctx context.Context, orig, nc *clusterv1beta1.NatsCluster, held *metav1.Condition) error {
	conditions.Set(&nc.Status.Conditions, nc.Generation, progressingCondition(nc, nil, Observed{Held: held}))
	return r.patchStatus(ctx, orig, nc)
}

func unsupportedSpec(msg string) *metav1.Condition {
	return &metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: ReasonUnsupportedSpec, Message: msg}
}

func (r *Reconciler) patchStatus(ctx context.Context, orig, nc *clusterv1beta1.NatsCluster) error {
	if err := r.Client.Status().Patch(ctx, nc, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("patch status: %w", err)
	}
	return nil
}

// applyShared creates or updates the Services and the PodDisruptionBudget,
// and deletes the gateway Service once gateway.service is unset.
func (r *Reconciler) applyShared(ctx context.Context, nc *clusterv1beta1.NatsCluster, plan *Plan) error {
	services := []*corev1.Service{plan.HeadlessService, plan.ClientService}
	if plan.GatewayService != nil {
		services = append(services, plan.GatewayService)
	} else if err := r.deleteOwned(ctx, nc, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: gatewayServiceName(nc), Namespace: nc.Namespace}}); err != nil {
		return err
	}
	for _, want := range services {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: want.Name, Namespace: want.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
			svc.Labels = merged(svc.Labels, want.Labels)
			svc.Annotations = merged(svc.Annotations, want.Annotations)
			if svc.Spec.ClusterIP == "" {
				svc.Spec.ClusterIP = want.Spec.ClusterIP
			}
			svc.Spec.Type = want.Spec.Type
			svc.Spec.Selector = want.Spec.Selector
			svc.Spec.Ports = want.Spec.Ports
			svc.Spec.PublishNotReadyAddresses = want.Spec.PublishNotReadyAddresses
			return controllerutil.SetControllerReference(nc, svc, r.Client.Scheme())
		}); err != nil {
			return fmt.Errorf("apply service %s: %w", want.Name, err)
		}
	}
	want := plan.PDB
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: want.Name, Namespace: want.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		pdb.Labels = merged(pdb.Labels, want.Labels)
		pdb.Spec.MaxUnavailable = want.Spec.MaxUnavailable
		pdb.Spec.Selector = want.Spec.Selector
		return controllerutil.SetControllerReference(nc, pdb, r.Client.Scheme())
	}); err != nil {
		return fmt.Errorf("apply pdb %s: %w", want.Name, err)
	}
	return nil
}

// deleteOwned deletes obj when it exists and nc controls it.
func (r *Reconciler) deleteOwned(ctx context.Context, nc *clusterv1beta1.NatsCluster, obj client.Object) error {
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(obj, nc) {
		return nil
	}
	if err := r.Client.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete %s: %w", obj.GetName(), err)
	}
	return nil
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// statefulSets returns the NatsCluster's server StatefulSets by name.
func (r *Reconciler) statefulSets(ctx context.Context, nc *clusterv1beta1.NatsCluster) (map[string]*appsv1.StatefulSet, error) {
	var list appsv1.StatefulSetList
	if err := r.Client.List(ctx, &list, client.InNamespace(nc.Namespace), client.MatchingLabels(clusterSelector(nc))); err != nil {
		return nil, fmt.Errorf("list statefulsets: %w", err)
	}
	out := map[string]*appsv1.StatefulSet{}
	for i := range list.Items {
		sts := &list.Items[i]
		if metav1.IsControlledBy(sts, nc) {
			out[sts.Name] = sts
		}
	}
	return out, nil
}

// createServer creates a server's ConfigMap, replacing one left without its
// StatefulSet, then the StatefulSet.
func (r *Reconciler) createServer(ctx context.Context, nc *clusterv1beta1.NatsCluster, s Server) (*appsv1.StatefulSet, error) {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: s.ConfigMap.Name, Namespace: s.ConfigMap.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = s.ConfigMap.Labels
		cm.Annotations = merged(cm.Annotations, s.ConfigMap.Annotations)
		cm.Data = s.ConfigMap.Data
		return controllerutil.SetControllerReference(nc, cm, r.Client.Scheme())
	}); err != nil {
		return nil, fmt.Errorf("apply configmap %s: %w", cm.Name, err)
	}
	sts := s.StatefulSet.DeepCopy()
	if err := controllerutil.SetControllerReference(nc, sts, r.Client.Scheme()); err != nil {
		return nil, err
	}
	if err := r.Client.Create(ctx, sts); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("statefulset %s exists and is not this NatsCluster's: %w", sts.Name, err)
		}
		return nil, fmt.Errorf("create statefulset %s: %w", sts.Name, err)
	}
	return sts, nil
}
