// Package natscluster is the cluster controller's reconciler: it renders a
// NatsCluster into one StatefulSet and ConfigMap per server, the Services, a
// PodDisruptionBudget and the route certificate, and reports Ready, Settled
// and Progressing.
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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// Observer observes the NATS cluster a NatsCluster deployed.
type Observer interface {
	Observe(ctx context.Context, nc *clusterv1beta1.NatsCluster) (*sysobs.Snapshot, error)
}

// MonitorObserver observes a NatsCluster's servers through each pod's HTTP
// monitoring port, addressed under the headless Service. It is how a NATS
// cluster without an auth plane is observed: its system account has no user
// to connect as.
type MonitorObserver struct {
	Monitor *sysobs.MonitorObserver
}

// Observe observes every server spec.replicas names.
func (m MonitorObserver) Observe(ctx context.Context, nc *clusterv1beta1.NatsCluster) (*sysobs.Snapshot, error) {
	var eps []sysobs.Endpoint
	for _, s := range serverNames(nc) {
		eps = append(eps, sysobs.Endpoint{Name: s, URL: fmt.Sprintf("http://%s:%d", podHost(nc, s), PortMonitor)})
	}
	return m.Monitor.Observe(ctx, eps)
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
	// Forget, when set, is called with the key of a NatsCluster that is
	// gone or being deleted.
	Forget func(types.NamespacedName)
	Now    func() time.Time
}

// +kubebuilder:rbac:groups=cluster.nats.mikluko.io,resources=natsclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.nats.mikluko.io,resources=natsclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;configmaps;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsoperatortrusts;natsreferencegrants,verbs=get;list;watch

// TrustField is the field index SetupWithManager registers on NatsClusters:
// the namespace/name of the NatsOperatorTrust auth.trustRef names.
const TrustField = "cluster.nats.mikluko.io/trust"

// SetupWithManager registers TrustField and the grant index on mgr's cache
// and registers r with mgr, watching the NatsOperatorTrusts and
// NatsReferenceGrants NatsClusters read.
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
		if a := o.(*clusterv1beta1.NatsCluster).Spec.Auth; a != nil {
			return []string{a.TrustRef.Namespace}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("index NatsCluster grants: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
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
		Complete(r)
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
	var list clusterv1beta1.NatsClusterList
	if err := r.Client.List(ctx, &list, client.MatchingFields{TrustField: t.GetNamespace() + "/" + t.GetName()}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "list NatsClusters reading NatsOperatorTrust", "trust", client.ObjectKeyFromObject(t))
		return nil
	}
	out := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
	}
	return out
}

// Reconcile creates what a NatsCluster renders and reports its status. A
// server's ConfigMap and StatefulSet are created when absent; a changed
// revision is reloaded where the change reloads, and is otherwise rolled
// out one restart at a time.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	nc := &clusterv1beta1.NatsCluster{}
	if err := r.Client.Get(ctx, req.NamespacedName, nc); err != nil {
		if apierrors.IsNotFound(err) && r.Forget != nil {
			r.Forget(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !nc.DeletionTimestamp.IsZero() {
		if r.Forget != nil {
			r.Forget(req.NamespacedName)
		}
		return ctrl.Result{}, nil
	}
	orig := nc.DeepCopy()

	if fields := unsupportedFields(&nc.Spec); len(fields) > 0 {
		setCondition(&nc.Status, metav1.Condition{
			Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: ReasonUnsupportedSpec,
			Message: "not rendered by this cluster controller: " + strings.Join(fields, ", "),
		}, nc.Generation)
		return ctrl.Result{}, r.patchStatus(ctx, orig, nc)
	}
	trust, cond, err := readTrust(ctx, r.Client, nc)
	if err != nil {
		return ctrl.Result{}, err
	}
	if cond != nil {
		setCondition(&nc.Status, *cond, nc.Generation)
		return ctrl.Result{RequeueAfter: resyncUnsettled}, r.patchStatus(ctx, orig, nc)
	}
	plan, err := Render(nc, trust)
	if err != nil {
		setCondition(&nc.Status, metav1.Condition{
			Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: ReasonUnsupportedSpec, Message: err.Error(),
		}, nc.Generation)
		return ctrl.Result{}, r.patchStatus(ctx, orig, nc)
	}

	if err := r.applyShared(ctx, nc, plan); err != nil {
		return ctrl.Result{}, err
	}
	certWait, err := r.ensureRouteCert(ctx, nc)
	if err != nil {
		return ctrl.Result{}, err
	}

	stsByName, err := r.statefulSets(ctx, nc)
	if err != nil {
		return ctrl.Result{}, err
	}
	obs := Observed{StatefulSets: stsByName}
	if certWait == "" {
		for _, s := range plan.Servers {
			if stsByName[s.Name] != nil {
				continue
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
	if certWait != "" {
		setCondition(&nc.Status, metav1.Condition{
			Type: ConditionProgressing, Status: metav1.ConditionTrue, Reason: ReasonRouteCertNotReady, Message: certWait,
		}, nc.Generation)
	}
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
	var out []string
	if spec.Gateway != nil {
		out = append(out, "gateway")
	}
	if spec.Leafnodes != nil {
		out = append(out, "leafnodes")
	}
	if len(spec.LeafRemotes) > 0 {
		out = append(out, "leafRemotes")
	}
	return out
}

func (r *Reconciler) patchStatus(ctx context.Context, orig, nc *clusterv1beta1.NatsCluster) error {
	if err := r.Client.Status().Patch(ctx, nc, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("patch status: %w", err)
	}
	return nil
}

// applyShared creates or updates the Services and the PodDisruptionBudget.
func (r *Reconciler) applyShared(ctx context.Context, nc *clusterv1beta1.NatsCluster, plan *Plan) error {
	for _, want := range []*corev1.Service{plan.HeadlessService, plan.ClientService} {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: want.Name, Namespace: want.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
			svc.Labels = merged(svc.Labels, want.Labels)
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

// ensureRouteCert makes the route certificate Secret exist: generated when
// self-signed, requested from cert-manager when it issues it, only read when
// named. It returns what the servers wait for, or "" once the Secret holds
// every key a server mounts.
func (r *Reconciler) ensureRouteCert(ctx context.Context, nc *clusterv1beta1.NatsCluster) (string, error) {
	name := routesSecret(nc)
	if name == "" {
		return "", nil
	}
	selfSigned := name == routesSecretName(nc)
	if issuer := certManagerIssuer(nc); issuer != nil {
		selfSigned = false
		want := routesCertificate(nc, issuer)
		cert := &unstructured.Unstructured{}
		cert.SetGroupVersionKind(want.GroupVersionKind())
		cert.SetName(want.GetName())
		cert.SetNamespace(want.GetNamespace())
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cert, func() error {
			cert.SetLabels(merged(cert.GetLabels(), want.GetLabels()))
			cert.Object["spec"] = want.Object["spec"]
			return controllerutil.SetControllerReference(nc, cert, r.Client.Scheme())
		})
		if meta.IsNoMatchError(err) {
			return "cert-manager Certificate is not a known kind: cert-manager is not installed", nil
		}
		if err != nil {
			return "", fmt.Errorf("apply route certificate: %w", err)
		}
	}

	secret := &corev1.Secret{}
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: nc.Namespace, Name: name}, secret)
	switch {
	case apierrors.IsNotFound(err) && selfSigned:
		secret, err = selfSignedRouteSecret(nc, routeDNSNames(nc), r.now())
		if err != nil {
			return "", err
		}
		if err := controllerutil.SetControllerReference(nc, secret, r.Client.Scheme()); err != nil {
			return "", err
		}
		if err := r.Client.Create(ctx, secret); err != nil {
			return "", fmt.Errorf("create route secret: %w", err)
		}
		return "", nil
	case apierrors.IsNotFound(err):
		return fmt.Sprintf("Secret %s does not exist", name), nil
	case err != nil:
		return "", fmt.Errorf("get route secret: %w", err)
	}
	for _, k := range []string{corev1.TLSCertKey, corev1.TLSPrivateKeyKey, caKey} {
		if len(secret.Data[k]) == 0 {
			return fmt.Sprintf("Secret %s has no %s", name, k), nil
		}
	}
	return "", nil
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
