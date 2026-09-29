package natscluster

import (
	"context"
	"fmt"
	"time"

	distref "github.com/distribution/reference"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// ServerReloader reads and reloads one server's config over the system
// account.
type ServerReloader interface {
	Config(ctx context.Context, serverID string) (sysobs.ConfigState, error)
	Reload(ctx context.Context, serverID string) (sysobs.ConfigState, error)
}

var _ ServerReloader = (*sysobs.SystemClient)(nil)

// ReloaderFunc returns the ServerReloader of the NATS cluster nc deployed,
// or an error saying why it has none.
type ReloaderFunc func(ctx context.Context, nc *clusterv1beta1.NatsCluster) (ServerReloader, error)

// reloadWindow bounds how long a server may keep loading the previous
// config after its ConfigMap was written for a reload: kubelet refreshes a
// mounted ConfigMap within its sync period plus its cache TTL, a minute
// each by default.
const reloadWindow = 3 * time.Minute

// configApply is how one reconcile applies the plan's revision to the
// servers not yet on it.
type configApply struct {
	// Restart maps each server whose revision needs a restart to why.
	Restart map[string]string
	// Reloading are the servers whose reload is not yet confirmed.
	Reloading []string
	// Reloaded are the servers a reload moved to the revision.
	Reloaded []string
}

func (a *configApply) restart(server, reason string) {
	if a.Restart == nil {
		a.Restart = map[string]string{}
	}
	a.Restart[server] = reason
}

// applyConfig reloads each server off plan's revision whose change reloads,
// recreating the missing ConfigMap of one on it, and reports as needing a
// restart the rest and any reload unconfirmed within reloadWindow.
func (r *Reconciler) applyConfig(ctx context.Context, nc *clusterv1beta1.NatsCluster, plan *Plan, sts map[string]*appsv1.StatefulSet, snap *sysobs.Snapshot) (configApply, error) {
	var a configApply
	var rl ServerReloader
	var rlErr error
	reloader := func() (ServerReloader, error) {
		if rl == nil && rlErr == nil {
			if r.Reloader == nil {
				rlErr = fmt.Errorf("no system account connection to reload over")
			} else {
				rl, rlErr = r.Reloader(ctx, nc)
			}
		}
		return rl, rlErr
	}

	for _, s := range plan.Servers {
		cur := sts[s.Name]
		if cur == nil {
			continue
		}
		onTarget := cur.Annotations[AnnotationConfigRevision] == plan.Revision
		cm := &corev1.ConfigMap{}
		err := r.Client.Get(ctx, client.ObjectKeyFromObject(s.ConfigMap), cm)
		if apierrors.IsNotFound(err) {
			if !onTarget {
				a.restart(s.Name, fmt.Sprintf("ConfigMap %s does not exist", s.ConfigMap.Name))
				continue
			}
			if cm, err = r.applyServerConfigMap(ctx, nc, s, dropRevision); err != nil {
				return a, err
			}
		} else if err != nil {
			return a, fmt.Errorf("get configmap %s: %w", s.ConfigMap.Name, err)
		}
		if onTarget && cm.Annotations[AnnotationConfigRevision] == plan.Revision {
			continue
		}
		if !metav1.IsControlledBy(cm, nc) {
			return a, r.notControlled(cm)
		}
		if onTarget {
			if err := r.markRevision(ctx, cur, cm.Annotations[AnnotationConfigRevision]); err != nil {
				return a, err
			}
		}

		if cm.Annotations[AnnotationConfigRevision] != plan.Revision {
			reason := changeRestartReason(nc, cur, cm, s)
			if reason == "" {
				if _, err := reloader(); err != nil {
					reason = err.Error()
				}
			}
			if reason != "" {
				a.restart(s.Name, reason)
				continue
			}
			if err := r.writeForReload(ctx, cm, s, plan.Revision); err != nil {
				return a, err
			}
		}
		if cm.Annotations[AnnotationConfigApply] != string(clusterv1beta1.ConfigAppliedByReload) {
			a.restart(s.Name, cm.Annotations[AnnotationRestartReason])
			continue
		}

		applied := false
		rl, err := reloader()
		if err == nil {
			applied, err = reloadServer(ctx, rl, snap, s, plan.Certs)
		}
		if err == nil && !applied && r.reloadExpired(cm) {
			err = fmt.Errorf("%s did not load revision %s within %s", s.Name, plan.Revision, reloadWindow)
		}
		switch {
		case err != nil:
			reason := "reload failed: " + err.Error()
			if err := r.markRestart(ctx, cm, reason); err != nil {
				return a, err
			}
			a.restart(s.Name, reason)
		case applied:
			if err := r.markRevision(ctx, cur, plan.Revision); err != nil {
				return a, err
			}
			a.Reloaded = append(a.Reloaded, s.Name)
		default:
			a.Reloading = append(a.Reloading, s.Name)
		}
	}
	return a, nil
}

// changeRestartReason classifies the change from what server s runs, its
// StatefulSet cur and ConfigMap cm, to what the plan renders for it: "" when
// a reload applies it, otherwise why it is restart-only.
func changeRestartReason(nc *clusterv1beta1.NatsCluster, cur *appsv1.StatefulSet, cm *corev1.ConfigMap, s Server) string {
	if cur.Annotations[AnnotationSpecDigest] != s.StatefulSet.Annotations[AnnotationSpecDigest] {
		if running := runningVersion(cur); running != "" && running != nc.Spec.Version {
			return fmt.Sprintf("version %s -> %s is restart-only", running, nc.Spec.Version)
		}
		return "the StatefulSet spec is restart-only"
	}
	return restartReason(nc.Spec.Version, []byte(cm.Data[configFile]), []byte(s.ConfigMap.Data[configFile]))
}

// runningVersion is the image tag of sts's nats container, or "" when the
// image has no tag or does not parse.
func runningVersion(sts *appsv1.StatefulSet) string {
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name != "nats" {
			continue
		}
		ref, err := distref.Parse(c.Image)
		if err != nil {
			return ""
		}
		if t, ok := ref.(distref.Tagged); ok {
			return t.Tag()
		}
		return ""
	}
	return ""
}

// reloadServer reports whether server s has loaded its rendered config and
// the certificates in certs, requesting a reload when it has not. It
// returns false without an error while the server is unobserved or still
// loads a previous file.
func reloadServer(ctx context.Context, rl ServerReloader, snap *sysobs.Snapshot, s Server, certs Certs) (bool, error) {
	id := serverID(snap, s.Name)
	if id == "" {
		return false, nil
	}
	want, err := configDigest([]byte(s.ConfigMap.Data[configFile]))
	if err != nil {
		return false, err
	}
	st, err := rl.Config(ctx, id)
	if err != nil {
		return false, err
	}
	if st.Digest == want && certs.loaded(st.CertNotAfter) {
		return true, nil
	}
	st, err = rl.Reload(ctx, id)
	if err != nil {
		return false, err
	}
	return st.Digest == want && certs.loaded(st.CertNotAfter), nil
}

// serverID is the ID of the server named name in snap, or "" when snap
// has no answer from it.
func serverID(snap *sysobs.Snapshot, name string) string {
	if snap == nil {
		return ""
	}
	for _, s := range snap.Silent {
		if s == name {
			return ""
		}
	}
	for _, s := range snap.Servers {
		if s.Name == name {
			return s.ID
		}
	}
	return ""
}

func (r *Reconciler) reloadExpired(cm *corev1.ConfigMap) bool {
	since, err := time.Parse(time.RFC3339, cm.Annotations[AnnotationReloadSince])
	return err != nil || r.now().Sub(since) > reloadWindow
}

// dropRevision removes the config revision from a server ConfigMap's
// annotations, so no StatefulSet reads the ConfigMap as on its revision.
func dropRevision(a map[string]string) {
	delete(a, AnnotationConfigRevision)
}

// writeForReload writes server s's rendered config into cm, marked for a
// reload to revision since r.now().
func (r *Reconciler) writeForReload(ctx context.Context, cm *corev1.ConfigMap, s Server, revision string) error {
	orig := cm.DeepCopy()
	cm.Data = s.ConfigMap.Data
	cm.Annotations = merged(cm.Annotations, map[string]string{
		AnnotationConfigRevision: revision,
		AnnotationConfigApply:    string(clusterv1beta1.ConfigAppliedByReload),
		AnnotationReloadSince:    r.now().UTC().Format(time.RFC3339),
	})
	delete(cm.Annotations, AnnotationRestartReason)
	if err := r.Client.Patch(ctx, cm, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("write configmap %s for reload: %w", cm.Name, err)
	}
	return nil
}

// markRestart records on cm that its revision needs a restart, and why.
func (r *Reconciler) markRestart(ctx context.Context, cm *corev1.ConfigMap, reason string) error {
	orig := cm.DeepCopy()
	cm.Annotations = merged(cm.Annotations, map[string]string{
		AnnotationConfigApply:   string(clusterv1beta1.ConfigAppliedByRestart),
		AnnotationRestartReason: reason,
	})
	if err := r.Client.Patch(ctx, cm, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("mark configmap %s for restart: %w", cm.Name, err)
	}
	return nil
}

// markRevision annotates sts with the revision its server runs, or may run;
// its pod template is left alone, so nothing restarts.
func (r *Reconciler) markRevision(ctx context.Context, sts *appsv1.StatefulSet, revision string) error {
	orig := sts.DeepCopy()
	sts.Annotations = merged(sts.Annotations, map[string]string{AnnotationConfigRevision: revision})
	if err := r.Client.Patch(ctx, sts, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("annotate statefulset %s: %w", sts.Name, err)
	}
	return nil
}
