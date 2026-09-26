package natscluster

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// world plays kubelet and the NATS cluster for one NatsCluster: every
// StatefulSet's pod is Ready and its server answers, reporting the revision
// its StatefulSet names; the meta group and streams move as the
// ServerAdmin calls ask, and a server that was not in the meta group stays
// out until readmit.
type world struct {
	c   client.Client
	ns  string
	obs *fakeObserver

	mu      sync.Mutex
	running []string
	fresh   []string
	meta    []string
	leader  string
	streams map[string][]string
	calls   []string
}

func (w *world) Evacuate(_ context.Context, server string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, "Evacuate "+server)
	for name, ms := range w.streams {
		i := slices.Index(ms, server)
		if i < 0 {
			continue
		}
		ms = slices.Delete(ms, i, i+1)
		for _, s := range w.running {
			if s != server && !slices.Contains(ms, s) {
				ms = append(ms, s)
				break
			}
		}
		w.streams[name] = ms
	}
	return nil
}

func (w *world) RemovePeer(_ context.Context, server string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, "RemovePeer "+server)
	if !slices.Contains(w.meta, server) {
		return sysobs.ErrNotMember
	}
	w.meta = slices.DeleteFunc(w.meta, func(s string) bool { return s == server })
	return nil
}

func (w *world) StepDownMeta(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, "StepDownMeta "+w.leader)
	w.leader = w.meta[(slices.Index(w.meta, w.leader)+1)%len(w.meta)]
	return nil
}

// readmitFresh readmits every running server not being removed, as the
// meta leader does once a removal tombstone lapses.
func (w *world) readmitFresh() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, s := range w.fresh {
		if !slices.Contains(w.meta, s) {
			w.meta = append(w.meta, s)
		}
	}
	slices.Sort(w.meta)
}

// sync brings every StatefulSet's pod up and observes the servers running.
func (w *world) sync(t *testing.T, ctx context.Context) {
	t.Helper()
	var list appsv1.StatefulSetList
	require.NoError(t, w.c.List(ctx, &list, client.InNamespace(w.ns)))
	w.mu.Lock()
	defer w.mu.Unlock()
	w.running, w.fresh = nil, nil
	snap := &sysobs.Snapshot{}
	for i := range list.Items {
		sts := &list.Items[i]
		if sts.Status.ReadyReplicas == 0 || sts.Status.ObservedGeneration != sts.Generation {
			sts.Status.ObservedGeneration = sts.Generation
			sts.Status.Replicas, sts.Status.UpdatedReplicas, sts.Status.ReadyReplicas = 1, 1, 1
			require.NoError(t, w.c.Status().Update(ctx, sts))
		}
		w.running = append(w.running, sts.Name)
		if p := removalPhase(sts.Annotations[AnnotationRemoval]); p == "" || p == phaseRejoining {
			w.fresh = append(w.fresh, sts.Name)
		}
		snap.Servers = append(snap.Servers, sysobs.Server{Name: sts.Name, ID: sts.Name, Version: "2.15.0", JetStream: true,
			Metadata: map[string]string{MetadataConfigRevision: sts.Annotations[AnnotationConfigRevision]}})
	}
	slices.Sort(w.running)
	members := func(ss []string) []sysobs.Member {
		var out []sysobs.Member
		for _, s := range ss {
			if slices.Contains(w.running, s) {
				out = append(out, sysobs.Member{Server: s, Current: true})
			}
		}
		return out
	}
	snap.Groups = append(snap.Groups, sysobs.Group{Kind: sysobs.KindMeta, Leader: w.leader, Members: members(w.meta)})
	names := make([]string, 0, len(w.streams))
	for name := range w.streams {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		ms := w.streams[name]
		snap.Groups = append(snap.Groups, sysobs.Group{Kind: sysobs.KindStream, Account: "$G", Stream: name, Leader: ms[0], Members: members(ms)})
	}
	w.obs.set(snap)
}

// TestEnvtestRemoval drives scale-down, server replacement and the
// deletion guard against a real API server, with world in place of the
// StatefulSet controller, kubelet and the NATS cluster.
func TestEnvtestRemoval(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, natsv1beta1.AddToScheme(scheme))
	require.NoError(t, clusterv1beta1.AddToScheme(scheme))
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := t.Context()

	// setUp creates story 1's NatsCluster with replicas servers in ns and
	// brings it to rest, an R3 stream ORDERS on its first three servers
	// and the meta group led by leader.
	setUp := func(t *testing.T, ns string, replicas int32, leader string) *removalHarness {
		t.Helper()
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		nc := storyCluster(t)
		nc.Namespace, nc.Spec.Replicas = ns, replicas
		require.NoError(t, c.Create(ctx, nc))
		w := &world{c: c, ns: ns, obs: &fakeObserver{}, leader: leader, streams: map[string][]string{}}
		for i := range replicas {
			w.meta = append(w.meta, serverName(nc, int(i)))
		}
		w.streams["ORDERS"] = slices.Clone(w.meta[:3])
		reloader := &fakeReloader{c: c}
		reloader.reset(ns)
		h := &removalHarness{c: c, w: w, key: client.ObjectKeyFromObject(nc), r: &Reconciler{Client: c, Observer: w.obs,
			Reloader: func(context.Context, *clusterv1beta1.NatsCluster) (ServerReloader, error) { return reloader, nil },
			Admin:    func(context.Context, *clusterv1beta1.NatsCluster) (ServerAdmin, error) { return w, nil }}}
		h.reconcile(t)
		h.reconcile(t)
		requireCondition(t, h.get(t), ConditionProgressing, metav1.ConditionFalse, ReasonUpToDate)
		return h
	}

	t.Run("scale-down from 5 to 3 under an R3 stream", func(t *testing.T) {
		h := setUp(t, "scaledown", 5, "demo-0")
		require.True(t, controllerutil.ContainsFinalizer(h.get(t), FinalizerJetStreamData))
		h.w.streams["R1"] = []string{"demo-4"}
		h.w.streams["WIDE"] = slices.Clone(h.w.meta)
		for _, s := range []string{"demo-3", "demo-4"} {
			createClaim(t, c, "scaledown", s)
		}
		h.w.sync(t, ctx)

		nc := h.get(t)
		nc.Spec.Replicas = 3
		require.NoError(t, c.Update(ctx, nc))
		h.reconcile(t)
		got := h.get(t)
		requireCondition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonScaleDownBlocked)
		require.Equal(t, "cannot remove demo-4, demo-3: $G/WIDE has 5 replicas; 3 servers cannot hold them",
			meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing).Message)
		require.Empty(t, h.w.calls)

		delete(h.w.streams, "WIDE")
		h.w.sync(t, ctx)
		reasons := h.settle(t)
		require.Contains(t, reasons, ReasonScalingDown)
		require.Equal(t, []string{
			"Evacuate demo-4", "RemovePeer demo-4",
			"Evacuate demo-3", "RemovePeer demo-3",
		}, h.w.calls)
		require.Equal(t, []string{"demo-0", "demo-1", "demo-2"}, h.w.running)
		require.Equal(t, []string{"demo-0", "demo-1", "demo-2"}, h.w.meta)
		require.Subset(t, h.w.running, h.w.streams["R1"])
		for _, s := range []string{"demo-3", "demo-4"} {
			requireClaimDeleted(t, c, "scaledown", s)
			requireGone(t, c, &corev1.ConfigMap{}, "scaledown", configMapName(s))
		}
		require.Equal(t, int32(3), h.get(t).Status.ReadyReplicas)
	})

	t.Run("replace-server replaces one server under its own name", func(t *testing.T) {
		h := setUp(t, "replace", 3, "demo-0")
		createClaim(t, c, "replace", "demo-1")
		before := h.sts(t, "demo-1").UID

		nc := h.get(t)
		nc.Annotations = map[string]string{clusterv1beta1.AnnotationReplaceServer: "demo-1"}
		require.NoError(t, c.Update(ctx, nc))
		h.reconcile(t)
		got := h.get(t)
		require.NotContains(t, got.Annotations, clusterv1beta1.AnnotationReplaceServer)
		require.Equal(t, string(phaseEvacuating), h.sts(t, "demo-1").Annotations[AnnotationRemoval])
		requireCondition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonReplacingServer)

		h.reconcile(t)
		h.reconcile(t)
		requireClaimDeleted(t, c, "replace", "demo-1")
		h.reconcile(t)
		requireGone(t, c, &appsv1.StatefulSet{}, "replace", "demo-1")
		requireCondition(t, h.get(t), ConditionProgressing, metav1.ConditionTrue, ReasonCreating)

		releaseClaim(t, c, "replace", "demo-1")
		h.reconcile(t)
		after := h.sts(t, "demo-1")
		require.NotEqual(t, before, after.UID, "demo-1 was not recreated")
		require.Equal(t, string(phaseRejoining), after.Annotations[AnnotationRemoval])

		h.reconcile(t)
		got = h.get(t)
		require.Equal(t, "demo-1", got.Status.Rollout.Current)
		require.Equal(t, GateSettled, got.Status.Rollout.Gate.WaitingFor)
		requireCondition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonReplacingServer)
		require.NotContains(t, h.w.meta, "demo-1")

		h.w.readmitFresh()
		h.w.sync(t, ctx)
		h.reconcile(t)
		requireCondition(t, h.get(t), ConditionProgressing, metav1.ConditionFalse, ReasonUpToDate)
		require.Empty(t, h.sts(t, "demo-1").Annotations[AnnotationRemoval])
		require.Equal(t, []string{"Evacuate demo-1", "RemovePeer demo-1"}, h.w.calls)
	})

	t.Run("a volume change replaces every server, the meta leader last", func(t *testing.T) {
		h := setUp(t, "revolume", 3, "demo-0")
		uids := map[string]types.UID{}
		for _, s := range []string{"demo-0", "demo-1", "demo-2"} {
			uids[s] = h.sts(t, s).UID
		}
		nc := h.get(t)
		nc.Spec.JetStream.VolumeClaimTemplate.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("40Gi")
		require.NoError(t, c.Update(ctx, nc))

		reasons := h.settle(t)
		require.Contains(t, reasons, ReasonReplacingServer)
		require.Equal(t, []string{
			"Evacuate demo-2", "RemovePeer demo-2",
			"Evacuate demo-1", "RemovePeer demo-1",
			"Evacuate demo-0", "StepDownMeta demo-0", "RemovePeer demo-0",
		}, h.w.calls)
		for s, uid := range uids {
			sts := h.sts(t, s)
			require.NotEqual(t, uid, sts.UID, "%s was not recreated", s)
			require.Equal(t, "40Gi", sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage().String())
		}
	})

	t.Run("deletion waits for stream groups, and force-delete overrides it", func(t *testing.T) {
		h := setUp(t, "guard", 3, "demo-0")
		require.NoError(t, c.Delete(ctx, h.get(t)))
		h.reconcile(t)
		got := h.get(t)
		requireCondition(t, got, ConditionDeleting, metav1.ConditionTrue, ReasonJetStreamDataRemains)
		require.Equal(t, "1 stream groups still placed in demo ($G/ORDERS)", meta.FindStatusCondition(got.Status.Conditions, ConditionDeleting).Message)

		got.Annotations = map[string]string{clusterv1beta1.AnnotationForceDelete: ""}
		require.NoError(t, c.Update(ctx, got))
		h.reconcile(t)
		requireGone(t, c, &clusterv1beta1.NatsCluster{}, "guard", "demo")
	})

	t.Run("deletion proceeds once no stream group remains", func(t *testing.T) {
		h := setUp(t, "empty", 3, "demo-0")
		delete(h.w.streams, "ORDERS")
		h.w.sync(t, ctx)
		require.NoError(t, c.Delete(ctx, h.get(t)))
		h.reconcile(t)
		requireGone(t, c, &clusterv1beta1.NatsCluster{}, "empty", "demo")
	})
}

// removalHarness reconciles one NatsCluster against world.
type removalHarness struct {
	c   client.Client
	w   *world
	r   *Reconciler
	key types.NamespacedName
}

func (h *removalHarness) reconcile(t *testing.T) {
	t.Helper()
	_, err := h.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: h.key})
	require.NoError(t, err)
	h.w.sync(t, t.Context())
}

func (h *removalHarness) get(t *testing.T) *clusterv1beta1.NatsCluster {
	t.Helper()
	nc := &clusterv1beta1.NatsCluster{}
	require.NoError(t, h.c.Get(t.Context(), h.key, nc))
	return nc
}

func (h *removalHarness) sts(t *testing.T, name string) *appsv1.StatefulSet {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, h.c.Get(t.Context(), types.NamespacedName{Namespace: h.key.Namespace, Name: name}, sts))
	return sts
}

// settle reconciles until Progressing reads UpToDate, readmitting
// recreated servers into the meta group whenever a reconcile acts on
// nothing, and returns every Progressing reason it saw.
func (h *removalHarness) settle(t *testing.T) []string {
	t.Helper()
	var reasons []string
	for range 60 {
		calls := len(h.w.calls)
		h.reconcile(t)
		c := meta.FindStatusCondition(h.get(t).Status.Conditions, ConditionProgressing)
		reasons = append(reasons, c.Reason)
		if c.Status == metav1.ConditionFalse {
			require.Equal(t, ReasonUpToDate, c.Reason, c.Message)
			return reasons
		}
		if len(h.w.calls) == calls {
			h.w.readmitFresh()
			h.w.sync(t, t.Context())
		}
	}
	t.Fatalf("did not settle: %v", reasons)
	return nil
}

func requireCondition(t *testing.T, nc *clusterv1beta1.NatsCluster, typ string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	c := meta.FindStatusCondition(nc.Status.Conditions, typ)
	require.NotNil(t, c, typ)
	require.Equal(t, status, c.Status, "%s: %s", typ, c.Message)
	require.Equal(t, reason, c.Reason, "%s: %s", typ, c.Message)
}

// createClaim creates server's data volume claim, as the StatefulSet
// controller would.
func createClaim(t *testing.T, c client.Client, ns, server string) {
	t.Helper()
	require.NoError(t, c.Create(t.Context(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: dataClaimName(server)},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("20Gi")}},
		},
	}))
}

// requireClaimDeleted requires server's data volume claim to be gone or
// terminating: the API server's storage protection holds a deleted claim
// until a controller that does not run under envtest releases it.
func requireClaimDeleted(t *testing.T, c client.Client, ns, server string) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	err := c.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: dataClaimName(server)}, pvc)
	if apierrors.IsNotFound(err) {
		return
	}
	require.NoError(t, err)
	require.False(t, pvc.DeletionTimestamp.IsZero(), "%s was not deleted", pvc.Name)
}

// releaseClaim plays the controller that finishes deleting a claim.
func releaseClaim(t *testing.T, c client.Client, ns, server string) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: dataClaimName(server)}, pvc))
	pvc.Finalizers = nil
	require.NoError(t, c.Update(t.Context(), pvc))
	requireGone(t, c, &corev1.PersistentVolumeClaim{}, ns, pvc.Name)
}

func requireGone(t *testing.T, c client.Client, obj client.Object, ns, name string) {
	t.Helper()
	err := c.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: name}, obj)
	require.True(t, apierrors.IsNotFound(err), "%s %s/%s still exists: %v", fmt.Sprintf("%T", obj), ns, name, err)
}
