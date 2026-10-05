package natscluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// fakeObserver returns whatever snapshot and leafnode connections it was
// last given.
type fakeObserver struct {
	mu    sync.Mutex
	snap  *sysobs.Snapshot
	leafs map[string][]sysobs.Leaf
}

func (f *fakeObserver) Observe(context.Context, *clusterv1beta1.NatsCluster) (*sysobs.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snap == nil {
		return nil, sysobs.ErrNoServers
	}
	return f.snap, nil
}

func (f *fakeObserver) set(s *sysobs.Snapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snap = s
}

func (f *fakeObserver) ObserveLeafs(context.Context, *clusterv1beta1.NatsCluster) (map[string][]sysobs.Leaf, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leafs == nil {
		return nil, sysobs.ErrNoServers
	}
	return f.leafs, nil
}

func (f *fakeObserver) setLeafs(l map[string][]sysobs.Leaf) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leafs = l
}

// fakeReloader reloads a server by digesting its ConfigMap as the API
// server holds it, as though kubelet had already refreshed the pod's file;
// with lag it keeps reporting the file each server first loaded.
type fakeReloader struct {
	c      client.Client
	ns     string
	lag    bool
	reject error

	mu     sync.Mutex
	loaded map[string]string
	calls  []string
}

func (f *fakeReloader) reset(ns string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ns, f.loaded, f.calls = ns, map[string]string{}, nil
}

func (f *fakeReloader) reloaded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func (f *fakeReloader) Config(_ context.Context, id string) (sysobs.ConfigState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return sysobs.ConfigState{Digest: f.loaded[id]}, nil
}

func (f *fakeReloader) Reload(ctx context.Context, id string) (sysobs.ConfigState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, id)
	if f.reject != nil {
		return sysobs.ConfigState{}, f.reject
	}
	if !f.lag {
		cm := &corev1.ConfigMap{}
		if err := f.c.Get(ctx, types.NamespacedName{Namespace: f.ns, Name: configMapName(id)}, cm); err != nil {
			return sysobs.ConfigState{}, err
		}
		d, err := configDigest([]byte(cm.Data[configFile]))
		if err != nil {
			return sysobs.ConfigState{}, err
		}
		f.loaded[id] = d
	}
	return sysobs.ConfigState{Digest: f.loaded[id]}, nil
}

// TestEnvtestReconcile drives the reconciler against a real API server:
// story 1's NatsCluster becomes its objects and reports Ready, Settled and
// Progressing as its servers come up. No StatefulSet controller runs, so
// pod readiness is written into StatefulSet status by the test.
func TestEnvtestReconcile(t *testing.T) {
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

	obs := &fakeObserver{}
	r := &Reconciler{Client: c, Observer: obs, ControllerNamespace: "nats-operator"}
	ctx := t.Context()

	newCluster := func(t *testing.T, ns string, mutate func(*clusterv1beta1.NatsCluster)) *clusterv1beta1.NatsCluster {
		t.Helper()
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		nc := storyCluster(t)
		nc.Namespace = ns
		mutate(nc)
		require.NoError(t, c.Create(ctx, nc))
		return nc
	}
	reconcile := func(t *testing.T, nc *clusterv1beta1.NatsCluster) (ctrl.Result, *clusterv1beta1.NatsCluster) {
		t.Helper()
		key := types.NamespacedName{Namespace: nc.Namespace, Name: nc.Name}
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		got := &clusterv1beta1.NatsCluster{}
		require.NoError(t, c.Get(ctx, key, got))
		return res, got
	}
	condition := func(t *testing.T, nc *clusterv1beta1.NatsCluster, typ string, status metav1.ConditionStatus, reason string) {
		t.Helper()
		cond := meta.FindStatusCondition(nc.Status.Conditions, typ)
		require.NotNil(t, cond, typ)
		require.Equal(t, status, cond.Status, "%s: %s", typ, cond.Message)
		require.Equal(t, reason, cond.Reason, "%s: %s", typ, cond.Message)
		require.Equal(t, nc.Generation, cond.ObservedGeneration)
	}
	statefulSets := func(t *testing.T, ns string) map[string]*appsv1.StatefulSet {
		t.Helper()
		var list appsv1.StatefulSetList
		require.NoError(t, c.List(ctx, &list, client.InNamespace(ns)))
		out := map[string]*appsv1.StatefulSet{}
		for i := range list.Items {
			out[list.Items[i].Name] = &list.Items[i]
		}
		return out
	}

	t.Run("story 1", func(t *testing.T) {
		nc := newCluster(t, "story1", func(*clusterv1beta1.NatsCluster) {})
		res, got := reconcile(t, nc)
		require.Equal(t, resyncUnsettled, res.RequeueAfter)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonCreating)
		condition(t, got, ConditionReady, metav1.ConditionFalse, ReasonQuorumUnavailable)
		condition(t, got, ConditionSettled, metav1.ConditionUnknown, ReasonObservationFailed)
		require.Equal(t, "nats://demo.story1.svc:4222", got.Status.Endpoints.Client)
		require.Equal(t, "http://demo-headless.story1.svc:8222", got.Status.Endpoints.Monitor)
		revision := got.Status.Config.Revision
		require.NotEmpty(t, revision)

		sets := statefulSets(t, "story1")
		require.Len(t, sets, 3)
		for _, name := range []string{"demo-0", "demo-1", "demo-2"} {
			sts := sets[name]
			require.NotNil(t, sts, name)
			require.True(t, metav1.IsControlledBy(sts, got))
			require.Equal(t, revision, sts.Annotations[AnnotationConfigRevision])
			var cm corev1.ConfigMap
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "story1", Name: name + "-config"}, &cm))
			require.Contains(t, cm.Data[configFile], `"server_name": "`+name+`"`)
			require.True(t, metav1.IsControlledBy(&cm, got))
		}
		for _, name := range []string{"demo", "demo-headless"} {
			var svc corev1.Service
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "story1", Name: name}, &svc))
			require.True(t, metav1.IsControlledBy(&svc, got))
		}
		var pdb policyv1.PodDisruptionBudget
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "story1", Name: "demo"}, &pdb))
		require.Equal(t, 1, pdb.Spec.MaxUnavailable.IntValue())
		var np networkingv1.NetworkPolicy
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "story1", Name: "demo"}, &np))
		require.True(t, metav1.IsControlledBy(&np, got))
		require.Equal(t, networkPolicy(got, "nats-operator").Spec, np.Spec)
		var secret corev1.Secret
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "story1", Name: "demo-routes-tls"}, &secret))
		require.True(t, metav1.IsControlledBy(&secret, got))
		caPEM := secret.Data[caKey]

		t.Run("reconciling again changes nothing", func(t *testing.T) {
			_, again := reconcile(t, got)
			condition(t, again, ConditionProgressing, metav1.ConditionFalse, ReasonUpToDate)
			require.Equal(t, revision, again.Status.Config.Revision)
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "story1", Name: "demo-routes-tls"}, &secret))
			require.Equal(t, caPEM, secret.Data[caKey], "the self-signed CA was regenerated")
		})

		t.Run("servers ready and settled", func(t *testing.T) {
			for _, sts := range statefulSets(t, "story1") {
				sts.Status.Replicas, sts.Status.ReadyReplicas = 1, 1
				require.NoError(t, c.Status().Update(ctx, sts))
			}
			plan, err := Render(got, Inputs{})
			require.NoError(t, err)
			obs.set(settledSnapshot(plan, revision, "demo-1"))
			res, ready := reconcile(t, got)
			require.Equal(t, resyncSettled, res.RequeueAfter)
			condition(t, ready, ConditionReady, metav1.ConditionTrue, ReasonAllServersReady)
			condition(t, ready, ConditionSettled, metav1.ConditionTrue, ReasonAllGroupsCurrent)
			condition(t, ready, ConditionProgressing, metav1.ConditionFalse, ReasonUpToDate)
			require.Equal(t, "2.15.0", ready.Status.Version)
			require.Equal(t, int32(3), ready.Status.ReadyReplicas)
			require.Equal(t, "demo-1", ready.Status.JetStream.MetaLeader)
			require.Equal(t, "3Gi", ready.Status.JetStream.Limits.MaxMemoryStore.String())
			require.Equal(t, "19Gi", ready.Status.JetStream.Limits.MaxFileStore.String())
			for _, s := range ready.Status.Servers {
				require.True(t, s.Ready)
				require.Equal(t, revision, s.ConfigRevision)
			}
			obs.set(nil)
		})

		t.Run("a restart waits for an observed cluster", func(t *testing.T) {
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(got), got))
			got.Spec.Version = "2.15.1"
			require.NoError(t, c.Update(ctx, got))
			_, changed := reconcile(t, got)
			condition(t, changed, ConditionProgressing, metav1.ConditionTrue, ReasonRollingRestart)
			require.NotEqual(t, revision, changed.Status.Config.Revision)
			require.Equal(t, clusterv1beta1.ConfigAppliedByRestart, changed.Status.Config.AppliedBy)
			require.Equal(t, "version 2.15.0 -> 2.15.1 is restart-only", changed.Status.Config.RestartReason)
			for _, sts := range statefulSets(t, "story1") {
				require.Equal(t, revision, sts.Annotations[AnnotationConfigRevision])
				require.Equal(t, "nats:2.15.0", sts.Spec.Template.Spec.Containers[0].Image)
			}
		})
	})

	t.Run("config changes", func(t *testing.T) {
		clock := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
		fake := &fakeReloader{c: c}
		cobs := &fakeObserver{}
		rr := &Reconciler{Client: c, Observer: cobs, Now: func() time.Time { return clock },
			Reloader: func(context.Context, *clusterv1beta1.NatsCluster) (ServerReloader, error) { return fake, nil }}
		reconcileWith := func(t *testing.T, nc *clusterv1beta1.NatsCluster) *clusterv1beta1.NatsCluster {
			t.Helper()
			_, err := rr.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
			require.NoError(t, err)
			got := &clusterv1beta1.NatsCluster{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(nc), got))
			return got
		}
		setUp := func(t *testing.T, ns string, change func(*clusterv1beta1.NatsClusterSpec)) (*clusterv1beta1.NatsCluster, string) {
			t.Helper()
			nc := newCluster(t, ns, func(*clusterv1beta1.NatsCluster) {})
			got := reconcileWith(t, nc)
			first := got.Status.Config.Revision
			for _, sts := range statefulSets(t, ns) {
				sts.Status.Replicas, sts.Status.ReadyReplicas = 1, 1
				require.NoError(t, c.Status().Update(ctx, sts))
			}
			plan, err := Render(got, Inputs{})
			require.NoError(t, err)
			snap := settledSnapshot(plan, first, "demo-0")
			for i := range snap.Servers {
				snap.Servers[i].ID = snap.Servers[i].Name
			}
			cobs.set(snap)
			fake.reset(ns)
			change(&got.Spec)
			require.NoError(t, c.Update(ctx, got))
			return got, first
		}
		configMap := func(t *testing.T, ns, server string) *corev1.ConfigMap {
			t.Helper()
			cm := &corev1.ConfigMap{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: ns, Name: server + "-config"}, cm))
			return cm
		}
		retag := func(s *clusterv1beta1.NatsClusterSpec) { s.ServerTags = map[string]string{"az": "b"} }

		t.Run("a reloadable change reloads", func(t *testing.T) {
			nc, first := setUp(t, "reload", retag)
			got := reconcileWith(t, nc)
			condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonUpToDate)
			require.Equal(t, clusterv1beta1.ConfigAppliedByReload, got.Status.Config.AppliedBy)
			require.Empty(t, got.Status.Config.RestartReason)
			require.NotEqual(t, first, got.Status.Config.Revision)
			for name, sts := range statefulSets(t, "reload") {
				require.Equal(t, got.Status.Config.Revision, sts.Annotations[AnnotationConfigRevision], name)
				require.Equal(t, "nats:2.15.0", sts.Spec.Template.Spec.Containers[0].Image)
				require.Contains(t, configMap(t, "reload", name).Data[configFile], `"az:b"`)
			}
			require.ElementsMatch(t, []string{"demo-0", "demo-1", "demo-2"}, fake.reloaded())

			again := reconcileWith(t, got)
			require.Equal(t, clusterv1beta1.ConfigAppliedByReload, again.Status.Config.AppliedBy)
			require.Len(t, fake.reloaded(), 3, "an applied revision was reloaded again")
		})

		t.Run("a rotated route certificate reloads", func(t *testing.T) {
			nc, first := setUp(t, "rotate", func(*clusterv1beta1.NatsClusterSpec) {})
			secret := &corev1.Secret{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "rotate", Name: routesSecretName(nc)}, secret))
			rotated, err := selfSignedRouteSecret(nc, routeDNSNames(nc), clock)
			require.NoError(t, err)
			secret.Data = rotated.Data
			require.NoError(t, c.Update(ctx, secret))

			got := reconcileWith(t, nc)
			require.NotEqual(t, first, got.Status.Config.Revision)
			require.Equal(t, clusterv1beta1.ConfigAppliedByReload, got.Status.Config.AppliedBy)
			for name, sts := range statefulSets(t, "rotate") {
				require.Equal(t, got.Status.Config.Revision, sts.Annotations[AnnotationConfigRevision], name)
			}
			require.ElementsMatch(t, []string{"demo-0", "demo-1", "demo-2"}, fake.reloaded())
		})

		t.Run("a route certificate due for renewal is renewed and reloads", func(t *testing.T) {
			nc, first := setUp(t, "renew", func(*clusterv1beta1.NatsClusterSpec) {})
			before := &corev1.Secret{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "renew", Name: routesSecretName(nc)}, before))
			start := clock
			clock = clock.Add(selfSignedValidity - selfSignedRenewBefore)
			t.Cleanup(func() { clock = start })

			got := reconcileWith(t, nc)
			after := &corev1.Secret{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "renew", Name: routesSecretName(nc)}, after))
			require.Equal(t, clock.Add(selfSignedValidity), mountedCert(after).NotAfter)
			require.NotEqual(t, first, got.Status.Config.Revision)
			require.Equal(t, clusterv1beta1.ConfigAppliedByReload, got.Status.Config.AppliedBy)
			require.ElementsMatch(t, []string{"demo-0", "demo-1", "demo-2"}, fake.reloaded())
		})

		t.Run("an unconfirmed reload falls back to a restart", func(t *testing.T) {
			nc, _ := setUp(t, "lag", retag)
			fake.lag = true
			t.Cleanup(func() { fake.lag = false })
			got := reconcileWith(t, nc)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonReloadPending)
			require.Equal(t, clusterv1beta1.ConfigAppliedByReload, got.Status.Config.AppliedBy)

			clock = clock.Add(reloadWindow + time.Second)
			got = reconcileWith(t, got)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRollingRestart)
			require.Equal(t, clusterv1beta1.ConfigAppliedByRestart, got.Status.Config.AppliedBy)
			require.Contains(t, got.Status.Config.RestartReason, "did not load revision "+got.Status.Config.Revision)
			cm := configMap(t, "lag", "demo-0")
			require.Equal(t, string(clusterv1beta1.ConfigAppliedByRestart), cm.Annotations[AnnotationConfigApply])

			calls := len(fake.calls)
			got = reconcileWith(t, got)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRollingRestart)
			require.Len(t, fake.calls, calls, "a revision marked for restart was reloaded")
			for _, sts := range statefulSets(t, "lag") {
				require.NotEqual(t, got.Status.Config.Revision, sts.Annotations[AnnotationConfigRevision])
			}
		})

		t.Run("a rejected reload falls back to a restart", func(t *testing.T) {
			nc, _ := setUp(t, "rejected", retag)
			fake.reject = fmt.Errorf("%w: RELOAD: 500 config reload not supported", sysobs.ErrServer)
			t.Cleanup(func() { fake.reject = nil })
			got := reconcileWith(t, nc)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRollingRestart)
			require.Equal(t, clusterv1beta1.ConfigAppliedByRestart, got.Status.Config.AppliedBy)
			require.Contains(t, got.Status.Config.RestartReason, "reload failed: ")
			require.Contains(t, got.Status.Config.RestartReason, "config reload not supported")
		})

		t.Run("a mixed change restarts", func(t *testing.T) {
			nc, first := setUp(t, "mixed", func(s *clusterv1beta1.NatsClusterSpec) {
				retag(s)
				s.JetStream.Domain = "hub"
			})
			got := reconcileWith(t, nc)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRollingRestart)
			require.Equal(t, clusterv1beta1.ConfigAppliedByRestart, got.Status.Config.AppliedBy)
			require.Equal(t, "jetstream.domain is restart-only", got.Status.Config.RestartReason)
			require.Empty(t, fake.calls)
			require.Equal(t, first, configMap(t, "mixed", "demo-0").Annotations[AnnotationConfigRevision], "a restart-only revision was written")
		})

		t.Run("without a system account connection a change restarts", func(t *testing.T) {
			nc, _ := setUp(t, "nosys", retag)
			got, err := func() (*clusterv1beta1.NatsCluster, error) {
				plain := &Reconciler{Client: c, Observer: cobs, Now: rr.Now}
				_, err := plain.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
				got := &clusterv1beta1.NatsCluster{}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(nc), got))
				return got, err
			}()
			require.NoError(t, err)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRollingRestart)
			require.Equal(t, "no system account connection to reload over", got.Status.Config.RestartReason)
		})
	})

	t.Run("a version bump rolls one server at a time", func(t *testing.T) {
		const ns = "rollout"
		robs := &fakeObserver{}
		rec := events.NewFakeRecorder(100)
		rr := &Reconciler{Client: c, Observer: robs, Recorder: rec}
		reconcileWith := func(t *testing.T, nc *clusterv1beta1.NatsCluster) *clusterv1beta1.NatsCluster {
			t.Helper()
			_, err := rr.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
			require.NoError(t, err)
			got := &clusterv1beta1.NatsCluster{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(nc), got))
			return got
		}
		running := map[string][2]string{}
		observe := func() {
			snap := &sysobs.Snapshot{}
			var members []sysobs.Member
			for _, name := range []string{"demo-0", "demo-1", "demo-2"} {
				snap.Servers = append(snap.Servers, sysobs.Server{Name: name, Version: running[name][0], JetStream: true,
					Metadata: map[string]string{MetadataConfigRevision: running[name][1]}})
				members = append(members, sysobs.Member{Server: name, Current: true})
			}
			snap.Groups = []sysobs.Group{{Kind: sysobs.KindMeta, Leader: "demo-0", Members: members}}
			robs.set(snap)
		}
		podUp := func(t *testing.T, sts *appsv1.StatefulSet) {
			t.Helper()
			sts.Status.ObservedGeneration = sts.Generation
			sts.Status.Replicas, sts.Status.UpdatedReplicas, sts.Status.ReadyReplicas = 1, 1, 1
			require.NoError(t, c.Status().Update(ctx, sts))
			running[sts.Name] = [2]string{runningVersion(sts), sts.Spec.Template.Annotations[AnnotationConfigRevision]}
		}
		changed := func(t *testing.T, before map[string]string) []string {
			t.Helper()
			var out []string
			for name, sts := range statefulSets(t, ns) {
				if sts.Spec.Template.Annotations[AnnotationConfigRevision] != before[name] {
					out = append(out, name)
				}
			}
			return out
		}
		templates := func(t *testing.T) map[string]string {
			t.Helper()
			out := map[string]string{}
			for name, sts := range statefulSets(t, ns) {
				out[name] = sts.Spec.Template.Annotations[AnnotationConfigRevision]
			}
			return out
		}

		nc := newCluster(t, ns, func(*clusterv1beta1.NatsCluster) {})
		got := reconcileWith(t, nc)
		first := got.Status.Config.Revision
		for _, sts := range statefulSets(t, ns) {
			podUp(t, sts)
		}
		observe()
		got = reconcileWith(t, got)
		condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonUpToDate)
		require.Nil(t, got.Status.Rollout)

		got.Spec.Version = "2.15.1"
		require.NoError(t, c.Update(ctx, got))

		var order []string
		for step := 1; step <= 3; step++ {
			before := templates(t)
			if step == 2 {
				got.Spec.Rollout = &clusterv1beta1.Rollout{Paused: true}
				require.NoError(t, c.Update(ctx, got))
				got = reconcileWith(t, got)
				condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRolloutPaused)
				require.Empty(t, changed(t, before), "stepped while paused")

				got.Annotations = map[string]string{clusterv1beta1.AnnotationForceStep: "demo-1"}
				require.NoError(t, c.Update(ctx, got))
			}
			got = reconcileWith(t, got)
			stepped := changed(t, before)
			require.Len(t, stepped, 1, "step %d", step)
			name := stepped[0]
			order = append(order, name)
			target := got.Status.Config.Revision
			require.NotEqual(t, first, target)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRollingRestart)
			require.Equal(t, name, got.Status.Rollout.Current)
			require.Len(t, got.Status.Rollout.Updated, step-1)
			require.Len(t, got.Status.Rollout.Pending, 3-step)
			require.Equal(t, GateSettled, got.Status.Rollout.Gate.WaitingFor)
			require.NotContains(t, got.Annotations, clusterv1beta1.AnnotationForceStep)

			sts := statefulSets(t, ns)[name]
			require.Equal(t, "nats:2.15.1", sts.Spec.Template.Spec.Containers[0].Image)
			require.Equal(t, target, sts.Annotations[AnnotationConfigRevision])
			var cm corev1.ConfigMap
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name + "-config"}, &cm))
			require.Equal(t, target, cm.Annotations[AnnotationConfigRevision])
			require.Equal(t, string(clusterv1beta1.ConfigAppliedByRestart), cm.Annotations[AnnotationConfigApply])
			require.Contains(t, cm.Data[configFile], `"config_revision": "`+target+`"`)

			got = reconcileWith(t, got)
			require.Empty(t, changed(t, templates(t)), "stepped again before %s was up", name)
			require.Equal(t, before[name], running[name][1], "%s restarted before its pod came up", name)

			podUp(t, sts)
			observe()
			if step == 2 {
				got.Spec.Rollout = nil
				require.NoError(t, c.Update(ctx, got))
			}
		}
		require.Equal(t, []string{"demo-2", "demo-1", "demo-0"}, order)
		require.Equal(t, []string{
			"Normal RolloutStep restarting demo-2",
			"Normal RolloutStep restarting demo-1",
			"Normal RolloutStep restarting demo-0",
		}, recorded(rec))

		got = reconcileWith(t, got)
		condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonUpToDate)
		require.Nil(t, got.Status.Rollout)
		require.Equal(t, "2.15.1", got.Status.Version)
		require.Equal(t, clusterv1beta1.ConfigAppliedByRestart, got.Status.Config.AppliedBy)
		require.Equal(t, "version 2.15.0 -> 2.15.1 is restart-only", got.Status.Config.RestartReason)
	})

	t.Run("unsupported fields are refused", func(t *testing.T) {
		nc := newCluster(t, "unsupported", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.LeafRemotes = []clusterv1beta1.LeafRemote{{ConnectionRef: natsv1beta1.ObjectReference{Name: "hub"}, LocalSystemAccount: true}}
			nc.Spec.JetStream = nil
		})
		_, got := reconcile(t, nc)
		condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonUnsupportedSpec)
		require.Contains(t, meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing).Message, "leafRemotes[0].localSystemAccount without auth")
		require.Empty(t, statefulSets(t, "unsupported"))
	})

	t.Run("objects it does not control are left untouched", func(t *testing.T) {
		ns := "uncontrolled"
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: ns},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app": "other"},
				Ports:    []corev1.ServicePort{{Name: "http", Port: 80}},
			},
		}
		deny := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: ns},
			Spec:       networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}},
		}
		require.NoError(t, c.Create(ctx, svc))
		require.NoError(t, c.Create(ctx, deny))
		nc := storyCluster(t)
		nc.Namespace = ns
		require.NoError(t, c.Create(ctx, nc))

		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
		var nce *notControlledError
		require.ErrorAs(t, err, &nce)
		require.Equal(t, []string{"Service demo", "NetworkPolicy demo"}, nce.Objects)

		got := &clusterv1beta1.NatsCluster{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(nc), got))
		condition(t, got, ConditionReady, metav1.ConditionFalse, ReasonReconcileFailed)
		require.Equal(t, "not controlled by this NatsCluster: Service demo, NetworkPolicy demo", meta.FindStatusCondition(got.Status.Conditions, ConditionReady).Message)
		condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonReconcileFailed)

		var haveSvc corev1.Service
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(svc), &haveSvc))
		require.Empty(t, haveSvc.OwnerReferences)
		require.Equal(t, svc.Spec.Selector, haveSvc.Spec.Selector)
		require.Equal(t, svc.Spec.Ports[0].Port, haveSvc.Spec.Ports[0].Port)
		require.Len(t, haveSvc.Spec.Ports, 1)
		var haveNP networkingv1.NetworkPolicy
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(deny), &haveNP))
		require.Empty(t, haveNP.OwnerReferences)
		require.Equal(t, deny.Spec, haveNP.Spec)
		require.Empty(t, statefulSets(t, ns))
	})

	t.Run("monitor.networkPolicy false deletes the NetworkPolicy", func(t *testing.T) {
		nc := newCluster(t, "netpol", func(*clusterv1beta1.NatsCluster) {})
		_, got := reconcile(t, nc)
		key := types.NamespacedName{Namespace: "netpol", Name: "demo"}
		require.NoError(t, c.Get(ctx, key, &networkingv1.NetworkPolicy{}))

		got.Spec.Monitor = &clusterv1beta1.Monitor{NetworkPolicy: ptr.To(false)}
		require.NoError(t, c.Update(ctx, got))
		reconcile(t, got)
		require.True(t, apierrors.IsNotFound(c.Get(ctx, key, &networkingv1.NetworkPolicy{})), "NetworkPolicy kept")
	})

	t.Run("client TLS", func(t *testing.T) {
		nc := newCluster(t, "clienttls", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.TLS = &clusterv1beta1.ListenerTLS{CertificateSource: clusterv1beta1.CertificateSource{SecretRef: &natsv1beta1.SecretReference{Name: "clients"}}}
		})
		_, got := reconcile(t, nc)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonClientCertNotReady)
		require.Equal(t, "Secret clients does not exist", meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing).Message)
		require.Empty(t, statefulSets(t, "clienttls"))

		cert, err := selfSignedRouteSecret(nc, clientHosts(nc), r.now())
		require.NoError(t, err)
		cert.Name = "clients"
		require.NoError(t, c.Create(ctx, cert))
		_, got = reconcile(t, got)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonCreating)
		require.Equal(t, "tls://demo.clienttls.svc:4222", got.Status.Endpoints.Client)
		cm := &corev1.ConfigMap{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "clienttls", Name: "demo-0-config"}, cm))
		var cfg Config
		require.NoError(t, json.Unmarshal([]byte(cm.Data[configFile]), &cfg))
		require.Equal(t, &ListenerTLSConfig{CertFile: clientTLSDir + "/tls.crt", KeyFile: clientTLSDir + "/tls.key"}, cfg.TLS)

		got.Spec.TLS.CertManager = &clusterv1beta1.CertManagerCertificate{IssuerRef: clusterv1beta1.IssuerReference{Name: "ca"}}
		err = c.Update(ctx, got)
		require.True(t, apierrors.IsInvalid(err), "%v", err)
		require.ErrorContains(t, err, "set exactly one of secretRef and certManager")
	})

	t.Run("gateway", func(t *testing.T) {
		west := storySupercluster(t, "west")
		nc := newCluster(t, "gateway", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Gateway = west.Spec.Gateway.DeepCopy()
			nc.Spec.Gateway.Remotes[1].Name = "demo"
			nc.Spec.Gateway.TLS = &clusterv1beta1.ListenerTLS{CertificateSource: clusterv1beta1.CertificateSource{
				SecretRef: &natsv1beta1.SecretReference{Name: "gw"},
			}}
		})
		gatewayService := func(t *testing.T) *corev1.Service {
			t.Helper()
			svc := &corev1.Service{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "gateway", Name: "demo-gateway"}, svc))
			return svc
		}

		_, got := reconcile(t, nc)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonGatewayCertNotReady)
		require.Contains(t, meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing).Message, "Secret gw does not exist")
		require.Empty(t, statefulSets(t, "gateway"))

		svc := gatewayService(t)
		require.True(t, metav1.IsControlledBy(svc, got))
		require.Equal(t, corev1.ServiceTypeLoadBalancer, svc.Spec.Type)
		for k, v := range west.Spec.Gateway.Service.Annotations {
			require.Equal(t, v, svc.Annotations[k], k)
		}
		require.Len(t, svc.Spec.Ports, 1)
		require.Equal(t, int32(PortGateway), svc.Spec.Ports[0].Port)
		nodePort := svc.Spec.Ports[0].NodePort
		require.NotZero(t, nodePort)

		own, err := selfSignedRouteSecret(nc, []string{"nats-west.example.net"}, r.now())
		require.NoError(t, err)
		own.Name = "gw"
		ca := own.Data[caKey]
		delete(own.Data, caKey)
		require.NoError(t, c.Create(ctx, own))
		_, got = reconcile(t, got)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonGatewayCertNotReady)
		require.Equal(t, "Secret gw has no ca.crt", meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing).Message)
		require.Empty(t, statefulSets(t, "gateway"), "a gateway without a CA accepts a peer certificate from any public root")

		own.Data[caKey] = ca
		require.NoError(t, c.Update(ctx, own))
		_, got = reconcile(t, got)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonCreating)
		require.Len(t, statefulSets(t, "gateway"), 3)
		cm := &corev1.ConfigMap{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "gateway", Name: "demo-0-config"}, cm))
		var cfg Config
		require.NoError(t, json.Unmarshal([]byte(cm.Data[configFile]), &cfg))
		require.Equal(t, &GatewayConfig{
			Name: "demo", Listen: "0.0.0.0:7222", Advertise: "nats-west.example.net:7222", RejectUnknown: true,
			TLS:      &TLSConfig{CertFile: gatewayTLSDir + "/tls.crt", KeyFile: gatewayTLSDir + "/tls.key", CAFile: gatewayTLSDir + "/ca.crt", Verify: true},
			Gateways: []RemoteGatewayConfig{{Name: "east", URLs: []string{"tls://nats-east.example.net:7222"}}},
		}, cfg.Gateway)

		plan, err := Render(got, Inputs{})
		require.NoError(t, err)
		snap := settledSnapshot(plan, plan.Revision, "demo-0")
		for i := range snap.Servers {
			snap.Servers[i].Gateways = &sysobs.Gateways{Outbound: []string{"east"}, Inbound: map[string]int{"east": 1}}
		}
		obs.set(snap)
		_, got = reconcile(t, got)
		condition(t, got, ConditionGatewaysConnected, metav1.ConditionTrue, ReasonAllMembersReachable)
		require.Equal(t, []clusterv1beta1.GatewayStatus{{Name: "east", Connected: true, Inbound: 3, Outbound: 3}}, got.Status.Gateways)
		require.Equal(t, "nats-west.example.net:7222", got.Status.Endpoints.Gateway)
		require.Equal(t, nodePort, gatewayService(t).Spec.Ports[0].NodePort, "the node port was reallocated")

		t.Run("the template's load balancer fields reach the Service", func(t *testing.T) {
			got.Spec.Gateway.Service.LoadBalancerSourceRanges = []string{"10.20.0.0/16", "2001:db8:20::/56"}
			require.NoError(t, c.Update(ctx, got))
			_, got = reconcile(t, got)
			require.Equal(t, []string{"10.20.0.0/16", "2001:db8:20::/56"}, gatewayService(t).Spec.LoadBalancerSourceRanges)

			got.Spec.Gateway.Service.LoadBalancerClass = ptr.To("a.example/lb")
			err := c.Update(ctx, got)
			require.True(t, apierrors.IsInvalid(err), "%v", err)
			require.ErrorContains(t, err, "loadBalancerClass cannot change while type stays LoadBalancer")

			_, got = reconcile(t, got)
			got.Spec.Gateway.Service.LoadBalancerSourceRanges = nil
			require.NoError(t, c.Update(ctx, got))
			_, got = reconcile(t, got)
			require.Nil(t, gatewayService(t).Spec.LoadBalancerSourceRanges)
		})

		t.Run("a class Kubernetes refuses on the Service is reported", func(t *testing.T) {
			template := got.Spec.Gateway.Service.DeepCopy()
			got.Spec.Gateway.Service = nil
			require.NoError(t, c.Update(ctx, got))
			got.Spec.Gateway.Service = template
			got.Spec.Gateway.Service.LoadBalancerClass = ptr.To("a.example/lb")
			require.NoError(t, c.Update(ctx, got), "a template removed and set again between reconciles is no transition")

			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(got)})
			require.ErrorContains(t, err, "may not change once set")
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(got), got))
			condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonReconcileFailed)
			require.Contains(t, meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing).Message, "spec.loadBalancerClass")
			require.Nil(t, gatewayService(t).Spec.LoadBalancerClass)

			got.Spec.Gateway.Service = nil
			require.NoError(t, c.Update(ctx, got))
			_, got = reconcile(t, got)
			got.Spec.Gateway.Service = template
			got.Spec.Gateway.Service.LoadBalancerClass = ptr.To("a.example/lb")
			require.NoError(t, c.Update(ctx, got))
			_, got = reconcile(t, got)
			require.Equal(t, ptr.To("a.example/lb"), gatewayService(t).Spec.LoadBalancerClass)

			got.Spec.Gateway.Service.Type = corev1.ServiceTypeClusterIP
			got.Spec.Gateway.Service.LoadBalancerClass = nil
			require.NoError(t, c.Update(ctx, got))
			_, got = reconcile(t, got)
			svc := gatewayService(t)
			require.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
			require.Nil(t, svc.Spec.LoadBalancerClass, "the API server drops the class with the type")
		})

		t.Run("a remote change restarts", func(t *testing.T) {
			got.Spec.Gateway.Remotes = append(got.Spec.Gateway.Remotes, clusterv1beta1.GatewayRemote{Name: "south", URL: "tls://nats-south.example.net:7222"})
			require.NoError(t, c.Update(ctx, got))
			_, got = reconcile(t, got)
			require.Equal(t, clusterv1beta1.ConfigAppliedByRestart, got.Status.Config.AppliedBy)
			require.Equal(t, "gateway.gateways is restart-only", got.Status.Config.RestartReason)
			condition(t, got, ConditionGatewaysConnected, metav1.ConditionFalse, ReasonMembersUnreachable)
		})

		t.Run("unsetting the service deletes it", func(t *testing.T) {
			got.Spec.Gateway.Service = nil
			require.NoError(t, c.Update(ctx, got))
			_, got = reconcile(t, got)
			err := c.Get(ctx, types.NamespacedName{Namespace: "gateway", Name: "demo-gateway"}, &corev1.Service{})
			require.True(t, apierrors.IsNotFound(err), "got %v", err)
		})

		t.Run("unsetting the gateway drops its status", func(t *testing.T) {
			got.Spec.Gateway = nil
			require.NoError(t, c.Update(ctx, got))
			_, got = reconcile(t, got)
			require.Nil(t, got.Status.Gateways)
			require.Empty(t, got.Status.Endpoints.Gateway)
			require.Nil(t, meta.FindStatusCondition(got.Status.Conditions, ConditionGatewaysConnected))
		})
	})

	t.Run("named route certificate", func(t *testing.T) {
		nc := newCluster(t, "named", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Replicas = 1
			nc.Spec.Routes = &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{CertificateSource: clusterv1beta1.CertificateSource{
				SecretRef: &natsv1beta1.SecretReference{Name: "mine"},
			}}}
		})
		_, got := reconcile(t, nc)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRouteCertNotReady)
		require.Empty(t, statefulSets(t, "named"))

		own, err := selfSignedRouteSecret(nc, routeDNSNames(nc), r.now())
		require.NoError(t, err)
		own.Name = "mine"
		require.NoError(t, c.Create(ctx, own))
		_, got = reconcile(t, got)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonCreating)
		sts := statefulSets(t, "named")["demo-0"]
		require.NotNil(t, sts)
		var secretVolume string
		for _, v := range sts.Spec.Template.Spec.Volumes {
			if v.Secret != nil {
				secretVolume = v.Secret.SecretName
			}
		}
		require.Equal(t, "mine", secretVolume)
	})

	t.Run("cert-manager not installed", func(t *testing.T) {
		nc := newCluster(t, "certmanager", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Routes = &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{CertificateSource: clusterv1beta1.CertificateSource{
				CertManager: &clusterv1beta1.CertManagerCertificate{IssuerRef: clusterv1beta1.IssuerReference{Name: "ca"}},
			}}}
		})
		_, got := reconcile(t, nc)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRouteCertNotReady)
		require.Contains(t, meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing).Message, "cert-manager is not installed")
		require.Empty(t, statefulSets(t, "certmanager"))
	})

	t.Run("auth plane", func(t *testing.T) {
		p := mintPlane(t)
		fake := &fakeReloader{c: c}
		aobs := &fakeObserver{}
		ar := &Reconciler{Client: c, Observer: aobs,
			Reloader: func(context.Context, *clusterv1beta1.NatsCluster) (ServerReloader, error) { return fake, nil }}
		reconcileAuth := func(t *testing.T, nc *clusterv1beta1.NatsCluster) *clusterv1beta1.NatsCluster {
			t.Helper()
			_, err := ar.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
			require.NoError(t, err)
			got := &clusterv1beta1.NatsCluster{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(nc), got))
			return got
		}
		story2 := storyAuthCluster(t)
		newAuthCluster := func(t *testing.T, ns string, mutate func(*clusterv1beta1.Auth)) *clusterv1beta1.NatsCluster {
			t.Helper()
			return newCluster(t, ns, func(nc *clusterv1beta1.NatsCluster) {
				nc.Spec.Auth = story2.Spec.Auth.DeepCopy()
				mutate(nc.Spec.Auth)
			})
		}
		literal := func(ns string, trust *Trust) *natsv1beta1.NatsOperatorTrust {
			return &natsv1beta1.NatsOperatorTrust{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "demo"},
				Spec:       natsv1beta1.NatsOperatorTrustSpec{OperatorJWT: trust.OperatorJWT, SystemAccountJWT: trust.SystemAccountJWT},
			}
		}
		requireTrustRendered := func(t *testing.T, ns string, trust *Trust) {
			t.Helper()
			sets := statefulSets(t, ns)
			require.Len(t, sets, 3)
			for name := range sets {
				cm := &corev1.ConfigMap{}
				require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name + "-config"}, cm))
				var m map[string]any
				require.NoError(t, json.Unmarshal([]byte(cm.Data[configFile]), &m))
				require.Equal(t, trust.OperatorJWT, m["operator"], name)
				require.Equal(t, trust.SystemAccount, m["system_account"], name)
				require.Equal(t, map[string]any{trust.SystemAccount: trust.SystemAccountJWT}, m["resolver_preload"], name)
				require.Equal(t, "full", m["resolver"].(map[string]any)["type"], name)
			}
		}

		t.Run("literal trust renders the trust roots", func(t *testing.T) {
			const ns = "auth-literal"
			nc := newAuthCluster(t, ns, func(*clusterv1beta1.Auth) {})
			require.NoError(t, c.Create(ctx, literal(ns, p.trust)))
			got := reconcileAuth(t, nc)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonCreating)
			requireTrustRendered(t, ns, p.trust)

			t.Run("a trust change is restart-only", func(t *testing.T) {
				first := got.Status.Config.Revision
				for _, sts := range statefulSets(t, ns) {
					sts.Status.Replicas, sts.Status.ReadyReplicas = 1, 1
					require.NoError(t, c.Status().Update(ctx, sts))
				}
				plan, err := Render(got, Inputs{Trust: p.trust})
				require.NoError(t, err)
				snap := settledSnapshot(plan, first, "demo-0")
				for i := range snap.Servers {
					snap.Servers[i].ID = snap.Servers[i].Name
				}
				aobs.set(snap)
				fake.reset(ns)
				settled := reconcileAuth(t, got)
				condition(t, settled, ConditionSettled, metav1.ConditionTrue, ReasonAllGroupsCurrent)
				condition(t, settled, ConditionProgressing, metav1.ConditionFalse, ReasonUpToDate)

				resigned := p.op
				resigned.Signing = append(slices.Clone(p.op.Signing), jwtplane.SigningKey{Name: "next", Pair: newTestPair(t, nkeys.PrefixByteOperator)})
				next := p.sign(t, resigned, jwtplane.SystemAccount{Name: "SYS", Keys: p.sys})
				trust := &natsv1beta1.NatsOperatorTrust{}
				require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "demo"}, trust))
				trust.Spec.OperatorJWT = next.OperatorJWT
				require.NoError(t, c.Update(ctx, trust))

				changed := reconcileAuth(t, settled)
				condition(t, changed, ConditionProgressing, metav1.ConditionTrue, ReasonRollingRestart)
				require.NotEqual(t, first, changed.Status.Config.Revision)
				require.Equal(t, clusterv1beta1.ConfigAppliedByRestart, changed.Status.Config.AppliedBy)
				require.Equal(t, "operator is restart-only", changed.Status.Config.RestartReason)
				require.Empty(t, fake.reloaded())
			})
		})

		t.Run("adding or removing auth restarts every server together", func(t *testing.T) {
			const ns = "auth-change"
			rec := events.NewFakeRecorder(100)
			rr := &Reconciler{Client: c, Observer: aobs, Recorder: rec,
				Reloader: func(context.Context, *clusterv1beta1.NatsCluster) (ServerReloader, error) { return fake, nil }}
			reconcileChange := func(t *testing.T, nc *clusterv1beta1.NatsCluster) *clusterv1beta1.NatsCluster {
				t.Helper()
				_, err := rr.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
				require.NoError(t, err)
				got := &clusterv1beta1.NatsCluster{}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(nc), got))
				return got
			}
			templates := func(t *testing.T) map[string]string {
				t.Helper()
				out := map[string]string{}
				for name, sts := range statefulSets(t, ns) {
					out[name] = sts.Spec.Template.Annotations[AnnotationConfigRevision]
				}
				return out
			}
			settle := func(t *testing.T, nc *clusterv1beta1.NatsCluster, trust *Trust) {
				t.Helper()
				for _, sts := range statefulSets(t, ns) {
					sts.Status.ObservedGeneration = sts.Generation
					sts.Status.Replicas, sts.Status.UpdatedReplicas, sts.Status.ReadyReplicas = 1, 1, 1
					require.NoError(t, c.Status().Update(ctx, sts))
				}
				plan, err := Render(nc, Inputs{Trust: trust})
				require.NoError(t, err)
				aobs.set(settledSnapshot(plan, nc.Status.Config.Revision, "demo-0"))
				fake.reset(ns)
			}
			requireHeld := func(t *testing.T, nc *clusterv1beta1.NatsCluster, change string, before map[string]string) {
				t.Helper()
				condition(t, nc, ConditionProgressing, metav1.ConditionFalse, ReasonAuthChangeBlocked)
				require.Equal(t, change+" cannot roll out one server at a time, as a server with auth and one without cannot route to each other: "+
					"annotate the NatsCluster cluster.nats-operator.io/restart-all to restart demo-2, demo-1, demo-0 together, or recreate it",
					meta.FindStatusCondition(nc.Status.Conditions, ConditionProgressing).Message)
				require.Equal(t, []string{"demo-2", "demo-1", "demo-0"}, nc.Status.Rollout.Pending)
				require.Equal(t, before, templates(t), "a server restarted across the auth change")
			}

			nc := newCluster(t, ns, func(*clusterv1beta1.NatsCluster) {})
			require.NoError(t, c.Create(ctx, literal(ns, p.trust)))
			got := reconcileChange(t, nc)
			settle(t, got, nil)
			got = reconcileChange(t, got)
			condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonUpToDate)
			first := templates(t)

			got.Spec.Auth = story2.Spec.Auth.DeepCopy()
			require.NoError(t, c.Update(ctx, got))
			for range 2 {
				got = reconcileChange(t, got)
				requireHeld(t, got, "adding auth", first)
			}

			got.Annotations = map[string]string{clusterv1beta1.AnnotationRestartAll: "true"}
			require.NoError(t, c.Update(ctx, got))
			got = reconcileChange(t, got)
			target := got.Status.Config.Revision
			require.NotContains(t, got.Annotations, clusterv1beta1.AnnotationRestartAll)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRollingRestart)
			require.Equal(t, "adding auth: restarting demo-2, demo-1, demo-0 together; waiting for Settled",
				meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing).Message)
			require.Equal(t, map[string]string{"demo-0": target, "demo-1": target, "demo-2": target}, templates(t))
			requireTrustRendered(t, ns, p.trust)
			require.Equal(t, []string{"Normal RolloutStep restarting demo-2, demo-1, demo-0 together"}, recorded(rec))

			aobs.set(nil)
			got = reconcileChange(t, got)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRollingRestart)
			require.Equal(t, GateSettled, got.Status.Rollout.Gate.WaitingFor)

			settle(t, got, p.trust)
			got = reconcileChange(t, got)
			condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonUpToDate)
			require.Nil(t, got.Status.Rollout)

			got.Spec.Auth = nil
			require.NoError(t, c.Update(ctx, got))
			got = reconcileChange(t, got)
			requireHeld(t, got, "removing auth", templates(t))
		})

		t.Run("reference form waits for the auth controller", func(t *testing.T) {
			const ns = "auth-ref"
			nc := newAuthCluster(t, ns, func(*clusterv1beta1.Auth) {})
			trust := &natsv1beta1.NatsOperatorTrust{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "demo"},
				Spec:       natsv1beta1.NatsOperatorTrustSpec{OperatorRef: &natsv1beta1.ObjectReference{Name: "demo"}},
			}
			require.NoError(t, c.Create(ctx, trust))
			got := reconcileAuth(t, nc)
			condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonTrustNotReady)
			require.Empty(t, statefulSets(t, ns))

			trust.Status.OperatorJWT, trust.Status.SystemAccountJWT = p.trust.OperatorJWT, p.trust.SystemAccountJWT
			require.NoError(t, c.Status().Update(ctx, trust))
			got = reconcileAuth(t, got)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonCreating)
			requireTrustRendered(t, ns, p.trust)
		})

		t.Run("missing trust", func(t *testing.T) {
			nc := newAuthCluster(t, "auth-missing", func(*clusterv1beta1.Auth) {})
			got := reconcileAuth(t, nc)
			condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonTrustNotFound)
			require.Empty(t, statefulSets(t, "auth-missing"))
		})

		t.Run("invalid trust", func(t *testing.T) {
			const ns = "auth-invalid"
			nc := newAuthCluster(t, ns, func(*clusterv1beta1.Auth) {})
			bad := literal(ns, p.trust)
			bad.Spec.SystemAccountJWT = mintPlane(t).trust.SystemAccountJWT
			require.NoError(t, c.Create(ctx, bad))
			got := reconcileAuth(t, nc)
			condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonTrustInvalid)
			require.Contains(t, meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing).Message, "is signed by")
			require.Empty(t, statefulSets(t, ns))
		})

		t.Run("a trust in another namespace needs a grant", func(t *testing.T) {
			const ns, trustNS = "auth-cross", "auth-trusts"
			require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: trustNS}}))
			require.NoError(t, c.Create(ctx, literal(trustNS, p.trust)))
			nc := newAuthCluster(t, ns, func(a *clusterv1beta1.Auth) { a.TrustRef.Namespace = trustNS })
			got := reconcileAuth(t, nc)
			condition(t, got, ConditionProgressing, metav1.ConditionFalse, grant.ReasonNoGrant)
			require.Empty(t, statefulSets(t, ns))

			require.NoError(t, c.Create(ctx, &natsv1beta1.NatsReferenceGrant{
				ObjectMeta: metav1.ObjectMeta{Namespace: trustNS, Name: "clusters"},
				Spec: natsv1beta1.NatsReferenceGrantSpec{
					From: []natsv1beta1.ReferenceGrantFrom{{Group: clusterv1beta1.GroupVersion.Group, Kind: "NatsCluster", Namespace: ns}},
					To:   []natsv1beta1.ReferenceGrantTo{{Group: natsv1beta1.GroupVersion.Group, Kind: "NatsOperatorTrust"}},
				},
			}))
			got = reconcileAuth(t, got)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonCreating)
			requireTrustRendered(t, ns, p.trust)
		})

		t.Run("the Memory resolver is refused at apply", func(t *testing.T) {
			require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "auth-memory"}}))
			nc := storyAuthCluster(t)
			nc.Namespace = "auth-memory"
			nc.Spec.Auth.Resolver = "Memory"
			err := c.Create(ctx, nc)
			require.True(t, apierrors.IsInvalid(err), "%v", err)
			require.ErrorContains(t, err, "spec.auth.resolver")
		})
	})

	t.Run("an unset issuer deletes the Certificate", func(t *testing.T) {
		_, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{CRDs: []*apiextensionsv1.CustomResourceDefinition{certificateCRD()}})
		require.NoError(t, err)
		issuer := &clusterv1beta1.CertManagerCertificate{IssuerRef: clusterv1beta1.IssuerReference{Name: "ca"}}
		nc := newCluster(t, "issuer", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Routes = &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{CertificateSource: clusterv1beta1.CertificateSource{CertManager: issuer}}}
			nc.Spec.Leafnodes = &clusterv1beta1.Leafnodes{TLS: &clusterv1beta1.ListenerTLS{CertificateSource: clusterv1beta1.CertificateSource{CertManager: issuer}}}
			nc.Spec.Auth = storyAuthCluster(t).Spec.Auth
		})
		trust := mintPlane(t).trust
		require.NoError(t, c.Create(ctx, &natsv1beta1.NatsOperatorTrust{
			ObjectMeta: metav1.ObjectMeta{Namespace: "issuer", Name: nc.Spec.Auth.TrustRef.Name},
			Spec:       natsv1beta1.NatsOperatorTrustSpec{OperatorJWT: trust.OperatorJWT, SystemAccountJWT: trust.SystemAccountJWT},
		}))
		certificate := func(name string) error {
			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(certificateGVK)
			return c.Get(ctx, types.NamespacedName{Namespace: "issuer", Name: name}, u)
		}
		_, got := reconcile(t, nc)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRouteCertNotReady)
		require.NoError(t, certificate("demo-routes"))
		require.NoError(t, certificate("demo-leafnodes"))

		got.Spec.Routes = nil
		got.Spec.Leafnodes.TLS = nil
		require.NoError(t, c.Update(ctx, got))
		_, got = reconcile(t, got)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonCreating)
		require.True(t, apierrors.IsNotFound(certificate("demo-routes")), "routes Certificate kept")
		require.True(t, apierrors.IsNotFound(certificate("demo-leafnodes")), "leafnodes Certificate kept")
	})
}

// certificateCRD is enough of cert-manager's Certificate CRD for the API
// server to store one.
func certificateCRD() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "certificates.cert-manager.io"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: certificateGVK.Group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "certificates", Singular: "certificate", Kind: certificateGVK.Kind, ListKind: "CertificateList"},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: certificateGVK.Version, Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					Type:                   "object",
					XPreserveUnknownFields: ptr.To(true),
				}},
			}},
		},
	}
}
