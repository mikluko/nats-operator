package natscluster

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// fakeObserver returns whatever snapshot it was last given.
type fakeObserver struct {
	mu   sync.Mutex
	snap *sysobs.Snapshot
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
	r := &Reconciler{Client: c, Observer: obs}
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
		require.Equal(t, "http://demo.story1.svc:8222", got.Status.Endpoints.Monitor)
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
			plan, err := Render(got)
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

		t.Run("a spec change is not rolled out", func(t *testing.T) {
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(got), got))
			got.Spec.Version = "2.15.1"
			require.NoError(t, c.Update(ctx, got))
			_, changed := reconcile(t, got)
			condition(t, changed, ConditionProgressing, metav1.ConditionTrue, ReasonRolloutPending)
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
		// setUp creates a NatsCluster in ns, brings its servers up on the
		// first revision, and applies change to its spec.
		setUp := func(t *testing.T, ns string, change func(*clusterv1beta1.NatsClusterSpec)) (*clusterv1beta1.NatsCluster, string) {
			t.Helper()
			nc := newCluster(t, ns, func(*clusterv1beta1.NatsCluster) {})
			got := reconcileWith(t, nc)
			first := got.Status.Config.Revision
			for _, sts := range statefulSets(t, ns) {
				sts.Status.Replicas, sts.Status.ReadyReplicas = 1, 1
				require.NoError(t, c.Status().Update(ctx, sts))
			}
			plan, err := Render(got)
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

		t.Run("an unconfirmed reload falls back to a restart", func(t *testing.T) {
			nc, _ := setUp(t, "lag", retag)
			fake.lag = true
			t.Cleanup(func() { fake.lag = false })
			got := reconcileWith(t, nc)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonReloadPending)
			require.Equal(t, clusterv1beta1.ConfigAppliedByReload, got.Status.Config.AppliedBy)

			clock = clock.Add(reloadWindow + time.Second)
			got = reconcileWith(t, got)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRolloutPending)
			require.Equal(t, clusterv1beta1.ConfigAppliedByRestart, got.Status.Config.AppliedBy)
			require.Contains(t, got.Status.Config.RestartReason, "did not load revision "+got.Status.Config.Revision)
			cm := configMap(t, "lag", "demo-0")
			require.Equal(t, string(clusterv1beta1.ConfigAppliedByRestart), cm.Annotations[AnnotationConfigApply])

			calls := len(fake.calls)
			got = reconcileWith(t, got)
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRolloutPending)
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
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRolloutPending)
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
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRolloutPending)
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
			condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonRolloutPending)
			require.Equal(t, "no system account connection to reload over", got.Status.Config.RestartReason)
		})
	})

	t.Run("unsupported fields are refused", func(t *testing.T) {
		nc := newCluster(t, "unsupported", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Gateway = &clusterv1beta1.Gateway{Discovery: clusterv1beta1.GatewayDiscoveryExplicit, Remotes: []clusterv1beta1.GatewayRemote{{Name: "a", URL: "nats://a"}}}
		})
		_, got := reconcile(t, nc)
		condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonUnsupportedSpec)
		require.Contains(t, meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing).Message, "gateway")
		require.Empty(t, statefulSets(t, "unsupported"))
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
}
