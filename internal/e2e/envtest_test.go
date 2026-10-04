package e2e

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// TestEnvtest_Runner pins, against an API server with no controller running,
// that a story whose status never arrives fails at the timeout with a diff
// naming the unmet fields, and that one whose status arrives passes.
func TestEnvtest_Runner(t *testing.T) {
	c := startAPIServer(t)

	t.Run("Quickstart times out with a diff", func(t *testing.T) {
		bundles, err := LoadBundles(storiesDir, generated(t))
		require.NoError(t, err)
		var log bytes.Buffer
		r := &Runner{Clients: []client.Client{c}, Timeout: 2 * time.Second, Interval: 200 * time.Millisecond, Log: &log}
		res := r.Run(t.Context(), bundles[0])
		require.Equal(t, Fail, res.Outcome)
		require.Contains(t, log.String(), "01-quickstart step 1: polling 1 files")
		require.NotContains(t, log.String(), "step 2")
		require.Contains(t, res.Detail, "01-quickstart step 1: timed out after 2s")
		require.Contains(t, res.Detail, "01-status-natscluster-at-rest.yaml -> NatsCluster nats-system/demo\n")
		require.Contains(t, res.Detail, `.status.conditions[type=Ready].status: want "True", got <absent>`)
		require.Contains(t, res.Detail, `.status.replicas: want 3, got <absent>`)
	})

	t.Run("Deadline before any read fails", func(t *testing.T) {
		root := writeBundle(t, map[string]string{
			"01-conn.yaml": `apiVersion: nats-operator.io/v1beta1
kind: NatsConnection
metadata: {name: demo, namespace: unread}
spec: {servers: ["nats://demo:4222"]}
`,
			"01-status-natsconnection.yaml": "status:\n  observedGeneration: 1\n",
		})
		bundles, err := LoadBundles(root, "")
		require.NoError(t, err)
		r := &Runner{Clients: []client.Client{c}, Timeout: time.Nanosecond, Interval: time.Second}
		res := r.Run(t.Context(), bundles[0])
		require.Equal(t, Fail, res.Outcome)
		require.Contains(t, res.Detail, "no status read before the deadline")
	})

	t.Run("Fresh runs once the namespaces are made, before the first step", func(t *testing.T) {
		bundle := func(ns string) *Bundle {
			bundles, err := LoadBundles(writeBundle(t, map[string]string{
				"01-conn.yaml": "apiVersion: nats-operator.io/v1beta1\nkind: NatsConnection\nmetadata: {name: demo, namespace: " + ns + "}\nspec: {servers: [\"nats://demo:4222\"]}\n",
			}), "")
			require.NoError(t, err)
			return bundles[0]
		}
		var calls []int
		r := &Runner{Clients: []client.Client{c}, Timeout: 20 * time.Second, Interval: 100 * time.Millisecond,
			Fresh: func(ctx context.Context, cluster int) error {
				calls = append(calls, cluster)
				require.NoError(t, c.Get(ctx, client.ObjectKey{Name: "freshened"}, &corev1.Namespace{}))
				u := &unstructured.Unstructured{}
				u.SetAPIVersion("nats-operator.io/v1beta1")
				u.SetKind("NatsConnection")
				err := c.Get(ctx, client.ObjectKey{Namespace: "freshened", Name: "demo"}, u)
				require.True(t, apierrors.IsNotFound(err), "Fresh ran after the first step: %v", err)
				return nil
			}}
		res := r.Run(t.Context(), bundle("freshened"))
		require.Equal(t, Pass, res.Outcome, res.Detail)
		require.Equal(t, []int{0}, calls)

		r.Fresh = func(context.Context, int) error { return errors.New("no roles") }
		res = r.Run(t.Context(), bundle("unfreshened"))
		require.Equal(t, Fail, res.Outcome)
		require.Contains(t, res.Detail, "fresh: no roles")
	})

	t.Run("Skipped story applies nothing", func(t *testing.T) {
		r := &Runner{Clients: []client.Client{c}, Timeout: time.Second, Interval: time.Second}
		res := r.Run(t.Context(), &Bundle{Name: "01-x", Number: 1, Skip: "reason"})
		require.Equal(t, Result{Story: "01-x", Outcome: Skip, Detail: "reason"}, res)
	})

	t.Run("Status arriving passes, then a step deletes", func(t *testing.T) {
		root := writeBundle(t, map[string]string{
			"02-delete-conn.yaml": "apiVersion: nats-operator.io/v1beta1\nkind: NatsConnection\nmetadata: {name: demo, namespace: arrives}\n",
			"01-conn.yaml": `apiVersion: nats-operator.io/v1beta1
kind: NatsConnection
metadata: {name: demo, namespace: arrives}
spec: {servers: ["nats://demo:4222"]}
`,
			"01-status-natsconnection.yaml": "status:\n  observedGeneration: 1\n  conditions:\n  - {type: Ready, status: \"True\", reason: Anything}\n  servers: !any 3\n",
		})
		bundles, err := LoadBundles(root, "")
		require.NoError(t, err)
		go setReadyWhenPresent(t.Context(), c, "arrives", "demo")
		r := &Runner{Clients: []client.Client{c}, Timeout: 20 * time.Second, Interval: 100 * time.Millisecond}
		res := r.Run(t.Context(), bundles[0])
		require.Equal(t, Pass, res.Outcome, res.Detail)
		u := &unstructured.Unstructured{}
		u.SetAPIVersion("nats-operator.io/v1beta1")
		u.SetKind("NatsConnection")
		err = c.Get(t.Context(), client.ObjectKey{Namespace: "arrives", Name: "demo"}, u)
		require.True(t, apierrors.IsNotFound(err), "step 2 deleted the connection: %v", err)
	})
}

func TestEnvtest_Chained(t *testing.T) {
	c := startAPIServer(t)
	cm := func(ns, name string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + name + ", namespace: " + ns + "}\ndata: {v: \"1\"}\n"
	}
	root := writeStories(t, map[string]map[string]string{
		"01-base": {
			"01-cm.yaml":             cm("chain-base", "base"),
			"01-live-configmap.yaml": "data: {v: \"1\"}\n",
		},
		"02-next": {
			"index.md":                    "---\nparams:\n  e2e:\n    after: 1\n---\n",
			"01-cm.yaml":                  cm("chain-next", "next"),
			"01-live-configmap-next.yaml": "data: {v: \"1\"}\n",
			"02-delete-cm.yaml":           cm("chain-base", "base"),
		},
	})
	bundles, err := LoadBundles(root, "")
	require.NoError(t, err)

	var log bytes.Buffer
	r := &Runner{Clients: []client.Client{c}, Timeout: 20 * time.Second, Interval: 100 * time.Millisecond, Log: &log}
	res := r.Run(t.Context(), bundles[1])
	require.Equal(t, Pass, res.Outcome, res.Detail)
	require.Equal(t, "02-next", res.Story)
	lines := strings.Split(strings.TrimSpace(log.String()), "\n")
	require.Equal(t, []string{
		"02-next: fresh namespace chain-base",
		"02-next: fresh namespace chain-next",
		"01-base step 1: apply ConfigMap chain-base/base",
		"01-base step 1: polling 1 files, timeout 20s",
	}, lines[:4])
	require.Contains(t, lines, "02-next step 1: apply ConfigMap chain-next/next")
	require.Equal(t, "02-next step 2: delete ConfigMap chain-base/base", lines[len(lines)-1])

	err = c.Get(t.Context(), client.ObjectKey{Namespace: "chain-base", Name: "base"}, &corev1.ConfigMap{})
	require.True(t, apierrors.IsNotFound(err), "step 2 of 02-next deleted 01-base's object: %v", err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "chain-next", Name: "next"}, &corev1.ConfigMap{}))
}

func TestEnvtest_CrashLoopFailsFast(t *testing.T) {
	c := startAPIServer(t)
	root := writeBundle(t, map[string]string{
		"index.md": "---\nparams:\n  e2e:\n    waits:\n      - {step: 1, wait: 2m, reason: slow}\n---\n",
		"01-pod.yaml": `apiVersion: v1
kind: Pod
metadata: {name: nats-0, namespace: crashing}
spec: {containers: [{name: nats, image: nats:2.15.0}]}
`,
		"01-conn.yaml": `apiVersion: nats-operator.io/v1beta1
kind: NatsConnection
metadata: {name: demo, namespace: crashing}
spec: {servers: ["nats://demo:4222"]}
`,
		"01-status-natsconnection.yaml": "status:\n  conditions:\n  - {type: Ready, status: \"True\"}\n",
	})
	bundles, err := LoadBundles(root, "")
	require.NoError(t, err)
	go crashLoopWhenPresent(t.Context(), c, "crashing", "nats-0")
	var log bytes.Buffer
	r := &Runner{Clients: []client.Client{c}, Timeout: time.Minute, Interval: 100 * time.Millisecond, Log: &log}
	start := time.Now()
	res := r.Run(t.Context(), bundles[0])
	require.Equal(t, Fail, res.Outcome)
	require.Less(t, time.Since(start), 15*time.Second)
	require.Contains(t, res.Detail, "step 1: pod crashing/nats-0 container nats: CrashLoopBackOff: back-off 10s restarting failed container\n")
	require.Contains(t, res.Detail, "01-status-natsconnection.yaml -> NatsConnection crashing/demo")
	require.Contains(t, log.String(), "step 1: polling 1 files, timeout 2m0s")
}

func crashLoopWhenPresent(ctx context.Context, c client.WithWatch, ns, name string) {
	whenPresent(ctx, c, corev1.SchemeGroupVersion.WithKind("Pod"), ns, name, func(u *unstructured.Unstructured) error {
		u.Object["status"] = map[string]any{"containerStatuses": []any{map[string]any{
			"name": "nats", "image": "nats:2.15.0", "imageID": "", "restartCount": int64(3), "ready": false,
			"state": map[string]any{"waiting": map[string]any{
				"reason": "CrashLoopBackOff", "message": "back-off 10s restarting failed container",
			}},
		}}}
		return c.Status().Update(ctx, u)
	})
}

func TestEnvtest_ReleaseGuards(t *testing.T) {
	c := startAPIServer(t)
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "guarded"}}))
	obj := func(apiVersion, kind, name string, spec map[string]any) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
		u.SetAPIVersion(apiVersion)
		u.SetKind(kind)
		u.SetNamespace("guarded")
		u.SetName(name)
		require.NoError(t, c.Create(t.Context(), u))
		return u
	}
	nc := obj("cluster.nats-operator.io/v1beta1", "NatsCluster", "demo", map[string]any{"version": "2.15.0", "replicas": int64(3)})
	consumer := obj("jetstream.nats-operator.io/v1beta1", "NatsConsumer", "audit",
		map[string]any{"connectionRef": map[string]any{"name": "demo"}, "stream": "LEDGER"})
	require.Equal(t, "Delete", consumer.Object["spec"].(map[string]any)["deletionPolicy"], "a consumer deletes by default")
	stream := obj("jetstream.nats-operator.io/v1beta1", "NatsStream", "ledger",
		map[string]any{"connectionRef": map[string]any{"name": "demo"}, "name": "LEDGER"})

	require.NoError(t, releaseGuards(t.Context(), c, "guarded"))
	require.NoError(t, releaseGuards(t.Context(), c, "guarded"), "a released namespace is left as it is")
	for _, o := range []*unstructured.Unstructured{nc, consumer, stream} {
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(o), o))
	}
	require.Contains(t, nc.GetAnnotations(), clusterv1beta1.AnnotationForceDelete)
	require.Equal(t, "Retain", consumer.Object["spec"].(map[string]any)["deletionPolicy"])
	require.Equal(t, "Retain", stream.Object["spec"].(map[string]any)["deletionPolicy"])
	require.NoError(t, releaseGuards(t.Context(), c, "empty"))
}

func TestEnvtest_ReleaseFinalizers(t *testing.T) {
	c := startAPIServer(t)
	const ns = "stuck"
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	held := map[string]*unstructured.Unstructured{}
	for _, h := range controllerFinalizers {
		u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}}
		u.SetGroupVersionKind(h.gvk)
		u.SetNamespace(ns)
		u.SetName("held")
		u.SetFinalizers([]string{h.finalizer})
		held[h.gvk.Kind] = u
	}
	spec := func(kind string, fields map[string]any) {
		held[kind].Object["spec"] = fields
	}
	conn := map[string]any{"name": "demo"}
	spec("NatsUser", map[string]any{"accountRef": map[string]any{"kind": "NatsAccount", "name": "held"}})
	spec("NatsAccount", map[string]any{"operatorRef": map[string]any{"name": "demo"}})
	spec("NatsConsumer", map[string]any{"connectionRef": conn, "stream": "LEDGER"})
	spec("NatsStream", map[string]any{"connectionRef": conn, "name": "LEDGER"})
	spec("NatsKeyValue", map[string]any{"connectionRef": conn, "bucket": "config"})
	spec("NatsObjectStore", map[string]any{"connectionRef": conn, "bucket": "blobs"})
	spec("NatsClusterEvacuation", map[string]any{"connectionRef": conn, "from": map[string]any{"cluster": "east"},
		"to": map[string]any{"serverTags": []any{"cluster:west"}}})
	spec("NatsCluster", map[string]any{"version": "2.15.0", "replicas": int64(3)})
	for _, u := range held {
		require.NoError(t, c.Create(t.Context(), u), u.GetKind())
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "nats-0", Finalizers: []string{"example.com/kubelet"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "nats", Image: "nats:2.15.0"}}},
	}
	require.NoError(t, c.Create(t.Context(), pod))

	go playNamespaceController(t.Context(), c, ns)
	var log bytes.Buffer
	r := &Runner{Clients: []client.Client{c}, Timeout: 30 * time.Second, Interval: 100 * time.Millisecond, Log: &log}
	done := make(chan error, 1)
	go func() { done <- r.freshNamespace(t.Context(), c, ns) }()

	user := held["NatsUser"].DeepCopy()
	require.Eventually(t, func() bool {
		return c.Get(t.Context(), client.ObjectKeyFromObject(user), user) == nil && user.GetDeletionTimestamp() != nil
	}, 10*time.Second, 50*time.Millisecond)
	require.Never(t, func() bool {
		return c.Get(t.Context(), client.ObjectKeyFromObject(user), user) != nil || len(user.GetFinalizers()) == 0
	}, time.Second, 100*time.Millisecond, "nothing is released while a Pod remains")

	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
	pod.Finalizers = nil
	require.NoError(t, c.Update(t.Context(), pod))
	require.NoError(t, <-done)
	for _, h := range controllerFinalizers {
		require.Contains(t, log.String(), "namespace stuck: released "+h.finalizer+" from "+h.gvk.Kind+" held\n")
	}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: ns}, &corev1.Namespace{}), "the namespace is created again")
}

// playNamespaceController does for namespace ns what kube-controller-manager
// does and envtest lacks: once ns is terminating it deletes every Pod and
// every object of controllerFinalizers' kinds in it, and finalizes ns when
// none remains.
func playNamespaceController(ctx context.Context, c client.Client, ns string) {
	lists := []client.ObjectList{&corev1.PodList{}}
	for _, h := range controllerFinalizers {
		l := &unstructured.UnstructuredList{}
		l.SetGroupVersionKind(h.gvk.GroupVersion().WithKind(h.gvk.Kind + "List"))
		lists = append(lists, l)
	}
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		var n corev1.Namespace
		if c.Get(ctx, client.ObjectKey{Name: ns}, &n) != nil || n.DeletionTimestamp == nil {
			continue
		}
		left := 0
		for _, l := range lists {
			if c.List(ctx, l, client.InNamespace(ns)) != nil {
				left++
				continue
			}
			items, _ := meta.ExtractList(l)
			for _, o := range items {
				left++
				_ = c.Delete(ctx, o.(client.Object), client.GracePeriodSeconds(0))
			}
		}
		if left == 0 {
			n.Spec.Finalizers = nil
			_ = c.SubResource("finalize").Update(ctx, &n)
		}
	}
}

func TestEnvtest_TwoClusters(t *testing.T) {
	east, west := startAPIServer(t), startAPIServer(t)
	root := writeBundle(t, map[string]string{
		"index.md": `---
params:
  e2e:
    clusters:
      - {name: east, files: [01-east.yaml]}
      - {name: west, files: [01-west.yaml, 01-status-natsconnection.yaml]}
---
`,
		"01-east.yaml": `apiVersion: nats-operator.io/v1beta1
kind: NatsConnection
metadata: {name: east, namespace: placed}
spec: {servers: ["nats://east:4222"]}
`,
		"01-west.yaml": `apiVersion: nats-operator.io/v1beta1
kind: NatsConnection
metadata: {name: west, namespace: placed}
spec: {servers: ["nats://west:4222"]}
`,
		"01-status-natsconnection.yaml": "status:\n  conditions:\n  - {type: Ready, status: \"True\"}\n",
	})
	bundles, err := LoadBundles(root, "")
	require.NoError(t, err)
	story := bundles[0]

	t.Run("Each file lands in its cluster", func(t *testing.T) {
		go setReadyWhenPresent(t.Context(), west, "placed", "west")
		r := &Runner{Clients: []client.Client{east, west}, Timeout: 20 * time.Second, Interval: 100 * time.Millisecond}
		res := r.Run(t.Context(), story)
		require.Equal(t, Pass, res.Outcome, res.Detail)
		present := func(c client.Client, name string) bool {
			u := &unstructured.Unstructured{}
			u.SetAPIVersion("nats-operator.io/v1beta1")
			u.SetKind("NatsConnection")
			return c.Get(t.Context(), client.ObjectKey{Namespace: "placed", Name: name}, u) == nil
		}
		require.True(t, present(east, "east"))
		require.False(t, present(east, "west"))
		require.True(t, present(west, "west"))
		require.False(t, present(west, "east"))
	})

	t.Run("Too few clusters skips", func(t *testing.T) {
		r := &Runner{Clients: []client.Client{east}, Timeout: time.Second, Interval: time.Second}
		res := r.Run(t.Context(), story)
		require.Equal(t, Skip, res.Outcome)
		require.Equal(t, "needs 2 Kubernetes clusters, the run has 1", res.Detail)
	})

	t.Run("Hostnames reach every cluster", func(t *testing.T) {
		for _, c := range []client.Client{east, west} {
			require.NoError(t, c.Create(t.Context(), &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns"},
				Data:       map[string]string{"Corefile": ".:53 {}\n"},
			}))
		}
		annotated := func(name string, typ corev1.ServiceType) *corev1.Service {
			return &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, Annotations: map[string]string{
					HostnameAnnotation: "nats-" + name + ".example.net, alt-" + name + ".example.net",
				}},
				Spec: corev1.ServiceSpec{Type: typ, Ports: []corev1.ServicePort{{Port: 7222}}},
			}
		}
		lb := annotated("east", corev1.ServiceTypeLoadBalancer)
		require.NoError(t, east.Create(t.Context(), lb))
		require.NoError(t, east.Create(t.Context(), annotated("internal", corev1.ServiceTypeClusterIP)))
		require.NoError(t, west.Create(t.Context(), annotated("pending", corev1.ServiceTypeLoadBalancer)))
		lb.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "192.0.2.10"}}
		require.NoError(t, east.Status().Update(t.Context(), lb))

		require.NoError(t, PublishHosts(t.Context(), []client.Client{east, west}))
		for _, c := range []client.Client{east, west} {
			var cm corev1.ConfigMap
			require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "kube-system", Name: "coredns"}, &cm))
			require.Equal(t, "192.0.2.10 alt-east.example.net\n192.0.2.10 nats-east.example.net\n", cm.Data[HostsKey])
			require.Equal(t, ".:53 {}\n", cm.Data["Corefile"])
		}
	})
}

// startAPIServer starts an API server with the CRDs installed, stopped when
// t ends, and skips t when KUBEBUILDER_ASSETS is unset.
func startAPIServer(t *testing.T) client.WithWatch {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../config/crd"},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme.Scheme})
	require.NoError(t, err)
	return c
}

func setReadyWhenPresent(ctx context.Context, c client.WithWatch, ns, name string) {
	gvk := schema.GroupVersionKind{Group: "nats-operator.io", Version: "v1beta1", Kind: "NatsConnection"}
	whenPresent(ctx, c, gvk, ns, name, func(u *unstructured.Unstructured) error {
		u.Object["status"] = map[string]any{
			"observedGeneration": u.GetGeneration(),
			"conditions": []any{map[string]any{
				"type": "Ready", "status": "True", "reason": "Connected", "message": "",
				"lastTransitionTime": "2026-09-26T00:00:00Z",
			}},
		}
		return c.Status().Update(ctx, u)
	})
}

// whenPresent watches for object ns/name of kind gvk and calls update with
// it as it is added or changed, until update succeeds or ctx ends.
func whenPresent(ctx context.Context, c client.WithWatch, gvk schema.GroupVersionKind, ns, name string, update func(*unstructured.Unstructured) error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	for ctx.Err() == nil {
		w, err := c.Watch(ctx, list, client.InNamespace(ns), client.MatchingFields{"metadata.name": name})
		if err != nil {
			return
		}
		for ev := range w.ResultChan() {
			u, ok := ev.Object.(*unstructured.Unstructured)
			if !ok || (ev.Type != watch.Added && ev.Type != watch.Modified) {
				continue
			}
			if update(u) == nil {
				w.Stop()
				return
			}
		}
	}
}
