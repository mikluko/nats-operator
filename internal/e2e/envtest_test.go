package e2e

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// TestEnvtest_Runner pins, against an API server with no controller running,
// that a story whose status never arrives fails at the timeout with a diff
// naming the unmet fields, and that one whose status arrives passes.
func TestEnvtest_Runner(t *testing.T) {
	c := startAPIServer(t)

	t.Run("Quickstart times out with a diff", func(t *testing.T) {
		bundles, err := LoadBundles(storiesDir)
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
			"01-conn.yaml": `apiVersion: nats.mikluko.io/v1beta1
kind: NatsConnection
metadata: {name: demo, namespace: unread}
spec: {servers: ["nats://demo:4222"]}
`,
			"01-status-natsconnection.yaml": "status:\n  observedGeneration: 1\n",
		})
		bundles, err := LoadBundles(root)
		require.NoError(t, err)
		r := &Runner{Clients: []client.Client{c}, Timeout: time.Nanosecond, Interval: time.Second}
		res := r.Run(t.Context(), bundles[0])
		require.Equal(t, Fail, res.Outcome)
		require.Contains(t, res.Detail, "no status read before the deadline")
	})

	t.Run("Skipped story applies nothing", func(t *testing.T) {
		r := &Runner{Clients: []client.Client{c}, Timeout: time.Second, Interval: time.Second}
		res := r.Run(t.Context(), &Bundle{Name: "01-x", Number: 1, Skip: "reason"})
		require.Equal(t, Result{Story: "01-x", Outcome: Skip, Detail: "reason"}, res)
	})

	t.Run("Status arriving passes, then a step deletes", func(t *testing.T) {
		root := writeBundle(t, map[string]string{
			"02-delete-conn.yaml": "apiVersion: nats.mikluko.io/v1beta1\nkind: NatsConnection\nmetadata: {name: demo, namespace: arrives}\n",
			"01-conn.yaml": `apiVersion: nats.mikluko.io/v1beta1
kind: NatsConnection
metadata: {name: demo, namespace: arrives}
spec: {servers: ["nats://demo:4222"]}
`,
			"01-status-natsconnection.yaml": "status:\n  observedGeneration: 1\n  conditions:\n  - {type: Ready, status: \"True\", reason: Anything}\n  servers: !any 3\n",
		})
		bundles, err := LoadBundles(root)
		require.NoError(t, err)
		go setReadyWhenPresent(t.Context(), c, "arrives", "demo")
		r := &Runner{Clients: []client.Client{c}, Timeout: 20 * time.Second, Interval: 100 * time.Millisecond}
		res := r.Run(t.Context(), bundles[0])
		require.Equal(t, Pass, res.Outcome, res.Detail)
		u := &unstructured.Unstructured{}
		u.SetAPIVersion("nats.mikluko.io/v1beta1")
		u.SetKind("NatsConnection")
		err = c.Get(t.Context(), client.ObjectKey{Namespace: "arrives", Name: "demo"}, u)
		require.True(t, apierrors.IsNotFound(err), "step 2 deleted the connection: %v", err)
	})
}

// TestEnvtest_TwoClusters pins, against two API servers, that a placed story
// applies each file only to its own Kubernetes cluster and reads each status
// from there, that it is skipped when the run reaches fewer clusters than it
// places files in, and that PublishHosts carries one cluster's LoadBalancer
// hostnames to both.
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
		"01-east.yaml": `apiVersion: nats.mikluko.io/v1beta1
kind: NatsConnection
metadata: {name: east, namespace: placed}
spec: {servers: ["nats://east:4222"]}
`,
		"01-west.yaml": `apiVersion: nats.mikluko.io/v1beta1
kind: NatsConnection
metadata: {name: west, namespace: placed}
spec: {servers: ["nats://west:4222"]}
`,
		"01-status-natsconnection.yaml": "status:\n  conditions:\n  - {type: Ready, status: \"True\"}\n",
	})
	bundles, err := LoadBundles(root)
	require.NoError(t, err)
	story := bundles[0]

	t.Run("Each file lands in its cluster", func(t *testing.T) {
		go setReadyWhenPresent(t.Context(), west, "placed", "west")
		r := &Runner{Clients: []client.Client{east, west}, Timeout: 20 * time.Second, Interval: 100 * time.Millisecond}
		res := r.Run(t.Context(), story)
		require.Equal(t, Pass, res.Outcome, res.Detail)
		present := func(c client.Client, name string) bool {
			u := &unstructured.Unstructured{}
			u.SetAPIVersion("nats.mikluko.io/v1beta1")
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
func startAPIServer(t *testing.T) client.Client {
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
	c, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	require.NoError(t, err)
	return c
}

// setReadyWhenPresent plays the controller for NatsConnection ns/name.
func setReadyWhenPresent(ctx context.Context, c client.Client, ns, name string) {
	for ctx.Err() == nil {
		u := &unstructured.Unstructured{}
		u.SetAPIVersion("nats.mikluko.io/v1beta1")
		u.SetKind("NatsConnection")
		if c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, u) == nil {
			u.Object["status"] = map[string]any{
				"observedGeneration": u.GetGeneration(),
				"conditions": []any{map[string]any{
					"type": "Ready", "status": "True", "reason": "Connected", "message": "",
					"lastTransitionTime": "2026-09-26T00:00:00Z",
				}},
			}
			if c.Status().Update(ctx, u) == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}
