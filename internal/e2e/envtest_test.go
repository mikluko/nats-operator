package e2e

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// TestEnvtest_Runner pins, against an API server with no controller running,
// that a story whose status never arrives fails at the timeout with a diff
// naming the unmet fields, and that one whose status arrives passes.
func TestEnvtest_Runner(t *testing.T) {
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

	t.Run("Quickstart times out with a diff", func(t *testing.T) {
		bundles, err := LoadBundles(storiesDir)
		require.NoError(t, err)
		var log bytes.Buffer
		r := &Runner{Client: c, Timeout: 2 * time.Second, Interval: 200 * time.Millisecond, Log: &log}
		res := r.Run(t.Context(), bundles[0])
		require.Equal(t, Fail, res.Outcome)
		require.Contains(t, log.String(), "01-quickstart step 1: polling 1 files")
		require.NotContains(t, log.String(), "step 2")
		require.Contains(t, res.Detail, "01-quickstart step 1: timed out after 2s")
		require.Contains(t, res.Detail, "01-status-natscluster-at-rest.yaml -> NatsCluster nats-system/demo\n")
		require.Contains(t, res.Detail, `.status.conditions[type=Ready].status: want "True", got <absent>`)
		require.Contains(t, res.Detail, `.status.replicas: want 3, got <absent>`)
	})

	t.Run("Skipped story applies nothing", func(t *testing.T) {
		r := &Runner{Client: c, Timeout: time.Second, Interval: time.Second}
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
		go setReadyWhenPresent(t.Context(), c)
		r := &Runner{Client: c, Timeout: 20 * time.Second, Interval: 100 * time.Millisecond}
		res := r.Run(t.Context(), bundles[0])
		require.Equal(t, Pass, res.Outcome, res.Detail)
		u := &unstructured.Unstructured{}
		u.SetAPIVersion("nats.mikluko.io/v1beta1")
		u.SetKind("NatsConnection")
		err = c.Get(t.Context(), client.ObjectKey{Namespace: "arrives", Name: "demo"}, u)
		require.True(t, apierrors.IsNotFound(err), "step 2 deleted the connection: %v", err)
	})
}

// setReadyWhenPresent plays the controller for NatsConnection arrives/demo.
func setReadyWhenPresent(ctx context.Context, c client.Client) {
	for ctx.Err() == nil {
		u := &unstructured.Unstructured{}
		u.SetAPIVersion("nats.mikluko.io/v1beta1")
		u.SetKind("NatsConnection")
		if c.Get(ctx, client.ObjectKey{Namespace: "arrives", Name: "demo"}, u) == nil {
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
