package e2e

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const storiesDir = "../../docs/content/stories"

func TestLoadBundles_Stories(t *testing.T) {
	bundles, err := LoadBundles(storiesDir)
	require.NoError(t, err)
	require.Len(t, bundles, 11)
	for i, b := range bundles {
		require.Equal(t, i+1, b.Number, b.Name)
		require.NotEmpty(t, b.Objects, b.Name)
	}

	quickstart := bundles[0]
	require.Equal(t, "01-quickstart", quickstart.Name)
	require.Equal(t, []string{"nats-system"}, quickstart.Namespaces())
	targets := map[string]string{}
	for _, e := range quickstart.Expectations {
		obj, err := quickstart.Target(e)
		require.NoError(t, err, e.File)
		targets[e.File] = obj.GetKind() + " " + key(obj)
	}
	require.Equal(t, map[string]string{
		"status-natscluster-at-rest.yaml":     "NatsCluster nats-system/demo",
		"status-natscluster-mid-rollout.yaml": "NatsCluster nats-system/demo",
		"status-natsstream.yaml":              "NatsStream nats-system/orders",
	}, targets)
}

func writeBundle(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "01-test")
	require.NoError(t, os.Mkdir(dir, 0o755))
	for name, body := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	}
	return root
}

const twoUsers = `apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata: {name: orders-batch, namespace: a}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata: {name: orders-api, namespace: b}
`

func TestTarget(t *testing.T) {
	tests := []struct {
		name   string
		status string
		want   string
		err    string
	}{
		{name: "qualifier names the object", status: "status-natsuser-orders-batch.yaml", want: "a/orders-batch"},
		{name: "only object of its kind", status: "status-natsstream-transferring.yaml", want: "a/orders"},
		{name: "no object of that name", status: "status-natsuser-denied.yaml", err: `none named "denied"`},
		{name: "no object of that kind", status: "status-natscluster.yaml", err: `0 objects of kind "natscluster"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := writeBundle(t, map[string]string{
				"users.yaml": twoUsers,
				"stream-before.yaml": `apiVersion: jetstream.nats.mikluko.io/v1beta1
kind: NatsStream
metadata: {name: orders, namespace: a}
spec: {replicas: 1}
`,
				"stream-after.yaml": `apiVersion: jetstream.nats.mikluko.io/v1beta1
kind: NatsStream
metadata: {name: orders, namespace: a}
spec: {replicas: 3}
`,
				tt.status: "status:\n  observedGeneration: 1\n",
			})
			bundles, err := LoadBundles(root)
			require.NoError(t, err)
			require.Len(t, bundles, 1)
			b := bundles[0]
			require.Len(t, b.Objects, 3, "a redeclared object replaces the earlier one")
			require.Equal(t, []string{"a", "b"}, b.Namespaces())
			obj, err := b.Target(b.Expectations[0])
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, key(obj))
		})
	}
}

func TestLoadBundles_RedeclaredObjectKeepsLastSpec(t *testing.T) {
	root := writeBundle(t, map[string]string{
		"a-before.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: a}\ndata: {v: before}\n",
		"b-after.yaml":  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: a}\ndata: {v: after}\n",
	})
	bundles, err := LoadBundles(root)
	require.NoError(t, err)
	require.Len(t, bundles[0].Objects, 1)
	require.Equal(t, map[string]any{"v": "after"}, bundles[0].Objects[0].Object["data"])
}

func TestLoadBundles_StatusFileWithoutStatus(t *testing.T) {
	root := writeBundle(t, map[string]string{"status-natsstream.yaml": "spec: {}\n"})
	_, err := LoadBundles(root)
	require.ErrorContains(t, err, "no status block")
}
