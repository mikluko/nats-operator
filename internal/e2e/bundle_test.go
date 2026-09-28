package e2e

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const storiesDir = "../../docs/content/docs/stories"

// TestLoadBundles_Stories pins the story bundles to the harness: every
// expectation names an object its bundle declares, and every story either
// runs or states why not.
func TestLoadBundles_Stories(t *testing.T) {
	bundles, err := LoadBundles(storiesDir)
	require.NoError(t, err)
	require.Len(t, bundles, 11)
	skipped := map[string]string{}
	for i, b := range bundles {
		require.Equal(t, i+1, b.Number, b.Name)
		require.NotEmpty(t, b.Steps, b.Name)
		for _, s := range b.Steps {
			for _, e := range s.Expectations {
				_, err := b.Target(s.Number, e)
				require.NoError(t, err, "%s/%s", b.Name, e.File)
			}
		}
		if r := b.SkipReason(); r != "" {
			skipped[b.Name] = r
		}
	}
	require.Equal(t, map[string]string{
		"08-stream-transfer": "runs in the story 6 supercluster, which spans Kubernetes clusters",
		"09-acceptance":      "needs more than one Kubernetes cluster",
	}, skipped)

	chains := map[string][]string{}
	for _, b := range bundles {
		for _, c := range b.Chain() {
			chains[b.Name] = append(chains[b.Name], c.Name)
		}
	}
	require.Equal(t, []string{"02-auth-plane", "04-team-self-service"}, chains["04-team-self-service"])
	require.Equal(t, []string{"02-auth-plane", "05-account-wiring"}, chains["05-account-wiring"])
	require.Equal(t, []string{"02-auth-plane", "04-team-self-service", "07-balancing"}, chains["07-balancing"])
	require.Equal(t, []string{"nats-system", "orders", "payments"}, bundles[3].Namespaces())

	quickstart := bundles[0]
	require.Equal(t, "01-quickstart", quickstart.Name)
	require.Equal(t, []string{"nats-system"}, quickstart.Namespaces())
	type stepSummary struct {
		Number  int
		Apply   []string
		Targets map[string]string
	}
	var steps []stepSummary
	for _, s := range quickstart.Steps {
		sum := stepSummary{Number: s.Number, Targets: map[string]string{}}
		for _, o := range s.Apply {
			sum.Apply = append(sum.Apply, o.GetKind()+" "+key(o))
		}
		for _, e := range s.Expectations {
			obj, err := quickstart.Target(s.Number, e)
			require.NoError(t, err)
			sum.Targets[e.File] = obj.GetKind() + " " + key(obj)
		}
		steps = append(steps, sum)
	}
	require.Equal(t, []stepSummary{
		{1, []string{"NatsCluster nats-system/demo"}, map[string]string{
			"01-status-natscluster-at-rest.yaml": "NatsCluster nats-system/demo",
		}},
		{2, []string{"NatsCluster nats-system/demo"}, map[string]string{
			"02-status-natscluster-mid-rollout.yaml": "NatsCluster nats-system/demo",
		}},
		{3, []string{"NatsConnection nats-system/demo", "NatsStream nats-system/orders"}, map[string]string{
			"03-status-natsstream.yaml": "NatsStream nats-system/orders",
		}},
	}, steps)
	require.Equal(t, "2.15.0", quickstart.Steps[0].Apply[0].Object["spec"].(map[string]any)["version"])
	require.Equal(t, "2.15.1", quickstart.Steps[1].Apply[0].Object["spec"].(map[string]any)["version"])
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

func TestParseFileName(t *testing.T) {
	tests := []struct {
		base string
		want FileName
		err  string
	}{
		{base: "01-natscluster.yaml", want: FileName{Step: 1, Role: RoleApply}},
		{base: "02-natscluster-2.15.1.yaml", want: FileName{Step: 2, Role: RoleApply}},
		{base: "02-delete-natscluster-prod-east.yaml", want: FileName{Step: 2, Role: RoleDelete}},
		{base: "03-status-natsstream.yaml", want: FileName{Step: 3, Role: RoleStatus, Kind: "natsstream"}},
		{base: "01-status-natsuser-orders-batch.yaml", want: FileName{Step: 1, Role: RoleStatus, Kind: "natsuser", Qualifier: "orders-batch"}},
		{base: "01-live-natsstream-payments.yaml", want: FileName{Step: 1, Role: RoleLive, Kind: "natsstream", Qualifier: "payments"}},
		{base: "natscluster.yaml", err: "starts with its step number"},
		{base: "status-natscluster.yaml", err: "starts with its step number"},
		{base: "01.yaml", err: "starts with its step number"},
		{base: "01-status.yaml", err: "names no kind"},
	}
	for _, tt := range tests {
		t.Run(tt.base, func(t *testing.T) {
			got, err := ParseFileName(tt.base)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
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
		{name: "qualifier names the object", status: "01-status-natsuser-orders-batch.yaml", want: "a/orders-batch"},
		{name: "only object of its kind", status: "02-status-natsstream-transferring.yaml", want: "a/orders"},
		{name: "deleted object", status: "03-status-natscluster.yaml", want: "a/old"},
		{name: "no object of that name", status: "01-status-natsuser-denied.yaml", err: `none named "denied"`},
		{name: "object declared only in a later step", status: "01-status-natsstream.yaml", err: `0 objects of kind "natsstream"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := writeBundle(t, map[string]string{
				"01-users.yaml": twoUsers,
				"02-stream.yaml": `apiVersion: jetstream.nats.mikluko.io/v1beta1
kind: NatsStream
metadata: {name: orders, namespace: a}
spec: {replicas: 1}
`,
				"03-delete-old.yaml": "apiVersion: cluster.nats.mikluko.io/v1beta1\nkind: NatsCluster\nmetadata: {name: old, namespace: a}\n",
				tt.status:            "status:\n  observedGeneration: 1\n",
			})
			bundles, err := LoadBundles(root)
			require.NoError(t, err)
			require.Len(t, bundles, 1)
			b := bundles[0]
			require.Equal(t, []string{"a", "b"}, b.Namespaces())
			name, err := ParseFileName(tt.status)
			require.NoError(t, err)
			i := slices.IndexFunc(b.Steps, func(s Step) bool { return s.Number == name.Step })
			obj, err := b.Target(name.Step, b.Steps[i].Expectations[0])
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, key(obj))
		})
	}
}

func TestLoadBundles_StepsInOrder(t *testing.T) {
	root := writeBundle(t, map[string]string{
		"10-after.yaml":          "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: a}\ndata: {v: after}\n",
		"2-before.yaml":          "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: a}\ndata: {v: before}\n",
		"2-delete-gone.yaml":     "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: gone, namespace: a}\n",
		"10-live-configmap.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: a}\ndata: {v: !any after}\n",
	})
	bundles, err := LoadBundles(root)
	require.NoError(t, err)
	b := bundles[0]
	require.Len(t, b.Steps, 2)
	require.Equal(t, 2, b.Steps[0].Number)
	require.Equal(t, map[string]any{"v": "before"}, b.Steps[0].Apply[0].Object["data"])
	require.Equal(t, "gone", b.Steps[0].Delete[0].GetName())
	require.Equal(t, 10, b.Steps[1].Number)
	require.Equal(t, map[string]any{"v": "after"}, b.Steps[1].Apply[0].Object["data"])
	require.Equal(t, map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "x", "namespace": "a"},
		"data":     map[string]any{"v": placeholder{}},
	}, b.Steps[1].Expectations[0].Want, "a live file is expected whole")
	require.Len(t, b.Objects(2), 2)
	require.Equal(t, map[string]any{"v": "after"}, b.Objects(10)[0].Object["data"], "a redeclared object is as last declared")
}

func TestLoadBundles_Rejects(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		err   string
	}{
		{name: "status file without status", files: map[string]string{"01-status-natsstream.yaml": "spec: {}\n"}, err: "no status block"},
		{name: "file without step number", files: map[string]string{"natsstream.yaml": "kind: X\n"}, err: "starts with its step number"},
		{name: "unclosed front matter", files: map[string]string{"index.md": "---\ntitle: x\n"}, err: "front matter is not closed"},
		{name: "after names no earlier story", files: map[string]string{"index.md": "---\nparams:\n  e2e:\n    after: 1\n---\n"}, err: "not an earlier story"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadBundles(writeBundle(t, tt.files))
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestSkipReason(t *testing.T) {
	base := &Bundle{Name: "02-base", Skip: "needs more than one Kubernetes cluster"}
	require.Equal(t, "starts from 02-base, which is skipped: needs more than one Kubernetes cluster",
		(&Bundle{Base: base}).SkipReason())
	require.Equal(t, "own", (&Bundle{Base: base, Skip: "own"}).SkipReason())
	require.Empty(t, (&Bundle{Base: &Bundle{}}).SkipReason())
}

func TestLoadBundles_SuperclusterParts(t *testing.T) {
	bundles, err := LoadBundles(storiesDir)
	require.NoError(t, err)
	super := bundles[5]
	require.Equal(t, "06-supercluster", super.Name)

	type placed struct {
		cluster string
		objects []string
		targets map[string]string
	}
	var got []placed
	for _, p := range super.Parts() {
		pl := placed{cluster: p.Cluster, targets: map[string]string{}}
		for _, o := range p.Objects(1) {
			pl.objects = append(pl.objects, o.GetKind()+" "+o.GetName())
		}
		for _, s := range p.Steps {
			for _, e := range s.Expectations {
				obj, err := p.Target(s.Number, e)
				require.NoError(t, err, e.File)
				pl.targets[e.File] = obj.GetKind() + " " + key(obj)
			}
		}
		got = append(got, pl)
	}
	require.Equal(t, []placed{
		{
			cluster: "east",
			objects: []string{
				"Secret acme-operator-keys", "Secret sys-keys", "NatsOperator acme", "NatsSystemAccount sys",
				"NatsUser cluster-controller", "NatsUser auth-controller", "NatsConnection auth-controller",
				"NatsUser west-cluster-controller", "NatsUser west-jetstream-controller", "NatsCluster east", "NatsOperatorTrust acme",
			},
			targets: map[string]string{},
		},
		{
			cluster: "west",
			objects: []string{"Secret west-cluster-controller-creds", "NatsOperatorTrust acme", "NatsCluster west"},
			targets: map[string]string{"01-status-natscluster-west.yaml": "NatsCluster nats-system/west"},
		},
	}, got)
}

func TestLoadBundles_UnplacedIsOnePart(t *testing.T) {
	bundles, err := LoadBundles(storiesDir)
	require.NoError(t, err)
	parts := bundles[0].Parts()
	require.Equal(t, []Part{{Steps: bundles[0].Steps}}, parts)
}

func TestLoadBundles_PlacementErrors(t *testing.T) {
	const cm = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: a}\n"
	tests := []struct {
		name  string
		index string
		err   string
	}{
		{
			name:  "file placed nowhere",
			index: "---\ntitle: t\nparams:\n  e2e:\n    clusters:\n      - {name: east, files: [01-a.yaml]}\n---\n",
			err:   "01-b.yaml: in none of the Kubernetes clusters",
		},
		{
			name:  "placement names a missing file",
			index: "---\nparams:\n  e2e:\n    clusters:\n      - {name: east, files: [01-a.yaml, 01-b.yaml, 01-c.yaml]}\n---\n",
			err:   "cluster east places 01-c.yaml, which the bundle does not have",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := writeBundle(t, map[string]string{"01-a.yaml": cm, "01-b.yaml": cm, "index.md": tt.index})
			_, err := LoadBundles(root)
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestLoadBundles_Substitutions(t *testing.T) {
	root := writeBundle(t, map[string]string{
		"index.md": `---
params:
  e2e:
    substitutions:
      - files: [01-a.yaml]
        reason: the harness has less memory
        patch: {data: {mem: 1Gi, gone: null}}
      - files: [01-status-configmap.yaml]
        reason: the harness has less memory
        patch: {status: {mem: 768Mi}}
      - files: [01-c.yaml]
        kind: ConfigMap
        name: c1
        reason: selected
        patch: {data: {picked: "yes"}}
---
`,
		"01-a.yaml":                "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: a}\ndata: {mem: 4Gi, gone: x, kept: z}\n",
		"01-b.yaml":                "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: y, namespace: a}\ndata: {mem: 4Gi}\n",
		"01-c.yaml":                "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c1, namespace: a}\n---\napiVersion: v1\nkind: Secret\nmetadata: {name: c1, namespace: a}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: c2, namespace: a}\n",
		"01-status-configmap.yaml": "status: {mem: !any 3Gi}\ndata: {mem: 3Gi}\n",
	})
	bundles, err := LoadBundles(root)
	require.NoError(t, err)
	s := bundles[0].Steps[0]
	require.Equal(t, map[string]any{"mem": "1Gi", "kept": "z"}, s.Apply[0].Object["data"])
	require.Equal(t, map[string]any{"mem": "4Gi"}, s.Apply[1].Object["data"], "a file no substitution names is as written")
	require.Equal(t, map[string]any{"status": map[string]any{"mem": "768Mi"}}, s.Expectations[0].Want)
	var picked []string
	for _, o := range s.Apply {
		if d, ok := o.Object["data"].(map[string]any); ok && d["picked"] == "yes" {
			picked = append(picked, o.GetKind()+" "+o.GetName())
		}
	}
	require.Equal(t, []string{"ConfigMap c1"}, picked, "kind and name narrow a substitution to one manifest")
}

func TestLoadBundles_SubstitutionErrors(t *testing.T) {
	const cm = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: a}\n"
	tests := []struct {
		name string
		sub  string
		err  string
	}{
		{name: "no reason", sub: "{files: [01-a.yaml], patch: {data: {}}}", err: "needs files, a reason and a patch"},
		{name: "no patch", sub: "{files: [01-a.yaml], reason: r}", err: "needs files, a reason and a patch"},
		{name: "missing file", sub: "{files: [01-z.yaml], reason: r, patch: {data: {}}}", err: "names 01-z.yaml, which the bundle does not have"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := "---\nparams:\n  e2e:\n    substitutions:\n      - " + tt.sub + "\n---\n"
			_, err := LoadBundles(writeBundle(t, map[string]string{"01-a.yaml": cm, "index.md": index}))
			require.ErrorContains(t, err, tt.err)
		})
	}
}

// TestLoadBundles_Waits pins that a story's front matter raises the wait
// of the steps it names, and that a wait without a reason, a positive
// duration or a step of the story fails loading.
func TestLoadBundles_Waits(t *testing.T) {
	const cm = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: a}\n"
	load := func(waits string) (*Bundle, error) {
		index := "---\nparams:\n  e2e:\n    waits:\n" + waits + "---\n"
		bs, err := LoadBundles(writeBundle(t, map[string]string{"01-a.yaml": cm, "02-a.yaml": cm, "index.md": index}))
		if err != nil {
			return nil, err
		}
		return bs[0], nil
	}

	b, err := load("      - {step: 2, wait: 4m, reason: gateways connect}\n")
	require.NoError(t, err)
	require.Equal(t, map[int]StepWait{2: {Step: 2, Wait: 4 * time.Minute, Reason: "gateways connect"}}, b.Waits)

	for _, tt := range []struct {
		name  string
		waits string
		err   string
	}{
		{name: "no reason", waits: "      - {step: 1, wait: 4m}\n", err: "the wait of step 1 needs a positive wait and a reason"},
		{name: "not a duration", waits: "      - {step: 1, wait: soon, reason: r}\n", err: "wait of step 1"},
		{name: "no such step", waits: "      - {step: 3, wait: 4m, reason: r}\n", err: "a wait names step 3, which has no files"},
		{name: "twice", waits: "      - {step: 1, wait: 4m, reason: r}\n      - {step: 1, wait: 5m, reason: r}\n", err: "two waits for step 1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(tt.waits)
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestLoadBundles_Fixtures(t *testing.T) {
	root := writeBundle(t, map[string]string{
		"01-a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: a, namespace: story}\n",
		"index.md":  "---\nparams:\n  e2e:\n    clusters:\n      - {name: east, files: [01-a.yaml, e2e/00-fixture.yaml, e2e/00-status-job.yaml]}\n---\n",
	})
	dir := filepath.Join(root, "01-test", FixtureDir)
	require.NoError(t, os.Mkdir(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "00-fixture.yaml"),
		[]byte("apiVersion: batch/v1\nkind: Job\nmetadata: {name: seed, namespace: fixture}\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "00-status-job.yaml"), []byte("status: {succeeded: 1}\n"), 0o600))
	bundles, err := LoadBundles(root)
	require.NoError(t, err)
	b := bundles[0]
	require.Len(t, b.Steps, 2)
	require.Equal(t, 0, b.Steps[0].Number)
	require.Equal(t, "seed", b.Steps[0].Apply[0].GetName())
	require.Equal(t, "00-status-job.yaml", b.Steps[0].Expectations[0].File)
	require.Equal(t, []string{"fixture", "story"}, b.Namespaces())
}
