package main

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

func TestLoadConfig(t *testing.T) {
	env := func(kv map[string]string) func(string) string {
		return func(k string) string { return kv[k] }
	}

	c, err := loadConfig(env(nil))
	require.NoError(t, err)
	require.Equal(t, config{
		machine:         "nats-operator-e2e",
		dns:             "9.9.9.9",
		cluster:         "nats-operator-e2e",
		clusters:        2,
		controllers:     []string{"cluster", "auth", "jetstream"},
		peerControllers: []string{"cluster", "jetstream"},
		wait:            90 * time.Second,
	}, c)
	require.Equal(t, []string{"nats-operator-e2e", "nats-operator-e2e-2"}, c.clusterNames())

	c, err = loadConfig(env(map[string]string{"E2E_CLUSTER": "t", "E2E_CLUSTERS": "3", "E2E_STORIES": "1,6", "E2E_WAIT": "4m"}))
	require.NoError(t, err)
	require.Equal(t, []string{"t", "t-2", "t-3"}, c.clusterNames())
	require.Equal(t, "1,6", c.stories)
	require.Equal(t, 4*time.Minute, c.wait)
	require.False(t, c.watchNamespaces)

	c, err = loadConfig(env(map[string]string{"E2E_WATCH_NAMESPACES": "true"}))
	require.NoError(t, err)
	require.True(t, c.watchNamespaces)
	_, err = loadConfig(env(map[string]string{"E2E_WATCH_NAMESPACES": "some"}))
	require.ErrorContains(t, err, `E2E_WATCH_NAMESPACES "some" is not a boolean`)

	_, err = loadConfig(env(map[string]string{"E2E_CLUSTERS": "0"}))
	require.ErrorContains(t, err, `E2E_CLUSTERS "0" is not a positive number`)
	_, err = loadConfig(env(map[string]string{"E2E_WAIT": "soon"}))
	require.ErrorContains(t, err, `E2E_WAIT "soon" is not a positive duration`)
}

func TestKoImage(t *testing.T) {
	img, err := koImage("jetstream-controller", "images/jetstream-controller.tar",
		"localhost/nats-operator/jetstream-controller:e2e@sha256:0123456789abcdef0123")
	require.NoError(t, err)
	require.Equal(t, image{
		Key:        "jetstream",
		Archive:    "images/jetstream-controller.tar",
		Source:     "localhost/nats-operator/jetstream-controller:e2e",
		Repository: "localhost/nats-operator/jetstream-controller",
		Tag:        "0123456789ab",
	}, img)

	_, err = koImage("x-controller", "x.tar", "localhost/nats-operator/x-controller:e2e")
	require.ErrorContains(t, err, "not a digest reference")
}

func TestChartSets(t *testing.T) {
	images := []image{{Key: "cluster", Repository: "localhost/nats-operator/cluster-controller", Tag: "abc"}}
	require.Equal(t, []string{
		"--set", "cluster.enabled=false", "--set", "auth.enabled=false", "--set", "jetstream.enabled=false",
		"--set", "auth.systemConnection=nats-system/auth-controller",
		"--set", "cluster.image.repository=localhost/nats-operator/cluster-controller",
		"--set", "cluster.image.tag=abc",
		"--set", "cluster.image.pullPolicy=Never",
		"--set", "cluster.enabled=true",
	}, chartSets([]string{"cluster"}, images, chartValues{}))
	require.Equal(t, []string{"--set", "watchNamespaces={a,nats-system}", "--set", "cluster.allowGatewayWithoutTLS=true"},
		chartSets(nil, nil, chartValues{watch: []string{"a", "nats-system"}, allowGatewayWithoutTLS: true})[8:])
	require.Equal(t, []string{"--values", "v.yaml"}, chartSets(nil, nil, chartValues{values: "v.yaml"})[8:])
}

// TestStoriesGatewayWithoutTLS pins the stories that run a gateway without
// TLS: 6, 8 after 6, 9 and 11, whose substitutions remove it, and 13, whose
// page has none.
func TestStoriesGatewayWithoutTLS(t *testing.T) {
	generated, err := generateFixtures(t.TempDir())
	require.NoError(t, err)
	stories, err := selectBundles("../..", generated, "")
	require.NoError(t, err)
	got := map[int]bool{}
	for _, b := range stories {
		got[b.Number] = b.GatewayWithoutTLS()
	}
	for n, without := range got {
		require.Equal(t, n == 6 || n == 8 || n == 9 || n == 11 || n == 13, without, "story %d", n)
	}
	require.Contains(t, got, 1)
}

// TestWatchedNamespaces pins that namespace-scoped runs watch story 1's
// namespace, and the auth controller's system connection's where it runs.
func TestWatchedNamespaces(t *testing.T) {
	generated, err := generateFixtures(t.TempDir())
	require.NoError(t, err)
	stories, err := selectBundles("../..", generated, "1")
	require.NoError(t, err)
	require.Len(t, stories, 1)
	require.Equal(t, []string{"nats-system"}, watchedNamespaces(stories, []string{"cluster", "auth", "jetstream"}))
	require.Equal(t, []string{"nats-system"}, watchedNamespaces(stories, []string{"cluster"}))
	require.Equal(t, []string{"nats-system"}, watchedNamespaces(nil, []string{"auth"}))
	require.Empty(t, watchedNamespaces(nil, []string{"cluster"}))

	_, err = selectBundles("../..", generated, "99")
	require.ErrorContains(t, err, `matches "99"`)
}

// TestGenerateFixtures pins that a run's fixtures are generated afresh, a
// previous run's gone, and that the stories load with them: story 6 holds the
// keys its fixture generates and a trust JWT from its patch file.
func TestGenerateFixtures(t *testing.T) {
	work := t.TempDir()
	stale := filepath.Join(work, "fixtures", "06-supercluster", "e2e", "00-stale.yaml")
	require.NoError(t, os.MkdirAll(filepath.Dir(stale), 0o755))
	require.NoError(t, os.WriteFile(stale, []byte("kind: ConfigMap\n"), 0o600))

	generated, err := generateFixtures(work)
	require.NoError(t, err)
	require.NoFileExists(t, stale)
	stories, err := selectBundles("../..", generated, "6")
	require.NoError(t, err)
	require.Len(t, stories, 1)

	raw, err := os.ReadFile(filepath.Join(generated, "06-supercluster", "e2e", "natsoperatortrust.json"))
	require.NoError(t, err)
	var patch struct {
		Spec struct {
			OperatorJWT string `json:"operatorJWT"`
		} `json:"spec"`
	}
	require.NoError(t, json.Unmarshal(raw, &patch))
	require.NotEmpty(t, patch.Spec.OperatorJWT)

	var keys, trustJWTs []string
	for _, o := range stories[0].Objects(1) {
		switch o.GetKind() {
		case "Secret":
			keys = append(keys, o.GetName())
		case "NatsOperatorTrust":
			jwt, _, err := unstructured.NestedString(o.Object, "spec", "operatorJWT")
			require.NoError(t, err)
			trustJWTs = append(trustJWTs, jwt)
		}
	}
	require.Subset(t, keys, []string{"acme-operator-keys", "sys-keys"})
	require.Equal(t, []string{patch.Spec.OperatorJWT}, trustJWTs)
}

// TestE2EEnv pins that the E2E_* variables reach the machine's shell whole,
// spaces and quotes included.
func TestE2EEnv(t *testing.T) {
	got := e2eEnv([]string{"HOME=/Users/x", "E2E_CONTROLLERS=cluster auth", "E2E_X=it's", "XE2E_Y=1"})
	require.Equal(t, []string{`E2E_CONTROLLERS='cluster auth'`, `E2E_X='it'\''s'`}, got)

	sh, err := exec.LookPath("sh")
	require.NoError(t, err)
	out, err := exec.Command(sh, "-c", "env "+got[0]+" "+got[1]+` sh -c 'printf "%s|%s" "$E2E_CONTROLLERS" "$E2E_X"'`).Output()
	require.NoError(t, err)
	require.Equal(t, "cluster auth|it's", string(out))
}

func TestDropLines(t *testing.T) {
	var out bytes.Buffer
	d := &dropLines{w: &out, substr: "Operation not supported by device"}
	for _, chunk := range []string{"Error: The operation couldn't be completed. Operation not ", "supported by device\nkept 1\nkept", " 2\npartial"} {
		n, err := d.Write([]byte(chunk))
		require.NoError(t, err)
		require.Equal(t, len(chunk), n)
	}
	require.Equal(t, "kept 1\nkept 2\n", out.String())
}

// TestShipTree pins what goes to the machine: every file git tracks or
// would track, as it is on disk, and every file under bin/e2e, nothing
// ignored besides; executables stay executable.
func TestShipTree(t *testing.T) {
	root := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
	}
	write := func(name, content string, mode os.FileMode) {
		t.Helper()
		p := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(content), mode))
	}
	git("init", "-q")
	write(".gitignore", "bin/\n*.log\n", 0o644)
	write("go.mod", "module x\n", 0o644)
	write("hack/run.sh", "#!/bin/sh\n", 0o755)
	write("gone.txt", "x", 0o644)
	git("add", ".")
	require.NoError(t, os.Remove(filepath.Join(root, "gone.txt")))
	write("new.txt", "untracked", 0o644)
	write("debug.log", "ignored", 0o644)
	write("bin/other", "ignored", 0o644)
	write("bin/e2e/images.json", "[]", 0o644)
	write("bin/e2e/e2e-linux", "ELF", 0o755)

	files, err := shippedFiles(t.Context(), root)
	require.NoError(t, err)
	require.Equal(t, []string{".gitignore", "bin/e2e/e2e-linux", "bin/e2e/images.json", "go.mod", "hack/run.sh", "new.txt"}, files)

	var buf bytes.Buffer
	require.NoError(t, writeTar(&buf, root, files))
	tr := tar.NewReader(&buf)
	got := map[string]string{}
	modes := map[string]int64{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		b, err := io.ReadAll(tr)
		require.NoError(t, err)
		got[h.Name] = string(b)
		modes[h.Name] = h.Mode & 0o777
		require.Zero(t, h.Uid)
	}
	require.Equal(t, "#!/bin/sh\n", got["hack/run.sh"])
	require.Equal(t, "untracked", got["new.txt"])
	require.Equal(t, int64(0o755), modes["bin/e2e/e2e-linux"])
	require.Equal(t, int64(0o644), modes["go.mod"])
}

// TestPipeTar pins that pipeTar returns, with extract's error, when extract
// stops reading before the archive ends, and that an archive read to its end
// holds every file.
func TestPipeTar(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "a.txt"), []byte("a"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "b.txt"), []byte("b"), 0o644))
	files := []string{"a.txt", "b.txt"}

	t.Run("extract ends early", func(t *testing.T) {
		exited := errors.New("tar exited")
		done := make(chan error, 1)
		go func() {
			done <- pipeTar(root, files, func(r io.Reader) error {
				if _, err := tar.NewReader(r).Next(); err != nil {
					return err
				}
				return exited
			})
		}()
		select {
		case err := <-done:
			require.ErrorIs(t, err, exited)
		case <-time.After(10 * time.Second):
			require.FailNow(t, "pipeTar is still writing the archive")
		}
	})

	t.Run("extract reads it all", func(t *testing.T) {
		var names []string
		require.NoError(t, pipeTar(root, files, func(r io.Reader) error {
			tr := tar.NewReader(r)
			for {
				h, err := tr.Next()
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
				names = append(names, h.Name)
			}
		}))
		require.Equal(t, files, names)
	})
}

// TestMachineVersions pins the helm version machine.sh installs to the helm
// CI runs the chart with.
func TestMachineVersions(t *testing.T) {
	out, err := exec.Command("sh", "machine.sh", "versions").Output()
	require.NoError(t, err)
	got := map[string]string{}
	for line := range strings.Lines(string(out)) {
		tool, version, ok := strings.Cut(strings.TrimSpace(line), " ")
		require.True(t, ok, "%q", line)
		got[tool] = version
	}

	b, err := os.ReadFile("../../.github/workflows/ci.yaml")
	require.NoError(t, err)
	var ci struct {
		Jobs map[string]struct {
			Steps []struct {
				Uses string            `json:"uses"`
				With map[string]string `json:"with"`
			} `json:"steps"`
		} `json:"jobs"`
	}
	require.NoError(t, yaml.Unmarshal(b, &ci))
	var helms int
	for job, j := range ci.Jobs {
		for _, s := range j.Steps {
			if strings.HasPrefix(s.Uses, "azure/setup-helm@") {
				helms++
				require.Equal(t, got["helm"], s.With["version"], "ci.yaml job %s", job)
			}
		}
	}
	require.NotZero(t, helms, "ci.yaml sets up helm")
}
