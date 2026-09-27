package main

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	}, chartSets([]string{"cluster"}, images))
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
