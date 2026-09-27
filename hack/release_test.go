package hack_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

// plan runs release-plan.sh with the given changelog outputs and returns the
// outputs it wrote, its combined output and its error.
func plan(t *testing.T, env map[string]string) (map[string]string, string, error) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "github_output")
	cmd := exec.Command("sh", "release-plan.sh")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "GITHUB_OUTPUT=" + out}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	combined, err := cmd.CombinedOutput()
	written := map[string]string{}
	if b, readErr := os.ReadFile(out); readErr == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if k, v, ok := strings.Cut(line, "="); ok {
				written[k] = v
			}
		}
	}
	return written, string(combined), err
}

func TestReleasePlan(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		want    map[string]string
		wantErr bool
	}{
		{
			name: "untagged final version is due",
			env:  map[string]string{"VALID": "true", "VERSION": "1.2.3", "TAGGED": "false", "PRERELEASE": "false"},
			want: map[string]string{"due": "true", "version": "1.2.3", "tag": "v1.2.3", "prerelease": "false"},
		},
		{
			name: "untagged pre-release is due and marked",
			env:  map[string]string{"VALID": "true", "VERSION": "0.0.1-rc.1", "TAGGED": "false", "PRERELEASE": "true"},
			want: map[string]string{"due": "true", "version": "0.0.1-rc.1", "tag": "v0.0.1-rc.1", "prerelease": "true"},
		},
		{
			name: "tagged version is not due",
			env:  map[string]string{"VALID": "true", "VERSION": "1.2.3", "TAGGED": "true", "PRERELEASE": "false"},
			want: map[string]string{"due": "false"},
		},
		{
			name: "only Unreleased is not due",
			env:  map[string]string{"VALID": "true", "VERSION": "", "TAGGED": "false", "PRERELEASE": "false"},
			want: map[string]string{"due": "false"},
		},
		{
			name:    "invalid changelog fails",
			env:     map[string]string{"VALID": "false", "VERSION": "1.2.3", "TAGGED": "false", "PRERELEASE": "false"},
			wantErr: true,
		},
		{
			name:    "missing input fails",
			env:     map[string]string{"VALID": "true", "VERSION": "1.2.3", "PRERELEASE": "false"},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, out, err := plan(t, tc.env)
			if tc.wantErr {
				require.Error(t, err, out)
				require.Empty(t, got["due"], "a failed plan writes no decision")
				return
			}
			require.NoError(t, err, out)
			require.Equal(t, tc.want, got)
		})
	}
}

type step struct {
	Uses string            `json:"uses"`
	Run  string            `json:"run"`
	Env  map[string]string `json:"env"`
}

type workflow struct {
	Env  map[string]string `json:"env"`
	Jobs map[string]struct {
		Env   map[string]string `json:"env"`
		Steps []step            `json:"steps"`
	} `json:"jobs"`
}

func readWorkflow(t *testing.T, name string) workflow {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("../.github/workflows", name))
	require.NoError(t, err)
	var wf workflow
	require.NoError(t, yaml.Unmarshal(b, &wf))
	return wf
}

type controllerValues struct {
	Image struct {
		Repository string `json:"repository"`
	} `json:"image"`
}

type chartValues struct {
	Cluster   controllerValues `json:"cluster"`
	Auth      controllerValues `json:"auth"`
	JetStream controllerValues `json:"jetstream"`
}

// controllers are the commands under cmd/ named *-controller, which the
// Justfile, the release workflow and hack/e2e build.
func controllers(t *testing.T) []string {
	t.Helper()
	dirs, err := filepath.Glob("../cmd/*-controller")
	require.NoError(t, err)
	names := make([]string, len(dirs))
	for i, d := range dirs {
		names[i] = filepath.Base(d)
	}
	return names
}

// TestRelease_PublishesWhatTheChartPulls holds the images the release
// workflow publishes, one per controller under cmd/, and its chart
// destination to the chart's default image repositories.
func TestRelease_PublishesWhatTheChartPulls(t *testing.T) {
	wf := readWorkflow(t, "release.yml")

	b, err := os.ReadFile("../charts/nats-operator/values.yaml")
	require.NoError(t, err)
	var values chartValues
	require.NoError(t, yaml.Unmarshal(b, &values))

	pulled := []string{
		values.Cluster.Image.Repository,
		values.Auth.Image.Repository,
		values.JetStream.Image.Repository,
	}

	registry := wf.Env["REGISTRY"]
	require.Equal(t, "ghcr.io/mikluko/nats-operator", registry)
	var pushed []string
	for _, c := range controllers(t) {
		pushed = append(pushed, registry+"/"+c)
	}
	require.ElementsMatch(t, pulled, pushed)
	require.Equal(t, "oci://"+registry+"/charts", wf.Env["CHART_REPOSITORY"])
}

// TestControllerList pins the Justfile's controller list to cmd/.
func TestControllerList(t *testing.T) {
	just, err := exec.LookPath("just")
	if err != nil {
		t.Skip("just is not on PATH")
	}
	out, err := exec.Command(just, "--justfile", "../Justfile", "--evaluate", "controllers").Output()
	require.NoError(t, err)
	require.ElementsMatch(t, controllers(t), strings.Fields(string(out)))
}

// setupHugo is the action every workflow installs Hugo with.
const setupHugo = "./.github/actions/setup-hugo"

// TestHugoInstalledOnce pins the Hugo release to the setup-hugo action: the
// workflows that build the site use it, and none names a version of its own.
func TestHugoInstalledOnce(t *testing.T) {
	b, err := os.ReadFile("../.github/actions/setup-hugo/action.yml")
	require.NoError(t, err)
	var action struct {
		Runs struct {
			Steps []step `json:"steps"`
		} `json:"runs"`
	}
	require.NoError(t, yaml.Unmarshal(b, &action))
	require.Len(t, action.Runs.Steps, 1)
	require.Regexp(t, `^\d+\.\d+\.\d+$`, action.Runs.Steps[0].Env["HUGO_VERSION"])

	for _, tc := range []struct{ workflow, job string }{
		{"ci.yml", "go"},
		{"docs.yml", "build"},
	} {
		t.Run(tc.workflow, func(t *testing.T) {
			wf := readWorkflow(t, tc.workflow)
			require.NotContains(t, wf.Env, "HUGO_VERSION")
			var uses bool
			for name, job := range wf.Jobs {
				require.NotContains(t, job.Env, "HUGO_VERSION", name)
				for _, s := range job.Steps {
					require.NotContains(t, s.Env, "HUGO_VERSION", name)
					uses = uses || (name == tc.job && s.Uses == setupHugo)
				}
			}
			require.True(t, uses, "job %s does not use %s", tc.job, setupHugo)
		})
	}
}
