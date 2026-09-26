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

type workflow struct {
	Env  map[string]string `json:"env"`
	Jobs map[string]struct {
		Strategy struct {
			Matrix map[string][]string `json:"matrix"`
		} `json:"strategy"`
	} `json:"jobs"`
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

// TestRelease_PublishesWhatTheChartPulls holds the release workflow's image
// names and chart destination to the chart's default image repositories.
func TestRelease_PublishesWhatTheChartPulls(t *testing.T) {
	b, err := os.ReadFile("../.github/workflows/release.yml")
	require.NoError(t, err)
	var wf workflow
	require.NoError(t, yaml.Unmarshal(b, &wf))

	b, err = os.ReadFile("../charts/nats-operator/values.yaml")
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
	for _, c := range wf.Jobs["images"].Strategy.Matrix["controller"] {
		pushed = append(pushed, registry+"/"+c)
	}
	require.ElementsMatch(t, pulled, pushed)
	require.Equal(t, "oci://"+registry+"/charts", wf.Env["CHART_REPOSITORY"])
}
