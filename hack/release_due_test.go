package hack_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubCurl is a curl that answers every request with the status in
// $STUB/status and the body in $STUB/page<N>.json, N the URL's page, and
// appends its arguments to $STUB/args and its config from stdin to
// $STUB/config.
const stubCurl = `#!/usr/bin/env bash
cat >>"$STUB/config"
echo "$*" >>"$STUB/args"
out= url=
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out=$2; shift ;;
    http*) url=$1 ;;
  esac
  shift
done
page=${url##*page=}
if [ -f "$STUB/page$page.json" ]; then cp "$STUB/page$page.json" "$out"; else echo '{"message":"stub"}' >"$out"; fi
cat "$STUB/status"
`

// versionList returns a page of the GitHub Packages versions API holding one
// container version per tag.
func versionList(t *testing.T, tags ...string) string {
	t.Helper()
	type container struct {
		Tags []string `json:"tags"`
	}
	type metadata struct {
		Container container `json:"container"`
	}
	type version struct {
		Metadata metadata `json:"metadata"`
	}
	list := []version{}
	for _, tag := range tags {
		list = append(list, version{metadata{container{[]string{tag}}}})
	}
	b, err := json.Marshal(list)
	require.NoError(t, err)
	return string(b)
}

// TestRelease_DueUntilChartPushed pins that a version is due only while it is
// untagged and its chart is absent from the registry, that a chart present
// for an untagged version or an unexpected registry answer fails the plan,
// and that every publishing job waits on that.
func TestRelease_DueUntilChartPushed(t *testing.T) {
	bash, err := exec.LookPath("bash")
	require.NoError(t, err)
	jq, err := exec.LookPath("jq")
	require.NoError(t, err)

	wf := readWorkflow(t, "release.yml")
	plan := wf.Jobs["plan"]
	require.Equal(t, "${{ steps.due.outputs.due }}", plan.Outputs["due"])
	require.Equal(t, "${{ steps.due.outputs.reason }}", plan.Outputs["reason"])
	require.Equal(t, "read", plan.Permissions["packages"])
	for _, job := range []string{"images", "chart"} {
		require.Contains(t, wf.Jobs[job].Needs, "plan", job)
		require.Equal(t, "needs.plan.outputs.due == 'true'", wf.Jobs[job].If, job)
	}

	due := stepByID(t, plan.Steps, "due")
	require.Empty(t, due.If)
	require.Equal(t, map[string]string{
		"VERSION": "${{ steps.changelog.outputs.version }}",
		"TAGGED":  "${{ steps.changelog.outputs['already-tagged'] }}",
		"TOKEN":   "${{ secrets.GITHUB_TOKEN }}",
	}, due.Env)
	require.Equal(t, "oci://ghcr.io/mikluko/nats-operator/charts", wf.Env["CHART_REPOSITORY"])

	const (
		version  = "0.1.0"
		token    = "stub-token"
		versions = "https://api.github.com/users/mikluko/packages/container/nats-operator%2Fcharts%2Fnats-operator/versions?per_page=100&page="
		absent   = "v0.1.0 is untagged and its chart is not in oci://ghcr.io/mikluko/nats-operator/charts"
	)
	full := make([]string, 100)
	for i := range full {
		full[i] = fmt.Sprintf("0.0.%d", i)
	}

	for _, tc := range []struct {
		name    string
		version string
		tagged  string
		status  string
		pages   []string
		// exit is the step's exit status; -1 is any non-zero one.
		exit    int
		outputs map[string]string
		stdout  string
		fetched int
	}{
		{
			name:    "no version",
			tagged:  "false",
			outputs: map[string]string{"due": "false", "reason": "CHANGELOG.md names no version"},
		},
		{
			name:    "tagged",
			version: version,
			tagged:  "true",
			outputs: map[string]string{"due": "false", "reason": "v0.1.0 is tagged"},
		},
		{
			name:    "200 untagged, chart pushed",
			version: version,
			tagged:  "false",
			status:  "200",
			pages:   []string{versionList(t, "0.0.9", version)},
			exit:    1,
			stdout:  "::error::chart 0.1.0 is in oci://ghcr.io/mikluko/nats-operator/charts but v0.1.0 is untagged",
			fetched: 1,
		},
		{
			name:    "200 untagged, chart pushed on the second page",
			version: version,
			tagged:  "false",
			status:  "200",
			pages:   []string{versionList(t, full...), versionList(t, version)},
			exit:    1,
			stdout:  "::error::chart 0.1.0 is in",
			fetched: 2,
		},
		{
			name:    "200 untagged, chart absent",
			version: version,
			tagged:  "false",
			status:  "200",
			pages:   []string{versionList(t, full...), versionList(t, "sha256-abc.sig")},
			outputs: map[string]string{"due": "true", "reason": absent},
			fetched: 2,
		},
		{
			name:    "200 with a body that is not a version list",
			version: version,
			tagged:  "false",
			status:  "200",
			exit:    -1,
			fetched: 1,
		},
		{
			name:    "404 no package",
			version: version,
			tagged:  "false",
			status:  "404",
			outputs: map[string]string{"due": "true", "reason": absent},
			fetched: 1,
		},
		{
			name:    "403",
			version: version,
			tagged:  "false",
			status:  "403",
			exit:    1,
			stdout:  "::error::GitHub answered 403 for the versions of package nats-operator/charts/nats-operator",
			fetched: 1,
		},
		{
			name:    "500",
			version: version,
			tagged:  "false",
			status:  "500",
			exit:    1,
			stdout:  "::error::GitHub answered 500",
			fetched: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := t.TempDir()
			bin := filepath.Join(stub, "bin")
			require.NoError(t, os.Mkdir(bin, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(bin, "curl"), []byte(stubCurl), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(stub, "status"), []byte(tc.status), 0o644))
			for i, body := range tc.pages {
				require.NoError(t, os.WriteFile(filepath.Join(stub, fmt.Sprintf("page%d.json", i+1)), []byte(body), 0o644))
			}
			output := filepath.Join(stub, "output")
			require.NoError(t, os.WriteFile(output, nil, 0o644))

			cmd := exec.Command(bash, "--noprofile", "--norc", "-e", "-c", due.Run)
			cmd.Env = []string{
				"PATH=" + bin + ":" + filepath.Dir(jq) + ":" + filepath.Dir(bash) + ":/usr/bin:/bin",
				"STUB=" + stub,
				"CHART_REPOSITORY=" + wf.Env["CHART_REPOSITORY"],
				"GITHUB_API_URL=https://api.github.com",
				"GITHUB_REPOSITORY_OWNER=mikluko",
				"GITHUB_OUTPUT=" + output,
				"RUNNER_TEMP=" + stub,
				"VERSION=" + tc.version,
				"TAGGED=" + tc.tagged,
				"TOKEN=" + token,
			}
			stdout, err := cmd.Output()
			switch tc.exit {
			case 0:
				require.NoError(t, err, "%s", stdout)
			case -1:
				require.Error(t, err, "%s", stdout)
			default:
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, "%s", stdout)
				require.Equal(t, tc.exit, exit.ExitCode(), "%s", stdout)
			}
			require.Contains(t, string(stdout), tc.stdout)

			b, err := os.ReadFile(output)
			require.NoError(t, err)
			outputs := map[string]string{}
			for line := range strings.Lines(string(b)) {
				k, v, ok := strings.Cut(strings.TrimSuffix(line, "\n"), "=")
				require.True(t, ok, line)
				outputs[k] = v
			}
			if tc.outputs == nil {
				tc.outputs = map[string]string{}
			}
			require.Equal(t, tc.outputs, outputs)

			var args, config []string
			if b, err := os.ReadFile(filepath.Join(stub, "args")); err == nil {
				args = strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
			}
			if b, err := os.ReadFile(filepath.Join(stub, "config")); err == nil {
				config = strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
			}
			require.Len(t, args, tc.fetched)
			for i, a := range args {
				require.True(t, strings.HasSuffix(a, " "+versions+fmt.Sprint(i+1)), a)
				require.NotContains(t, a, token)
				require.Equal(t, `header = "Authorization: Bearer `+token+`"`, config[i])
			}
		})
	}
}
