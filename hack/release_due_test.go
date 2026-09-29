package hack_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubCurl is a curl answering from files under $STUB and recording how it
// was called.
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
case "$url" in
  */token\?*) answer=token ;;
  https://api.github.com/*) answer=package ;;
  *) answer=manifest ;;
esac
if [ -f "$STUB/$answer.json" ]; then cp "$STUB/$answer.json" "$out"; else : >"$out"; fi
cat "$STUB/$answer.status"
`

// registryError returns a body in the shape of an OCI distribution error
// response carrying code.
func registryError(code, message string) string {
	return `{"errors":[{"code":"` + code + `","message":"` + message + `"}]}`
}

// TestRelease_DueUntilChartPushed pins that a version is due only while it is
// untagged and either the registry answers its chart's manifest with 404
// MANIFEST_UNKNOWN or NAME_UNKNOWN, or it denies a pull token with 403 and the
// GitHub Packages API answers the chart's package with 404; that any other
// answer fails the plan; and that every publishing job waits on that.
func TestRelease_DueUntilChartPushed(t *testing.T) {
	bash, err := exec.LookPath("bash")
	require.NoError(t, err)
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Skip("jq is not on PATH")
	}

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
		actor    = "stub-actor"
		token    = "stub-token"
		bearer   = "stub-bearer"
		tokenURL = "https://ghcr.io/token?service=ghcr.io&scope=repository:mikluko/nats-operator/charts/nats-operator:pull"
		manifest = "https://ghcr.io/v2/mikluko/nats-operator/charts/nats-operator/manifests/0.1.0"
		pkgURL   = "https://api.github.com/users/mikluko/packages/container/nats-operator%2Fcharts%2Fnats-operator"
		absent   = "v0.1.0 is untagged and its chart is not in oci://ghcr.io/mikluko/nats-operator/charts"
		granted  = `{"token":"` + bearer + `"}`
	)

	for _, tc := range []struct {
		name           string
		version        string
		tagged         string
		tokenStatus    string
		tokenBody      string
		manifestStatus string
		manifestBody   string
		packageStatus  string
		exit           int
		outputs        map[string]string
		stdout         string
		requested      []string
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
			name:        "token 401",
			version:     version,
			tagged:      "false",
			tokenStatus: "401",
			tokenBody:   registryError("UNAUTHORIZED", "authentication required"),
			exit:        1,
			stdout:      "::error::ghcr.io answered 401 without a pull token for mikluko/nats-operator/charts/nats-operator",
			requested:   []string{tokenURL},
		},
		{
			name:        "token 200 without a token",
			version:     version,
			tagged:      "false",
			tokenStatus: "200",
			tokenBody:   `{}`,
			exit:        1,
			stdout:      "::error::ghcr.io answered 200 without a pull token",
			requested:   []string{tokenURL},
		},
		{
			name:          "token 403, package 404",
			version:       version,
			tagged:        "false",
			tokenStatus:   "403",
			tokenBody:     registryError("DENIED", "requested access to the resource is denied"),
			packageStatus: "404",
			outputs:       map[string]string{"due": "true", "reason": absent},
			requested:     []string{tokenURL, pkgURL},
		},
		{
			name:          "token 403, package 200",
			version:       version,
			tagged:        "false",
			tokenStatus:   "403",
			tokenBody:     registryError("DENIED", "requested access to the resource is denied"),
			packageStatus: "200",
			exit:          1,
			stdout:        "::error::ghcr.io answered 403 without a pull token for mikluko/nats-operator/charts/nats-operator, and the GitHub Packages API answered 200: the package exists but its pull was denied",
			requested:     []string{tokenURL, pkgURL},
		},
		{
			name:          "token 403, package 500",
			version:       version,
			tagged:        "false",
			tokenStatus:   "403",
			tokenBody:     registryError("DENIED", "requested access to the resource is denied"),
			packageStatus: "500",
			exit:          1,
			stdout:        "::error::ghcr.io answered 403 without a pull token for mikluko/nats-operator/charts/nats-operator, and the GitHub Packages API answered 500",
			requested:     []string{tokenURL, pkgURL},
		},
		{
			name:           "200 untagged, chart pushed",
			version:        version,
			tagged:         "false",
			tokenStatus:    "200",
			tokenBody:      granted,
			manifestStatus: "200",
			manifestBody:   `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`,
			exit:           1,
			stdout:         "::error::chart 0.1.0 is in oci://ghcr.io/mikluko/nats-operator/charts but v0.1.0 is untagged; re-run the failed jobs of the release run that pushed it",
			requested:      []string{tokenURL, manifest},
		},
		{
			name:           "404 MANIFEST_UNKNOWN",
			version:        version,
			tagged:         "false",
			tokenStatus:    "200",
			tokenBody:      granted,
			manifestStatus: "404",
			manifestBody:   registryError("MANIFEST_UNKNOWN", "manifest unknown"),
			outputs:        map[string]string{"due": "true", "reason": absent},
			requested:      []string{tokenURL, manifest},
		},
		{
			name:           "404 NAME_UNKNOWN",
			version:        version,
			tagged:         "false",
			tokenStatus:    "200",
			tokenBody:      granted,
			manifestStatus: "404",
			manifestBody:   registryError("NAME_UNKNOWN", "repository name not known to registry"),
			outputs:        map[string]string{"due": "true", "reason": absent},
			requested:      []string{tokenURL, manifest},
		},
		{
			name:           "404 without an error body",
			version:        version,
			tagged:         "false",
			tokenStatus:    "200",
			tokenBody:      granted,
			manifestStatus: "404",
			manifestBody:   "404 page not found",
			exit:           1,
			stdout:         "::error::ghcr.io answered 404 with no error code for mikluko/nats-operator/charts/nats-operator:0.1.0",
			requested:      []string{tokenURL, manifest},
		},
		{
			name:           "401 UNAUTHORIZED",
			version:        version,
			tagged:         "false",
			tokenStatus:    "200",
			tokenBody:      granted,
			manifestStatus: "401",
			manifestBody:   registryError("UNAUTHORIZED", "authentication required"),
			exit:           1,
			stdout:         "::error::ghcr.io answered 401 UNAUTHORIZED for mikluko/nats-operator/charts/nats-operator:0.1.0",
			requested:      []string{tokenURL, manifest},
		},
		{
			name:           "403 DENIED",
			version:        version,
			tagged:         "false",
			tokenStatus:    "200",
			tokenBody:      granted,
			manifestStatus: "403",
			manifestBody:   registryError("DENIED", "requested access to the resource is denied"),
			exit:           1,
			stdout:         "::error::ghcr.io answered 403 DENIED for mikluko/nats-operator/charts/nats-operator:0.1.0",
			requested:      []string{tokenURL, manifest},
		},
		{
			name:           "404 DENIED",
			version:        version,
			tagged:         "false",
			tokenStatus:    "200",
			tokenBody:      granted,
			manifestStatus: "404",
			manifestBody:   registryError("DENIED", "requested access to the resource is denied"),
			exit:           1,
			stdout:         "::error::ghcr.io answered 404 DENIED",
			requested:      []string{tokenURL, manifest},
		},
		{
			name:           "500",
			version:        version,
			tagged:         "false",
			tokenStatus:    "200",
			tokenBody:      granted,
			manifestStatus: "500",
			exit:           1,
			stdout:         "::error::ghcr.io answered 500 with no error code",
			requested:      []string{tokenURL, manifest},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := t.TempDir()
			bin := filepath.Join(stub, "bin")
			require.NoError(t, os.Mkdir(bin, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(bin, "curl"), []byte(stubCurl), 0o755))
			for name, content := range map[string]string{
				"token.status":    tc.tokenStatus,
				"token.json":      tc.tokenBody,
				"manifest.status": tc.manifestStatus,
				"manifest.json":   tc.manifestBody,
				"package.status":  tc.packageStatus,
			} {
				require.NoError(t, os.WriteFile(filepath.Join(stub, name), []byte(content), 0o644))
			}
			output := filepath.Join(stub, "output")
			require.NoError(t, os.WriteFile(output, nil, 0o644))

			cmd := exec.Command(bash, "--noprofile", "--norc", "-e", "-c", due.Run)
			cmd.Env = []string{
				"PATH=" + bin + ":" + filepath.Dir(jq) + ":" + filepath.Dir(bash) + ":/usr/bin:/bin",
				"STUB=" + stub,
				"CHART_REPOSITORY=" + wf.Env["CHART_REPOSITORY"],
				"GITHUB_ACTOR=" + actor,
				"GITHUB_API_URL=https://api.github.com",
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
			require.Len(t, args, len(tc.requested))
			configs := map[string]string{
				tokenURL: `user = "` + actor + `:` + token + `"`,
				manifest: `header = "Authorization: Bearer ` + bearer + `"`,
				pkgURL:   `header = "Authorization: Bearer ` + token + `"`,
			}
			for i, a := range args {
				require.True(t, strings.HasSuffix(a, " "+tc.requested[i]), a)
				require.NotContains(t, a, token)
				require.NotContains(t, a, bearer)
				require.Equal(t, configs[tc.requested[i]], config[i])
			}
			if slices.Contains(tc.requested, manifest) {
				require.Contains(t, string(stdout), "::add-mask::"+bearer)
			}
		})
	}
}
