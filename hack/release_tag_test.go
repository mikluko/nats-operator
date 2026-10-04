package hack_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	yamlv3 "go.yaml.in/yaml/v3"
)

// triggers is a workflow's on: the events it names, sorted, and what it says
// of push.
type triggers struct {
	Events []string
	Push   struct {
		Branches []string `yaml:"branches"`
		Tags     []string `yaml:"tags"`
	}
}

func readTriggers(t *testing.T, name string) triggers {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("../.github/workflows", name))
	require.NoError(t, err)
	var wf struct {
		On map[string]yamlv3.Node `yaml:"on"`
	}
	require.NoError(t, yamlv3.Unmarshal(b, &wf))
	var on triggers
	for event, node := range wf.On {
		on.Events = append(on.Events, event)
		if event == "push" {
			require.NoError(t, node.Decode(&on.Push))
		}
	}
	slices.Sort(on.Events)
	return on
}

// stubGit is a git that records its arguments in $STUB/calls and exits 1
// where $STUB/fail names its first.
const stubGit = `#!/usr/bin/env bash
echo "$*" >>"$STUB/calls"
[ "$1" != "$(cat "$STUB/fail")" ]
`

// publishes is the condition every publishing job or step of release-roll.yaml
// carries.
const publishes = "needs.plan.outputs.publish == 'true'"

// TestCut_TagsAsTheAppAfterCI pins that release-cut runs on a push to main
// and on nothing else, that its cut waits on ci.yaml called as a job, which no
// push starts by itself, and that the cut pushes the tag only where the
// changelog action reports a release due, on the pushed commit, with the
// app's token, which its GITHUB_TOKEN cannot do.
func TestCut_TagsAsTheAppAfterCI(t *testing.T) {
	on := readTriggers(t, "release-cut.yaml")
	require.Equal(t, []string{"push"}, on.Events)
	require.Equal(t, []string{"main"}, on.Push.Branches)
	require.Empty(t, on.Push.Tags)
	require.Equal(t, []string{"pull_request", "workflow_call"}, readTriggers(t, "ci.yaml").Events)

	wf := readWorkflow(t, "release-cut.yaml")
	require.Equal(t, map[string]string{"contents": "read"}, wf.Permissions)
	require.Len(t, wf.Jobs, 2)
	require.Equal(t, "./.github/workflows/ci.yaml", wf.Jobs["ci"].Uses)
	require.Empty(t, wf.Jobs["ci"].If)
	cut := wf.Jobs["cut"]
	require.Equal(t, needs{"ci"}, cut.Needs)
	require.Empty(t, cut.If)
	require.Empty(t, cut.Permissions)

	require.Len(t, cut.Steps, 4)
	app, checkout, changelog, tag := cut.Steps[0], cut.Steps[1], cut.Steps[2], cut.Steps[3]
	require.Equal(t, "app-token", app.ID)
	require.True(t, strings.HasPrefix(app.Uses, "actions/create-github-app-token@"), app.Uses)
	require.Equal(t, inputs{
		"client-id":           "${{ vars.RELEASER_APP_CLIENT_ID }}",
		"private-key":         "${{ secrets.RELEASER_APP_CERT }}",
		"permission-contents": "write",
	}, app.With)

	require.True(t, strings.HasPrefix(checkout.Uses, "actions/checkout@"), checkout.Uses)
	require.Equal(t, inputs{
		"fetch-depth": "0",
		"token":       "${{ steps.app-token.outputs.token }}",
	}, checkout.With)

	require.Equal(t, "changelog", changelog.ID)
	require.Empty(t, changelog.With)
	require.Equal(t, "Cut", tag.Name)
	require.Equal(t, "steps.changelog.outputs.due == 'true'", tag.If)
	require.Equal(t, map[string]string{"VERSION": "${{ steps.changelog.outputs.version }}"}, tag.Env)

	bash, err := exec.LookPath("bash")
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		fail    string
		exit    int
		summary string
	}{
		{
			name:    "the tag is pushed",
			summary: "Cut **v0.1.0**.\n",
		},
		{
			name: "the push is refused",
			fail: "push",
			exit: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := t.TempDir()
			bin := filepath.Join(stub, "bin")
			require.NoError(t, os.Mkdir(bin, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(bin, "git"), []byte(stubGit), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(stub, "fail"), []byte(tc.fail), 0o644))
			summary := filepath.Join(stub, "summary")
			require.NoError(t, os.WriteFile(summary, nil, 0o644))

			cmd := exec.Command(bash, "--noprofile", "--norc", "-e", "-c", tag.Run)
			cmd.Env = []string{
				"PATH=" + bin + ":" + filepath.Dir(bash) + ":/usr/bin:/bin",
				"STUB=" + stub,
				"GITHUB_STEP_SUMMARY=" + summary,
				"VERSION=0.1.0",
			}
			out, err := cmd.CombinedOutput()
			if tc.exit == 0 {
				require.NoError(t, err, "%s", out)
			} else {
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, "%s", out)
				require.Equal(t, tc.exit, exit.ExitCode(), "%s", out)
			}

			calls, err := os.ReadFile(filepath.Join(stub, "calls"))
			require.NoError(t, err)
			require.Equal(t, "tag v0.1.0\npush origin refs/tags/v0.1.0\n", string(calls))
			b, err := os.ReadFile(summary)
			require.NoError(t, err)
			require.Equal(t, tc.summary, string(b))
		})
	}
}

// TestWorkflows_OneCheckoutKeepsCredentials holds every checkout of every
// workflow to persist-credentials: false, apart from the one cut pushes the
// tag through.
func TestWorkflows_OneCheckoutKeepsCredentials(t *testing.T) {
	files, err := filepath.Glob("../.github/workflows/*.yaml")
	require.NoError(t, err)
	var keeps []string
	for _, f := range files {
		name := filepath.Base(f)
		for job, j := range readWorkflow(t, name).Jobs {
			for _, s := range j.Steps {
				if !strings.HasPrefix(s.Uses, "actions/checkout@") {
					continue
				}
				if s.With["persist-credentials"] != "false" {
					keeps = append(keeps, name+" "+job)
				}
			}
		}
	}
	require.Equal(t, []string{"release-cut.yaml cut"}, keeps)
}

// TestRelease_VersionFromTag pins that release-roll.yaml runs on a pushed v tag or
// by hand and on nothing else, that a tag run is titled with its tag, takes
// its version from it, and fails unless that is the newest version
// CHANGELOG.md names, and that a run by hand takes the changelog's version,
// builds it only while the changelog action reports it due, a prerelease
// included, and never publishes.
func TestRelease_VersionFromTag(t *testing.T) {
	on := readTriggers(t, "release-roll.yaml")
	require.Equal(t, []string{"push", "workflow_dispatch"}, on.Events)
	require.Equal(t, []string{"v*"}, on.Push.Tags)
	require.Empty(t, on.Push.Branches)

	b, err := os.ReadFile("../.github/workflows/release-roll.yaml")
	require.NoError(t, err)
	var titled struct {
		RunName string `yaml:"run-name"`
	}
	require.NoError(t, yamlv3.Unmarshal(b, &titled))
	require.Equal(t, "${{ github.event_name == 'push' && format('Releasing {0}', github.ref_name) || format('Dry run of a release from {0}', github.ref_name) }}", titled.RunName)

	wf := readWorkflow(t, "release-roll.yaml")
	plan := wf.Jobs["plan"]
	require.Empty(t, plan.If)
	require.Equal(t, "${{ steps.version.outputs.due }}", plan.Outputs["due"])
	require.Equal(t, "${{ steps.version.outputs.reason }}", plan.Outputs["reason"])
	require.Equal(t, "${{ steps.version.outputs.version }}", plan.Outputs["version"])
	require.Equal(t, "v${{ steps.version.outputs.version }}", plan.Outputs["tag"])
	require.Equal(t, "${{ steps.version.outputs.publish }}", plan.Outputs["publish"])
	for _, job := range []string{"images", "chart"} {
		require.Contains(t, wf.Jobs[job].Needs, "plan", job)
		require.Equal(t, "needs.plan.outputs.due == 'true'", wf.Jobs[job].If, job)
	}

	version := stepByID(t, plan.Steps, "version")
	require.Empty(t, version.If)
	require.Equal(t, map[string]string{
		"EVENT":      "${{ github.event_name }}",
		"REF_NAME":   "${{ github.ref_name }}",
		"NEWEST":     "${{ steps.changelog.outputs.version }}",
		"DUE":        "${{ steps.changelog.outputs.due }}",
		"DUE_REASON": "${{ steps.changelog.outputs['due-reason'] }}",
	}, version.Env)
	require.Equal(t, inputs{"due-prerelease": "true"}, stepByID(t, plan.Steps, "changelog").With)

	bash, err := exec.LookPath("bash")
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		event   string
		ref     string
		newest  string
		due     string
		reason  string
		exit    int
		stdout  string
		outputs map[string]string
	}{
		{
			name:   "tag is the newest version",
			event:  "push",
			ref:    "v0.1.0",
			newest: "0.1.0",
			due:    "false",
			reason: "0.1.0 is already tagged",
			outputs: map[string]string{
				"due": "true", "publish": "true", "version": "0.1.0",
				"reason": "v0.1.0 is the newest version CHANGELOG.md names",
			},
		},
		{
			name:   "tag is an older version",
			event:  "push",
			ref:    "v0.1.0",
			newest: "0.2.0",
			due:    "true",
			exit:   1,
			stdout: "::error::tag v0.1.0 is not the newest version CHANGELOG.md names, 0.2.0",
		},
		{
			name:   "tag without the v of the version",
			event:  "push",
			ref:    "v0.1.0-rc.1",
			newest: "0.1.0",
			due:    "true",
			exit:   1,
			stdout: "::error::tag v0.1.0-rc.1 is not the newest version CHANGELOG.md names, 0.1.0",
		},
		{
			name:   "tag, no version",
			event:  "push",
			ref:    "v",
			due:    "true",
			exit:   1,
			stdout: "::error::tag v is not the newest version CHANGELOG.md names, which is none",
		},
		{
			name:   "by hand, untagged",
			event:  "workflow_dispatch",
			ref:    "release-0.1.0",
			newest: "0.1.0",
			due:    "true",
			outputs: map[string]string{
				"due": "true", "publish": "false", "version": "0.1.0",
				"reason": "",
			},
		},
		{
			name:   "by hand, tagged",
			event:  "workflow_dispatch",
			ref:    "main",
			newest: "0.1.0",
			due:    "false",
			reason: "0.1.0 is already tagged",
			outputs: map[string]string{
				"due": "false", "publish": "false", "version": "0.1.0",
				"reason": "0.1.0 is already tagged",
			},
		},
		{
			name:   "by hand on a tag",
			event:  "workflow_dispatch",
			ref:    "v0.1.0",
			newest: "0.1.0",
			due:    "false",
			reason: "0.1.0 is already tagged",
			outputs: map[string]string{
				"due": "false", "publish": "false", "version": "0.1.0",
				"reason": "0.1.0 is already tagged",
			},
		},
		{
			name:   "by hand, no version",
			event:  "workflow_dispatch",
			ref:    "main",
			due:    "false",
			reason: "the changelog names no version",
			outputs: map[string]string{
				"due": "false", "publish": "false", "version": "",
				"reason": "the changelog names no version",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "output")
			require.NoError(t, os.WriteFile(output, nil, 0o644))
			cmd := exec.Command(bash, "--noprofile", "--norc", "-e", "-c", version.Run)
			cmd.Env = []string{
				"PATH=" + filepath.Dir(bash) + ":/usr/bin:/bin",
				"GITHUB_OUTPUT=" + output,
				"EVENT=" + tc.event,
				"REF_NAME=" + tc.ref,
				"NEWEST=" + tc.newest,
				"DUE=" + tc.due,
				"DUE_REASON=" + tc.reason,
			}
			stdout, err := cmd.Output()
			if tc.exit == 0 {
				require.NoError(t, err, "%s", stdout)
			} else {
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
		})
	}
}

// TestRelease_DispatchPublishesNothing holds every step of release-roll.yaml that
// logs in to a registry, pushes, signs, attests or creates a release to a
// run that publishes, which a run by hand is not, and holds the release to
// the tag that triggered the run.
func TestRelease_DispatchPublishesNothing(t *testing.T) {
	wf := readWorkflow(t, "release-roll.yaml")
	var gated int
	for name, job := range wf.Jobs {
		for _, s := range job.Steps {
			var publishing bool
			for _, cmd := range []string{"docker login", "helm registry login", "helm push", "cosign sign", "gh release", "git push", "git tag"} {
				publishing = publishing || strings.Contains(s.Run, cmd)
			}
			publishing = publishing || strings.HasPrefix(s.Uses, "actions/attest-build-provenance@")
			if !publishing {
				continue
			}
			gated++
			require.Contains(t, []string{job.If, s.If}, publishes, "job %s step %q", name, s.Name+s.Uses)
		}
		if job.Permissions["contents"] == "write" {
			require.Equal(t, "release", name)
		}
	}
	require.NotZero(t, gated)

	build := stepByName(t, wf.Jobs["images"].Steps, "Build")
	require.Equal(t, "${{ "+publishes+" }}", build.Env["PUSH"])
	require.Contains(t, build.Run, `--push="$PUSH"`)

	release := stepByName(t, wf.Jobs["release"].Steps, "Release")
	require.Equal(t, "${{ needs.plan.outputs.tag }}", release.Env["TAG"])
	require.Contains(t, release.Run, `gh release create "$TAG" --verify-tag `)
	require.NotContains(t, release.Run, "--target")
}
