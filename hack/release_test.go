package hack_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	yamlv3 "go.yaml.in/yaml/v3"
	"sigs.k8s.io/yaml"
)

type step struct {
	ID   string            `json:"id"`
	Name string            `json:"name"`
	Uses string            `json:"uses"`
	Run  string            `json:"run"`
	Env  map[string]string `json:"env"`
	With map[string]string `json:"with"`
}

// needs is a job's needs, which a workflow writes as one job or a list.
type needs []string

func (n *needs) UnmarshalJSON(b []byte) error {
	var one string
	if json.Unmarshal(b, &one) == nil {
		*n = needs{one}
		return nil
	}
	return json.Unmarshal(b, (*[]string)(n))
}

type workflow struct {
	Env  map[string]string `json:"env"`
	Jobs map[string]struct {
		If    string            `json:"if"`
		Needs needs             `json:"needs"`
		Env   map[string]string `json:"env"`
		Steps []step            `json:"steps"`
		Uses  string            `json:"uses"`
		With  map[string]string `json:"with"`
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

// TestRelease_WaitsForCI holds the release to ci: it starts on ci completing
// on main, or by hand, and never on a push of its own; only the first
// publishes.
func TestRelease_WaitsForCI(t *testing.T) {
	b, err := os.ReadFile("../.github/workflows/release.yml")
	require.NoError(t, err)
	var wf struct {
		On map[string]yamlv3.Node `yaml:"on"`
	}
	require.NoError(t, yamlv3.Unmarshal(b, &wf))
	require.ElementsMatch(t, []string{"workflow_run", "workflow_dispatch"}, keys(wf.On))

	var run struct {
		Workflows []string `yaml:"workflows"`
		Types     []string `yaml:"types"`
		Branches  []string `yaml:"branches"`
	}
	node := wf.On["workflow_run"]
	require.NoError(t, node.Decode(&run))
	require.Equal(t, []string{"ci"}, run.Workflows)
	require.Equal(t, []string{"completed"}, run.Types)
	require.Equal(t, []string{"main"}, run.Branches)

	var dispatch struct {
		Inputs map[string]yamlv3.Node `yaml:"inputs"`
	}
	node = wf.On["workflow_dispatch"]
	require.NoError(t, node.Decode(&dispatch))
	require.Empty(t, dispatch.Inputs)

	plan := readWorkflow(t, "release.yml").Jobs["plan"]
	require.Contains(t, plan.If, "github.event.workflow_run.conclusion == 'success'")
	publish := stepByID(t, plan.Steps, "publish")
	require.Equal(t, "${{ github.event_name == 'workflow_run' }}", publish.Env["PUBLISH"])
}

// stepByID returns the step of steps whose id is id.
func stepByID(t *testing.T, steps []step, id string) step {
	t.Helper()
	for _, s := range steps {
		if s.ID == id {
			return s
		}
	}
	require.Failf(t, "no step", "id %s", id)
	return step{}
}

// TestRelease_InvalidChangelogFails holds the plan to the changelog action's
// failing step: an invalid CHANGELOG.md fails the plan job, which has no
// branch of its own for it.
func TestRelease_InvalidChangelogFails(t *testing.T) {
	plan := readWorkflow(t, "release.yml").Jobs["plan"]
	var changelog *step
	for i, s := range plan.Steps {
		if s.ID == "changelog" {
			changelog = &plan.Steps[i]
		}
	}
	require.NotNil(t, changelog)
	require.True(t, strings.HasPrefix(changelog.Uses, "mikluko/action-changelog@"), changelog.Uses)
	require.Contains(t, []string{"", "error"}, changelog.With["fail-on"])

	ci, err := os.ReadFile("../.github/workflows/ci.yml")
	require.NoError(t, err)
	var name struct {
		Name string `yaml:"name"`
	}
	require.NoError(t, yamlv3.Unmarshal(ci, &name))
	require.Equal(t, "ci", name.Name)
}

// TestDocs_DeployedOnRelease holds the site to the released version: only the
// release workflow deploys it, after the release, at the commit it tagged.
func TestDocs_DeployedOnRelease(t *testing.T) {
	b, err := os.ReadFile("../.github/workflows/docs.yml")
	require.NoError(t, err)
	var on struct {
		On map[string]yamlv3.Node `yaml:"on"`
	}
	require.NoError(t, yamlv3.Unmarshal(b, &on))
	require.ElementsMatch(t, []string{"pull_request", "workflow_dispatch", "workflow_call"}, keys(on.On))

	docs := readWorkflow(t, "docs.yml")
	require.Equal(t, "inputs.ref != ''", docs.Jobs["deploy"].If)
	require.Equal(t, "${{ inputs.ref }}", docs.Jobs["build"].Steps[0].With["ref"])

	release := readWorkflow(t, "release.yml")
	var callers []string
	for name, job := range release.Jobs {
		if job.Uses == "./.github/workflows/docs.yml" {
			callers = append(callers, name)
			require.Contains(t, job.Needs, "release")
			require.Equal(t, "${{ github.event.workflow_run.head_sha }}", job.With["ref"])
			require.Equal(t, "needs.plan.outputs.publish == 'true'", job.If)
		}
	}
	require.Len(t, callers, 1)
	require.Contains(t, release.Env["SHA"], "github.event.workflow_run.head_sha")
}

func keys[V any](m map[string]V) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
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

// controllers are the commands under cmd/ named *-controller.
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

// TestHugoInstalledOnce pins the Hugo release and its checksum to the
// setup-hugo action: the workflows that build the site use it, and none names
// a version of its own.
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
	require.Regexp(t, `^[0-9a-f]{64}$`, action.Runs.Steps[0].Env["HUGO_SHA256"])
	require.Contains(t, action.Runs.Steps[0].Run, "sha256sum -c")

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

// TestCI_LintsWorkflows holds ci to actionlint and to zizmor under the
// repository's zizmor configuration.
func TestCI_LintsWorkflows(t *testing.T) {
	ci := readWorkflow(t, "ci.yml")
	var actionlint, zizmor bool
	for _, s := range ci.Jobs["actionlint"].Steps {
		actionlint = actionlint || strings.Contains(s.Run, "github.com/rhysd/actionlint/cmd/actionlint@")
	}
	for _, s := range ci.Jobs["zizmor"].Steps {
		if strings.HasPrefix(s.Uses, "zizmorcore/zizmor-action@") {
			zizmor = true
			require.Equal(t, ".github/zizmor.yml", s.With["config"])
			require.NotEmpty(t, s.With["version"])
		}
	}
	require.True(t, actionlint, "ci runs no actionlint")
	require.True(t, zizmor, "ci runs no zizmor")
	_, err := os.Stat("../.github/zizmor.yml")
	require.NoError(t, err)
}

// TestCI_RunsQuickstart holds ci to story 1 end to end on one cluster,
// through the workflow a manual e2e run uses.
func TestCI_RunsQuickstart(t *testing.T) {
	e2e := readWorkflow(t, "ci.yml").Jobs["e2e"]
	require.Equal(t, "./.github/workflows/e2e.yml", e2e.Uses)
	require.Equal(t, "1", e2e.With["stories"])

	stories := readWorkflow(t, "e2e.yml").Jobs["stories"]
	run := stepByName(t, stories.Steps, "Stories")
	require.Equal(t, "1", run.Env["E2E_CLUSTERS"])
	require.Equal(t, "${{ inputs.stories }}", run.Env["E2E_STORIES"])

	b, err := os.ReadFile("../.github/workflows/e2e.yml")
	require.NoError(t, err)
	var on struct {
		On map[string]yamlv3.Node `yaml:"on"`
	}
	require.NoError(t, yamlv3.Unmarshal(b, &on))
	require.ElementsMatch(t, []string{"workflow_dispatch", "workflow_call"}, keys(on.On))

	quickstart, err := filepath.Glob("../docs/content/docs/stories/01-*/index.md")
	require.NoError(t, err)
	require.Len(t, quickstart, 1)
}

// stepByName returns the step of steps whose name is name.
func stepByName(t *testing.T, steps []step, name string) step {
	t.Helper()
	for _, s := range steps {
		if s.Name == name {
			return s
		}
	}
	require.Failf(t, "no step", "name %s", name)
	return step{}
}

// TestGoToolchainOnce holds every workflow's Go to go.mod's toolchain line:
// each setup-go reads go.mod, and none names a version of its own.
func TestGoToolchainOnce(t *testing.T) {
	out, err := exec.Command("go", "mod", "edit", "-json", "../go.mod").Output()
	require.NoError(t, err)
	var mod struct{ Toolchain string }
	require.NoError(t, json.Unmarshal(out, &mod))
	require.Regexp(t, `^go1\.\d+\.\d+$`, mod.Toolchain)

	files, err := filepath.Glob("../.github/workflows/*.yml")
	require.NoError(t, err)
	var setups int
	for _, f := range files {
		for job, j := range readWorkflow(t, filepath.Base(f)).Jobs {
			for _, s := range j.Steps {
				if strings.HasPrefix(s.Uses, "actions/setup-go@") {
					setups++
					require.Equal(t, "go.mod", s.With["go-version-file"], "%s job %s", f, job)
					require.Empty(t, s.With["go-version"], "%s job %s", f, job)
				}
			}
		}
	}
	require.NotZero(t, setups)
}

// TestHelmVersionOnce holds every job that installs Helm to the one release
// ci tests the chart with.
func TestHelmVersionOnce(t *testing.T) {
	versions := map[string]bool{}
	for _, name := range []string{"ci.yml", "release.yml"} {
		for job, j := range readWorkflow(t, name).Jobs {
			for _, s := range j.Steps {
				if strings.HasPrefix(s.Uses, "azure/setup-helm@") {
					require.NotEmpty(t, s.With["version"], "%s job %s", name, job)
					versions[s.With["version"]] = true
				}
			}
		}
	}
	require.Len(t, versions, 1, "%v", versions)
}

// TestMachineBasePinned holds the e2e machine's base image to a digest.
func TestMachineBasePinned(t *testing.T) {
	b, err := os.ReadFile("machine.Containerfile")
	require.NoError(t, err)
	require.Regexp(t, `(?m)^FROM \S+@sha256:[0-9a-f]{64}$`, string(b))
}

// TestKoBasePinned holds the controller images' base to a digest.
func TestKoBasePinned(t *testing.T) {
	b, err := os.ReadFile("../.ko.yaml")
	require.NoError(t, err)
	var ko struct {
		DefaultBaseImage string `json:"defaultBaseImage"`
	}
	require.NoError(t, yaml.Unmarshal(b, &ko))
	require.Regexp(t, `^\S+@sha256:[0-9a-f]{64}$`, ko.DefaultBaseImage)
}

// TestRelease_AttestsBeforeRelease holds the tag and the GitHub release to
// the build provenance of the images and the chart, and the chart to the
// images it deploys.
func TestRelease_AttestsBeforeRelease(t *testing.T) {
	wf := readWorkflow(t, "release.yml")
	require.Subset(t, wf.Jobs["release"].Needs, []string{"images", "provenance", "chart"})
	require.Contains(t, wf.Jobs["chart"].Needs, "images")
	for _, job := range []string{"provenance", "chart"} {
		var attests bool
		for _, s := range wf.Jobs[job].Steps {
			attests = attests || strings.HasPrefix(s.Uses, "actions/attest-build-provenance@")
		}
		require.True(t, attests, "job %s does not attest", job)
	}
}

// renovateConfig is the part of .github/renovate.json its regex managers
// are read from.
type renovateConfig struct {
	CustomManagers []struct {
		ManagerFilePatterns []string `json:"managerFilePatterns"`
		MatchStrings        []string `json:"matchStrings"`
		DepNameTemplate     string   `json:"depNameTemplate"`
	} `json:"customManagers"`
}

// renovateMatches returns, per dependency name, how many pins in the
// repository's tracked files the regex managers of cfg find.
func renovateMatches(t *testing.T, cfg renovateConfig) map[string]int {
	t.Helper()
	cmd := exec.Command("git", "ls-files")
	cmd.Dir = ".."
	out, err := cmd.Output()
	require.NoError(t, err)
	files := strings.Fields(string(out))

	found := map[string]int{}
	for i, m := range cfg.CustomManagers {
		var hits int
		for _, p := range m.ManagerFilePatterns {
			require.True(t, strings.HasPrefix(p, "/") && strings.HasSuffix(p, "/"), "manager %d: %s", i, p)
			pattern := regexp.MustCompile(p[1 : len(p)-1])
			for _, f := range files {
				if !pattern.MatchString(f) {
					continue
				}
				b, err := os.ReadFile(filepath.Join("..", f))
				require.NoError(t, err)
				for _, s := range m.MatchStrings {
					re := regexp.MustCompile(s)
					for _, sub := range re.FindAllStringSubmatch(string(b), -1) {
						dep := m.DepNameTemplate
						if j := re.SubexpIndex("depName"); j >= 0 {
							dep = sub[j]
						}
						require.NotEmpty(t, dep, "manager %d in %s", i, f)
						found[dep]++
						hits++
					}
				}
			}
		}
		require.NotZero(t, hits, "manager %d matches nothing", i)
	}
	return found
}

// TestRenovate_WatchesToolPins holds every tool version an action input pins
// to a Renovate regex manager that finds it.
func TestRenovate_WatchesToolPins(t *testing.T) {
	b, err := os.ReadFile("../.github/renovate.json")
	require.NoError(t, err)
	var cfg renovateConfig
	require.NoError(t, json.Unmarshal(b, &cfg))
	found := renovateMatches(t, cfg)

	inputs := map[string]struct{ input, dep string }{
		"azure/setup-helm@":              {"version", "helm/helm"},
		"ko-build/setup-ko@":             {"version", "ko-build/ko"},
		"golangci/golangci-lint-action@": {"version", "golangci/golangci-lint"},
		"extractions/setup-just@":        {"just-version", "casey/just"},
		"zizmorcore/zizmor-action@":      {"version", "zizmorcore/zizmor"},
	}
	files, err := filepath.Glob("../.github/workflows/*.yml")
	require.NoError(t, err)
	pinned := map[string]int{}
	for _, f := range files {
		for job, j := range readWorkflow(t, filepath.Base(f)).Jobs {
			for _, s := range j.Steps {
				for prefix, in := range inputs {
					if strings.HasPrefix(s.Uses, prefix) {
						require.NotEmpty(t, s.With[in.input], "%s job %s: %s", f, job, s.Uses)
						pinned[in.dep]++
					}
				}
			}
		}
	}
	for dep, n := range pinned {
		require.GreaterOrEqual(t, found[dep], n, dep)
	}
	for _, dep := range []string{
		"helm-unittest/helm-unittest",
		"gohugoio/hugo",
		"lycheeverse/lychee",
		"cgr.dev/chainguard/static",
		"kubernetes-sigs/kind",
		"golang.org/x/vuln",
		"github.com/rhysd/actionlint",
	} {
		require.NotZero(t, found[dep], dep)
	}
}
