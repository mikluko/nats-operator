package hack_test

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	yamlv3 "go.yaml.in/yaml/v3"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/semver"
	"sigs.k8s.io/yaml"

	"github.com/mikluko/nats-operator/internal/e2e"
)

type step struct {
	ID   string            `json:"id"`
	If   string            `json:"if"`
	Name string            `json:"name"`
	Uses string            `json:"uses"`
	Run  string            `json:"run"`
	Env  map[string]string `json:"env"`
	With inputs            `json:"with"`
}

// inputs is a step's with, each value as the string the runner passes, since
// YAML reads cache: false as a boolean.
type inputs map[string]string

func (in *inputs) UnmarshalJSON(b []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*in = inputs{}
	for k, v := range raw {
		(*in)[k] = fmt.Sprint(v)
	}
	return nil
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
		If          string            `json:"if"`
		Needs       needs             `json:"needs"`
		Permissions map[string]string `json:"permissions"`
		Outputs     map[string]string `json:"outputs"`
		Env         map[string]string `json:"env"`
		Strategy    struct {
			Matrix map[string]any `json:"matrix"`
		} `json:"strategy"`
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

func TestRelease_FailOnAndCIName(t *testing.T) {
	changelog := stepByID(t, readWorkflow(t, "release.yml").Jobs["plan"].Steps, "changelog")
	require.True(t, strings.HasPrefix(changelog.Uses, "mikluko/action-changelog@"), changelog.Uses)
	require.Contains(t, []string{"", "error"}, changelog.With["fail-on"])

	var release struct {
		On struct {
			WorkflowRun struct {
				Workflows []string `yaml:"workflows"`
			} `yaml:"workflow_run"`
		} `yaml:"on"`
	}
	b, err := os.ReadFile("../.github/workflows/release.yml")
	require.NoError(t, err)
	require.NoError(t, yamlv3.Unmarshal(b, &release))

	var ci struct {
		Name string `yaml:"name"`
	}
	b, err = os.ReadFile("../.github/workflows/ci.yml")
	require.NoError(t, err)
	require.NoError(t, yamlv3.Unmarshal(b, &ci))
	require.Equal(t, []string{ci.Name}, release.On.WorkflowRun.Workflows)
}

// TestE2E_Nightly pins that e2e runs two Kubernetes clusters on its daily
// schedule, three on its weekly one, the number a manual run asks for, and
// one otherwise.
func TestE2E_Nightly(t *testing.T) {
	var e2eWorkflow struct {
		On struct {
			Schedule []struct {
				Cron string `yaml:"cron"`
			} `yaml:"schedule"`
			WorkflowDispatch struct {
				Inputs struct {
					Clusters struct {
						Type    string   `yaml:"type"`
						Options []string `yaml:"options"`
						Default string   `yaml:"default"`
					} `yaml:"clusters"`
				} `yaml:"inputs"`
			} `yaml:"workflow_dispatch"`
		} `yaml:"on"`
	}
	b, err := os.ReadFile("../.github/workflows/e2e.yml")
	require.NoError(t, err)
	require.NoError(t, yamlv3.Unmarshal(b, &e2eWorkflow))
	require.Len(t, e2eWorkflow.On.Schedule, 2)
	daily, weekly := e2eWorkflow.On.Schedule[0].Cron, e2eWorkflow.On.Schedule[1].Cron
	for _, cron := range []string{daily, weekly} {
		require.Len(t, strings.Fields(cron), 5, "a five-field cron: %q", cron)
	}
	require.Equal(t, []string{"*", "*", "*"}, strings.Fields(daily)[2:], "daily: %q", daily)
	require.Equal(t, []string{"*", "*"}, strings.Fields(weekly)[2:4], "weekly: %q", weekly)
	require.NotEqual(t, "*", strings.Fields(weekly)[4], "weekly: %q", weekly)
	clusters := e2eWorkflow.On.WorkflowDispatch.Inputs.Clusters
	require.Equal(t, "choice", clusters.Type)
	require.Equal(t, []string{"1", "2", "3"}, clusters.Options)
	require.Equal(t, "1", clusters.Default)

	var run step
	for _, s := range readWorkflow(t, "e2e.yml").Jobs["stories"].Steps {
		if s.Name == "Stories" {
			run = s
		}
	}
	require.Equal(t, "${{ github.event_name == 'schedule' && (github.event.schedule == '"+weekly+"' && '3' || '2') || inputs.clusters || '1' }}", run.Env["E2E_CLUSTERS"])
	require.Equal(t, "${{ inputs.stories }}", run.Env["E2E_STORIES"])
	require.Equal(t, "${{ inputs.watch-namespaces }}", run.Env["E2E_WATCH_NAMESPACES"])
}

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

type controllerValues struct {
	Image struct {
		Repository string  `json:"repository"`
		Digest     *string `json:"digest"`
	} `json:"image"`
}

type chartValues struct {
	Cluster   controllerValues `json:"cluster"`
	Auth      controllerValues `json:"auth"`
	JetStream controllerValues `json:"jetstream"`
}

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

// TestRelease_ChartPinsImageDigests holds the packaged chart to the digests
// the images job builds, on dry runs too, under a values key per controller.
func TestRelease_ChartPinsImageDigests(t *testing.T) {
	wf := readWorkflow(t, "release.yml")
	images := wf.Jobs["images"]
	require.Equal(t, "${{ steps.digests.outputs.digests }}", images.Outputs["digests"])
	require.Empty(t, stepByID(t, images.Steps, "digests").If)

	chart := wf.Jobs["chart"]
	require.Contains(t, chart.Needs, "images")
	pin, pkg := -1, -1
	for i, s := range chart.Steps {
		if s.ID == "pin" {
			pin = i
			require.Empty(t, s.If)
			require.Equal(t, "${{ needs.images.outputs.digests }}", s.Env["DIGESTS"])
		}
		if strings.Contains(s.Run, "helm package") {
			pkg = i
		}
	}
	require.NotEqual(t, -1, pin, "no pin step in the chart job")
	require.Less(t, pin, pkg)

	b, err := os.ReadFile("../charts/nats-operator/values.yaml")
	require.NoError(t, err)
	var values map[string]json.RawMessage
	require.NoError(t, yaml.Unmarshal(b, &values))
	for _, c := range controllers(t) {
		raw, ok := values[strings.TrimSuffix(c, "-controller")]
		require.True(t, ok, c)
		var v controllerValues
		require.NoError(t, json.Unmarshal(raw, &v))
		require.NotNil(t, v.Image.Digest, "%s has no image.digest", c)
		require.Empty(t, *v.Image.Digest, c)
	}
}

func TestRelease_OneDigestList(t *testing.T) {
	wf := readWorkflow(t, "release.yml")
	images := wf.Jobs["images"]
	require.Equal(t, map[string]string{"digests": "${{ steps.digests.outputs.digests }}"}, images.Outputs)

	var builds, signs int
	for _, s := range images.Steps {
		require.NotContains(t, s.Run, "--image-refs", s.Name)
		if s.ID == "digests" {
			builds++
		}
		if strings.Contains(s.Run, "cosign sign") {
			signs++
			require.Equal(t, "${{ steps.digests.outputs.digests }}", s.Env["DIGESTS"])
		}
	}
	require.Equal(t, 1, builds)
	require.Equal(t, 1, signs)
	require.Equal(t, "${{ fromJSON(needs.images.outputs.digests) }}", wf.Jobs["provenance"].Strategy.Matrix["subject"])
}

// TestDocs_BuildJob holds the Pages build job to the permission
// configure-pages reads the site with, and to one Hugo build on a pull request.
func TestDocs_BuildJob(t *testing.T) {
	build := readWorkflow(t, "docs.yml").Jobs["build"]
	require.Equal(t, map[string]string{"contents": "read", "pages": "read"}, build.Permissions)

	var hugo []step
	for _, s := range build.Steps {
		if strings.Contains(s.Run, "hugo ") || strings.Contains(s.Run, "just site-check") {
			hugo = append(hugo, s)
		}
	}
	require.Len(t, hugo, 2)
	require.Contains(t, hugo[0].Run, "just site-check")
	require.Empty(t, hugo[0].If)
	require.Equal(t, "github.event_name != 'pull_request'", hugo[1].If)
}

// TestDocs_Deploy pins that the site deploys from the docs workflow on a push
// to main, and that no release job deploys it.
func TestDocs_Deploy(t *testing.T) {
	var docs struct {
		On struct {
			Push struct {
				Branches []string `yaml:"branches"`
			} `yaml:"push"`
		} `yaml:"on"`
	}
	b, err := os.ReadFile("../.github/workflows/docs.yml")
	require.NoError(t, err)
	require.NoError(t, yamlv3.Unmarshal(b, &docs))
	require.Equal(t, []string{"main"}, docs.On.Push.Branches)
	require.Equal(t, "github.event_name == 'push'", readWorkflow(t, "docs.yml").Jobs["deploy"].If)

	for name, job := range readWorkflow(t, "release.yml").Jobs {
		require.NotContains(t, job.Uses, "docs.yml", name)
		require.NotContains(t, job.Permissions, "pages", name)
	}
}

func TestControllerList(t *testing.T) {
	just, err := exec.LookPath("just")
	if err != nil {
		t.Skip("just is not on PATH")
	}
	out, err := exec.Command(just, "--justfile", "../Justfile", "--evaluate", "controllers").Output()
	require.NoError(t, err)
	require.ElementsMatch(t, controllers(t), strings.Fields(string(out)))
}

const setupHugo = "./.github/actions/setup-hugo"

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
					if filepath.Base(f) == "ci.yml" && s.With["cache"] != "false" {
						require.Equal(t, []string{"go.sum", filepath.Join(filepath.Dir(toolsModfile), "go.sum")},
							strings.Fields(s.With["cache-dependency-path"]), "%s job %s", f, job)
					}
				}
			}
		}
	}
	require.NotZero(t, setups)
}

// TestHelmVersionOnce holds every job that installs Helm to the one release
// ci tests the chart with.
func TestHelmVersionOnce(t *testing.T) {
	files, err := filepath.Glob("../.github/workflows/*.yml")
	require.NoError(t, err)
	versions := map[string]bool{}
	for _, f := range files {
		name := filepath.Base(f)
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

func TestMachineBasePinned(t *testing.T) {
	b, err := os.ReadFile("machine.Containerfile")
	require.NoError(t, err)
	require.Regexp(t, `(?m)^FROM \S+@sha256:[0-9a-f]{64}$`, string(b))
}

func TestKoBasePinned(t *testing.T) {
	b, err := os.ReadFile("../.ko.yaml")
	require.NoError(t, err)
	var ko struct {
		DefaultBaseImage string `json:"defaultBaseImage"`
	}
	require.NoError(t, yaml.Unmarshal(b, &ko))
	require.Regexp(t, `^\S+@sha256:[0-9a-f]{64}$`, ko.DefaultBaseImage)
}

// TestRelease_GoUncached holds every Go toolchain release.yml sets up to
// running without the Actions cache, which ci on main writes under the same key.
func TestRelease_GoUncached(t *testing.T) {
	var setups int
	for job, j := range readWorkflow(t, "release.yml").Jobs {
		for _, s := range j.Steps {
			if strings.HasPrefix(s.Uses, "actions/setup-go@") {
				setups++
				require.Equal(t, "false", s.With["cache"], job)
			}
		}
	}
	require.NotZero(t, setups)
}

func TestRelease_AttestsBeforeRelease(t *testing.T) {
	wf := readWorkflow(t, "release.yml")
	require.Subset(t, wf.Jobs["release"].Needs, []string{"images", "provenance", "chart"})
	for _, job := range []string{"provenance", "chart"} {
		var attests bool
		for _, s := range wf.Jobs[job].Steps {
			attests = attests || strings.HasPrefix(s.Uses, "actions/attest-build-provenance@")
		}
		require.True(t, attests, "job %s does not attest", job)
	}
}

// toolsModfile is the go.mod that pins golangci-lint.
const toolsModfile = "hack/tools/go.mod"

// TestCI_LintsWithToolsPin pins that ci lints through `just lint`, which runs
// the golangci-lint toolsModfile requires, the one pin of its version.
func TestCI_LintsWithToolsPin(t *testing.T) {
	var lints int
	files, err := filepath.Glob("../.github/workflows/*.yml")
	require.NoError(t, err)
	for _, f := range files {
		for job, j := range readWorkflow(t, filepath.Base(f)).Jobs {
			for _, s := range j.Steps {
				require.False(t, strings.HasPrefix(s.Uses, "golangci/golangci-lint-action@"), "%s job %s", f, job)
				require.NotContains(t, s.Run, "golangci-lint ", "%s job %s", f, job)
				if s.Run == "just lint" {
					lints++
				}
			}
		}
	}
	require.Equal(t, 1, lints)

	const lintModule = "github.com/golangci/golangci-lint/v2"
	b, err := os.ReadFile(filepath.Join("..", toolsModfile))
	require.NoError(t, err)
	tools, err := modfile.Parse(toolsModfile, b, nil)
	require.NoError(t, err)
	require.Len(t, tools.Tool, 1)
	require.Equal(t, lintModule+"/cmd/golangci-lint", tools.Tool[0].Path)
	var version string
	for _, r := range tools.Require {
		if r.Mod.Path == lintModule {
			version = r.Mod.Version
		}
	}
	require.True(t, semver.IsValid(version), "%s requires %s at %q", toolsModfile, lintModule, version)

	b, err = os.ReadFile("../go.mod")
	require.NoError(t, err)
	root, err := modfile.Parse("go.mod", b, nil)
	require.NoError(t, err)
	for _, r := range root.Require {
		require.NotEqual(t, lintModule, r.Mod.Path, "go.mod")
	}

	just, err := exec.LookPath("just")
	if err != nil {
		t.Skip("just is not on PATH")
	}
	out, err := exec.Command(just, "--justfile", "../Justfile", "--dump", "--dump-format", "json").Output()
	require.NoError(t, err)
	var dump struct {
		Recipes map[string]struct {
			Body         [][]json.RawMessage `json:"body"`
			Dependencies []json.RawMessage   `json:"dependencies"`
		} `json:"recipes"`
	}
	require.NoError(t, json.Unmarshal(out, &dump))
	lint := dump.Recipes["lint"]
	require.Empty(t, lint.Dependencies)
	require.Len(t, lint.Body, 1)
	var cmdline string
	require.Len(t, lint.Body[0], 1)
	require.NoError(t, json.Unmarshal(lint.Body[0][0], &cmdline))
	args := strings.Fields(cmdline)
	require.Equal(t, []string{"go", "tool", "-modfile=" + toolsModfile, "golangci-lint", "run"}, args)

	cmd := exec.Command(args[0], append(args[1:len(args)-1], "version", "--short")...)
	cmd.Dir = ".."
	out, err = cmd.Output()
	require.NoError(t, err)
	require.Equal(t, strings.TrimPrefix(version, "v"), strings.TrimSpace(string(out)))
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

func TestRenovate_WatchesToolPins(t *testing.T) {
	b, err := os.ReadFile("../.github/renovate.json")
	require.NoError(t, err)
	var cfg renovateConfig
	require.NoError(t, json.Unmarshal(b, &cfg))
	found := renovateMatches(t, cfg)

	inputs := []struct{ prefix, input, dep string }{
		{"azure/setup-helm@", "version", "helm/helm"},
		{"ko-build/setup-ko@", "version", "ko-build/ko"},
		{"extractions/setup-just@", "just-version", "casey/just"},
		{"zizmorcore/zizmor-action@", "version", "zizmorcore/zizmor"},
		{"helm/chart-testing-action@", "version", "helm/chart-testing"},
		{"helm/kind-action@", "version", "kubernetes-sigs/kind"},
		{"helm/kind-action@", "node_image", "kindest/node"},
	}
	files, err := filepath.Glob("../.github/workflows/*.yml")
	require.NoError(t, err)
	pinned := map[string]int{}
	for _, f := range files {
		for job, j := range readWorkflow(t, filepath.Base(f)).Jobs {
			for _, s := range j.Steps {
				for _, in := range inputs {
					if strings.HasPrefix(s.Uses, in.prefix) {
						require.NotEmpty(t, s.With[in.input], "%s job %s: %s", f, job, s.Uses)
						pinned[in.dep]++
					}
				}
			}
		}
	}
	require.NotZero(t, pinned["helm/chart-testing"])
	require.NotZero(t, pinned["kindest/node"])
	for dep, n := range pinned {
		require.GreaterOrEqual(t, found[dep], n, dep)
	}
	for _, dep := range []string{
		"helm-unittest/helm-unittest",
		"gohugoio/hugo",
		"lycheeverse/lychee",
		"cgr.dev/chainguard/static",
		"natsio/prometheus-nats-exporter",
		"golang.org/x/vuln",
		"github.com/rhysd/actionlint",
		"renovate",
	} {
		require.NotZero(t, found[dep], dep)
	}
	require.Equal(t, pinned["kindest/node"]+1, found["kindest/node"], "internal/e2e.KindNodeImage")
	require.Equal(t, 2, found["busybox"], "values.yaml and install.md")

	var gomod struct {
		Gomod struct {
			ManagerFilePatterns []string `json:"managerFilePatterns"`
		} `json:"gomod"`
	}
	require.NoError(t, json.Unmarshal(b, &gomod))
	patterns := gomod.Gomod.ManagerFilePatterns
	if len(patterns) == 0 {
		patterns = []string{`/(^|/)go\.mod$/`}
	}
	require.True(t, slices.ContainsFunc(patterns, func(p string) bool {
		return regexp.MustCompile(p[1 : len(p)-1]).MatchString(toolsModfile)
	}), "gomod manager misses %s", toolsModfile)
}

func TestRenovate_OwnsActionsAndGomod(t *testing.T) {
	b, err := os.ReadFile("../.github/renovate.json")
	require.NoError(t, err)
	var cfg struct {
		EnabledManagers []string `json:"enabledManagers"`
	}
	require.NoError(t, json.Unmarshal(b, &cfg))
	require.Subset(t, cfg.EnabledManagers, []string{"github-actions", "gomod"})
	for _, f := range []string{"../.github/dependabot.yml", "../.github/dependabot.yaml"} {
		require.NoFileExists(t, f)
	}
}

// TestKindPinnedOnce holds ci's kind-action to the kind module and node image
// the e2e harness creates clusters with.
func TestKindPinnedOnce(t *testing.T) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Version}}", "sigs.k8s.io/kind").Output()
	require.NoError(t, err)

	var runs int
	for _, j := range readWorkflow(t, "ci.yml").Jobs {
		for _, s := range j.Steps {
			if strings.HasPrefix(s.Uses, "helm/kind-action@") {
				runs++
				require.Equal(t, strings.TrimSpace(string(out)), s.With["version"])
				require.Equal(t, e2e.KindNodeImage, s.With["node_image"])
			}
		}
	}
	require.NotZero(t, runs)
}

// TestCI_HelmUnittestPinned holds ci's one helm-unittest install to a release
// asset checked against a pinned sha256.
func TestCI_HelmUnittestPinned(t *testing.T) {
	var installs []step
	for _, s := range readWorkflow(t, "ci.yml").Jobs["helm"].Steps {
		if s.Env["HELM_UNITTEST_VERSION"] != "" {
			installs = append(installs, s)
		}
		require.NotContains(t, s.Run, "--verify=false", s.Name)
	}
	require.Len(t, installs, 1)
	require.Regexp(t, `^v\d+\.\d+\.\d+$`, installs[0].Env["HELM_UNITTEST_VERSION"])
	require.Regexp(t, `^[0-9a-f]{64}$`, installs[0].Env["HELM_UNITTEST_SHA256"])
	require.Contains(t, installs[0].Run, "sha256sum -c")
}

// versionPkg and versionVar name the variable .ko.yaml's ldflags set to the
// release.
const (
	versionPkg = "github.com/mikluko/nats-operator/internal/manager"
	versionVar = "Version"
)

// TestKo_LdflagsSetVersion holds every controller's ko build to one ldflag
// setting versionVar, a string variable of versionPkg, to VERSION.
func TestKo_LdflagsSetVersion(t *testing.T) {
	b, err := os.ReadFile("../.ko.yaml")
	require.NoError(t, err)
	var ko struct {
		DefaultBaseImage string `json:"defaultBaseImage"`
		Builds           []struct {
			ID      string   `json:"id"`
			Main    string   `json:"main"`
			Ldflags []string `json:"ldflags"`
		} `json:"builds"`
	}
	require.NoError(t, yaml.UnmarshalStrict(b, &ko))
	flags := map[string][]string{}
	for _, build := range ko.Builds {
		require.Equal(t, "./cmd/"+build.ID, build.Main)
		flags[build.ID] = build.Ldflags
	}
	want := map[string][]string{}
	for _, c := range controllers(t) {
		want[c] = []string{"-X " + versionPkg + "." + versionVar + "={{.Env.VERSION}}"}
	}
	require.Equal(t, want, flags)

	out, err := exec.Command("go", "list", "-f", "{{.Dir}}", versionPkg).Output()
	require.NoError(t, err)
	require.True(t, stringVar(t, strings.TrimSpace(string(out)), versionVar), "%s.%s is not a string variable", versionPkg, versionVar)
}

// stringVar reports whether the package in dir declares name as a
// package-level variable initialised to a string literal, which is what
// the linker's -X sets.
func stringVar(t *testing.T, dir, name string) bool {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				v := spec.(*ast.ValueSpec)
				for i, n := range v.Names {
					if n.Name != name || i >= len(v.Values) {
						continue
					}
					lit, ok := v.Values[i].(*ast.BasicLit)
					return ok && lit.Kind == token.STRING
				}
			}
		}
	}
	return false
}

// TestKo_BuildsSetVersion holds every ko build ci, release and the Justfile
// run to setting VERSION, which .ko.yaml's ldflags read.
func TestKo_BuildsSetVersion(t *testing.T) {
	files, err := filepath.Glob("../.github/workflows/*.yml")
	require.NoError(t, err)
	var builds int
	for _, f := range files {
		for job, j := range readWorkflow(t, filepath.Base(f)).Jobs {
			for _, s := range j.Steps {
				if strings.Contains(s.Run, "ko build") {
					builds++
					require.NotEmpty(t, s.Env["VERSION"], "%s job %s", f, job)
				}
			}
		}
	}
	require.NotZero(t, builds)
	require.Equal(t, "${{ needs.plan.outputs.version }}", stepByName(t, readWorkflow(t, "release.yml").Jobs["images"].Steps, "Build").Env["VERSION"])

	just, err := exec.LookPath("just")
	if err != nil {
		t.Skip("just is not on PATH")
	}
	out, err := exec.Command(just, "--justfile", "../Justfile", "--dump", "--dump-format", "json").Output()
	require.NoError(t, err)
	var dump struct {
		Recipes map[string]struct {
			Body [][]json.RawMessage `json:"body"`
		} `json:"recipes"`
	}
	require.NoError(t, json.Unmarshal(out, &dump))
	var koLines int
	for _, line := range dump.Recipes["image"].Body {
		var text strings.Builder
		for _, fragment := range line {
			var s string
			if json.Unmarshal(fragment, &s) == nil {
				text.WriteString(s)
			}
		}
		if strings.Contains(text.String(), "ko build") {
			koLines++
			require.Contains(t, text.String(), `VERSION="${VERSION:-dev}" `)
		}
	}
	require.Equal(t, 1, koLines)
}

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

// renovateVersion is a pinned Renovate release, as npx names its package.
var renovateVersion = regexp.MustCompile(`^renovate@\d+\.\d+\.\d+$`)

// TestCI_ValidatesRenovateConfig holds ci to running Renovate's own validator,
// strict and at a pinned release, over .github/renovate.json as a repository
// config.
func TestCI_ValidatesRenovateConfig(t *testing.T) {
	var runs int
	for job, j := range readWorkflow(t, "ci.yml").Jobs {
		for _, s := range j.Steps {
			args := strings.Fields(s.Run)
			if !slices.Contains(args, "renovate-config-validator") {
				continue
			}
			runs++
			require.Equal(t, []string{"npx", "--yes", "--package"}, args[:3], job)
			require.Regexp(t, renovateVersion, args[3], job)
			require.Equal(t, []string{"--", "renovate-config-validator", "--strict", "--no-global", ".github/renovate.json"}, args[4:], job)
			var node bool
			for _, prior := range j.Steps {
				node = node || strings.HasPrefix(prior.Uses, "actions/setup-node@")
			}
			require.True(t, node, "job %s runs npx without setup-node", job)
		}
	}
	require.Equal(t, 1, runs)
}
