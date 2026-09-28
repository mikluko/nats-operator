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
	If   string            `json:"if"`
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
		If      string            `json:"if"`
		Needs   needs             `json:"needs"`
		Outputs map[string]string `json:"outputs"`
		Env     map[string]string `json:"env"`
		Steps   []step            `json:"steps"`
		Uses    string            `json:"uses"`
		With    map[string]string `json:"with"`
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

// TestRelease_FailOnAndCIName pins the plan job's changelog step to failing
// on an invalid CHANGELOG.md, and release.yml's trigger to ci.yml's name.
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

// TestRelease_PublishesWhatTheChartPulls holds the release's images, one per
// controller under cmd/, to the chart's image repositories, and the chart's
// destination to their registry.
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
		if s.Env["DIGESTS"] == "${{ needs.images.outputs.digests }}" {
			pin = i
			require.Empty(t, s.If)
			require.Contains(t, s.Run, "yq -i")
		}
		if strings.Contains(s.Run, "helm package") {
			pkg = i
		}
	}
	require.NotEqual(t, -1, pin, "no chart step reads the images job's digests")
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

// TestHugoInstalledOnce holds the workflows that build the site to the
// setup-hugo action's pinned and checksummed Hugo.
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
// the build provenance of the images and the chart.
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
		"golang.org/x/vuln",
		"github.com/rhysd/actionlint",
	} {
		require.NotZero(t, found[dep], dep)
	}
}
