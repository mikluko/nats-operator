package hack_test

import (
	"encoding/json"
	"fmt"
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
// on main, or by hand, and never on a push of its own.
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

	plan := readWorkflow(t, "release.yml").Jobs["plan"]
	require.Contains(t, plan.If, "github.event.workflow_run.conclusion == 'success'")
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

// TestActionsPinnedBySHA holds every action a workflow or a local action
// uses, other than a local one, to a full commit SHA with the version it
// resolves from as its line comment.
func TestActionsPinnedBySHA(t *testing.T) {
	files, err := filepath.Glob("../.github/workflows/*.yml")
	require.NoError(t, err)
	actions, err := filepath.Glob("../.github/actions/*/action.yml")
	require.NoError(t, err)
	files = append(files, actions...)
	require.NotEmpty(t, files)

	pinned := regexp.MustCompile(`^[\w.-]+/[\w./-]+@[0-9a-f]{40}$`)
	version := regexp.MustCompile(`^# v\d+(\.\d+){0,2}$`)
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		var doc yamlv3.Node
		require.NoError(t, yamlv3.Unmarshal(b, &doc))
		for _, uses := range usesNodes(&doc) {
			if strings.HasPrefix(uses.Value, "./") {
				continue
			}
			where := fmt.Sprintf("%s:%d", filepath.Base(f), uses.Line)
			require.Regexp(t, pinned, uses.Value, where)
			require.Regexp(t, version, uses.LineComment, where)
		}
	}
}

// usesNodes returns the value of every "uses" key under n.
func usesNodes(n *yamlv3.Node) []*yamlv3.Node {
	var found []*yamlv3.Node
	if n.Kind == yamlv3.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			if n.Content[i].Value == "uses" {
				found = append(found, n.Content[i+1])
			}
		}
	}
	for _, c := range n.Content {
		found = append(found, usesNodes(c)...)
	}
	return found
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
