package chart_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

const chartDir = "../../charts/nats-operator"

// controllers are the chart's three controllers by values key.
var controllers = []string{"cluster", "auth", "jetstream"}

// ownGroups is each controller's own API group.
var ownGroups = map[string]string{
	"cluster":   "cluster.nats.mikluko.io",
	"auth":      "auth.nats.mikluko.io",
	"jetstream": "jetstream.nats.mikluko.io",
}

// grants maps "group/resource" to the verbs granted on it.
type grants map[string][]string

var events = grants{
	"/events":              {"create", "patch"},
	"events.k8s.io/events": {"create", "patch"},
}

var (
	crud  = []string{"create", "delete", "get", "list", "patch", "update", "watch"}
	read  = []string{"get", "list", "watch"}
	owned = []string{"get", "list", "patch", "update", "watch"}
	stat  = []string{"get", "patch", "update"}
	final = []string{"update"}
)

// ownKinds grants a controller's own resources, their status and their
// finalizers.
func ownKinds(group string, resources ...string) grants {
	g := grants{}
	for _, r := range resources {
		g[group+"/"+r] = owned
		g[group+"/"+r+"/status"] = stat
		g[group+"/"+r+"/finalizers"] = final
	}
	return g
}

func merge(gs ...grants) grants {
	out := grants{}
	for _, g := range gs {
		for k, v := range g {
			out[k] = v
		}
	}
	return out
}

// wantRules is each controller's ClusterRole, exactly.
var wantRules = map[string]grants{
	"cluster": merge(
		ownKinds("cluster.nats.mikluko.io", "natsclusters"),
		grants{
			"nats.mikluko.io/natsoperatortrusts":  read,
			"nats.mikluko.io/natsaccounttrusts":   read,
			"nats.mikluko.io/natsconnections":     read,
			"nats.mikluko.io/natsreferencegrants": read,
			"apps/statefulsets":                   crud,
			"/configmaps":                         crud,
			"/services":                           crud,
			"policy/poddisruptionbudgets":         crud,
			"/secrets":                            crud,
			"/pods":                               read,
			"/persistentvolumeclaims":             {"delete", "get", "list", "watch"},
		},
		events,
	),
	"auth": merge(
		ownKinds("auth.nats.mikluko.io", "natsoperators", "natssystemaccounts", "natsaccounts", "natsusers"),
		grants{
			"nats.mikluko.io/natsoperatortrusts":        read,
			"nats.mikluko.io/natsaccounttrusts":         read,
			"nats.mikluko.io/natsconnections":           read,
			"nats.mikluko.io/natsreferencegrants":       read,
			"nats.mikluko.io/natsoperatortrusts/status": stat,
			"nats.mikluko.io/natsaccounttrusts/status":  stat,
			"/secrets": crud,
		},
		events,
	),
	"jetstream": merge(
		ownKinds("jetstream.nats.mikluko.io", "natsstreams", "natsconsumers", "natskeyvalues", "natsobjectstores",
			"natsbalancers", "natssystembalancers", "natsclusterevacuations"),
		grants{
			"nats.mikluko.io/natsconnections":     read,
			"nats.mikluko.io/natsreferencegrants": read,
			"/secrets":                            read,
		},
		events,
	),
}

// TestChart_Subsets pins that each subset of controllers renders exactly the
// enabled controllers, each with its own ServiceAccount, bound ClusterRole,
// leader election Role and Deployment.
func TestChart_Subsets(t *testing.T) {
	subsets := [][]string{{"cluster"}, {"auth"}, {"jetstream"}, {"cluster", "auth", "jetstream"}}
	for _, enabled := range subsets {
		t.Run(strings.Join(enabled, "+"), func(t *testing.T) {
			objs := render(t, enabledSets(enabled)...)
			for _, c := range controllers {
				name := "rel-" + c + "-controller"
				on := slices.Contains(enabled, c)
				for _, kind := range []string{"ServiceAccount", "ClusterRole", "ClusterRoleBinding", "Deployment"} {
					_, ok := objs[kind+"/"+name]
					require.Equal(t, on, ok, "%s/%s", kind, name)
				}
				_, ok := objs["Role/"+name+"-leader-election"]
				require.Equal(t, on, ok)
				if on {
					requireController(t, objs, c)
				}
			}
		})
	}
}

// requireController pins one enabled controller's objects.
func requireController(t *testing.T, objs map[string]*unstructured.Unstructured, c string) {
	t.Helper()
	name := "rel-" + c + "-controller"

	var cr rbacv1.ClusterRole
	convert(t, objs["ClusterRole/"+name], &cr)
	got := flatten(t, cr.Rules)
	require.Equal(t, wantRules[c], got)
	plurals := crdPlurals(t)
	for key := range got {
		group, resource, _ := strings.Cut(key, "/")
		for other, own := range ownGroups {
			require.False(t, other != c && group == own, "%s holds %s", c, key)
		}
		if strings.HasSuffix(group, "nats.mikluko.io") {
			base, _, _ := strings.Cut(resource, "/")
			require.True(t, plurals[group+"/"+base], "%s names no CRD", key)
		}
	}

	var crb rbacv1.ClusterRoleBinding
	convert(t, objs["ClusterRoleBinding/"+name], &crb)
	require.Equal(t, name, crb.RoleRef.Name)
	require.Equal(t, []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: "nats"}}, crb.Subjects)

	var role rbacv1.Role
	convert(t, objs["Role/"+name+"-leader-election"], &role)
	require.Equal(t, grants{"coordination.k8s.io/leases": crud}, flatten(t, role.Rules))

	var d appsv1.Deployment
	convert(t, objs["Deployment/"+name], &d)
	require.Equal(t, name, d.Spec.Template.Spec.ServiceAccountName)
	container := d.Spec.Template.Spec.Containers[0]
	require.Equal(t, "ghcr.io/mikluko/nats-operator/"+c+"-controller:"+appVersion(t), container.Image)
	require.Contains(t, container.Args, "--leader-elect=true")
	require.Contains(t, container.Args, "--leader-election-id="+ownGroups[c])
}

// TestChart_Values pins the image tag override and leader election off.
func TestChart_Values(t *testing.T) {
	objs := render(t, "cluster.image.tag=v9", "leaderElection.enabled=false")
	for _, c := range controllers {
		name := "rel-" + c + "-controller"
		_, ok := objs["Role/"+name+"-leader-election"]
		require.False(t, ok)
		var d appsv1.Deployment
		convert(t, objs["Deployment/"+name], &d)
		require.Contains(t, d.Spec.Template.Spec.Containers[0].Args, "--leader-elect=false")
	}
	var d appsv1.Deployment
	convert(t, objs["Deployment/rel-cluster-controller"], &d)
	require.Equal(t, "ghcr.io/mikluko/nats-operator/cluster-controller:v9", d.Spec.Template.Spec.Containers[0].Image)
}

func enabledSets(enabled []string) []string {
	sets := make([]string, 0, len(controllers))
	for _, c := range controllers {
		v := "false"
		if slices.Contains(enabled, c) {
			v = "true"
		}
		sets = append(sets, c+".enabled="+v)
	}
	return sets
}

// render runs helm template on the chart as release "rel" in namespace
// "nats" and returns its objects by "Kind/name".
func render(t *testing.T, sets ...string) map[string]*unstructured.Unstructured {
	t.Helper()
	out := renderRaw(t, sets...)
	objs := map[string]*unstructured.Unstructured{}
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(out), 4096)
	for {
		var m map[string]any
		err := dec.Decode(&m)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if m == nil {
			continue
		}
		u := &unstructured.Unstructured{Object: m}
		key := u.GetKind() + "/" + u.GetName()
		require.NotContains(t, objs, key)
		objs[key] = u
	}
	return objs
}

func renderRaw(t *testing.T, sets ...string) []byte {
	t.Helper()
	helm, err := exec.LookPath("helm")
	require.NoError(t, err, "the chart tests need helm on PATH")
	args := []string{"template", "rel", chartDir, "--namespace", "nats"}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	var stderr bytes.Buffer
	cmd := exec.CommandContext(t.Context(), helm, args...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoError(t, err, stderr.String())
	return out
}

func appVersion(t *testing.T) string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "helm", "show", "chart", chartDir).Output()
	require.NoError(t, err)
	var chart struct {
		AppVersion string `json:"appVersion"`
	}
	require.NoError(t, yaml.Unmarshal(out, &chart))
	require.NotEmpty(t, chart.AppVersion)
	return chart.AppVersion
}

// crdPlurals returns "group/plural" for every CRD in the chart.
func crdPlurals(t *testing.T) map[string]bool {
	t.Helper()
	files, err := filepath.Glob(chartDir + "/crds/*.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	out := map[string]bool{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		var crd apiextensionsv1.CustomResourceDefinition
		require.NoError(t, yaml.Unmarshal(b, &crd))
		out[crd.Spec.Group+"/"+crd.Spec.Names.Plural] = true
	}
	return out
}

func convert(t *testing.T, u *unstructured.Unstructured, into any) {
	t.Helper()
	require.NotNil(t, u)
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, into))
}

// flatten turns rules into grants, refusing a group/resource granted twice so
// that a rule cannot hide behind another.
func flatten(t *testing.T, rules []rbacv1.PolicyRule) grants {
	t.Helper()
	g := grants{}
	for _, r := range rules {
		require.Empty(t, r.ResourceNames)
		require.Empty(t, r.NonResourceURLs)
		verbs := slices.Sorted(slices.Values(r.Verbs))
		for _, group := range r.APIGroups {
			for _, res := range r.Resources {
				key := group + "/" + res
				require.NotContains(t, g, key)
				g[key] = verbs
			}
		}
	}
	return g
}
