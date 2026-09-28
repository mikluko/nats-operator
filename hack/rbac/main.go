// Command rbac writes each controller's ClusterRole to
// config/rbac/<controller>/role.yaml with controller-gen, from the
// +kubebuilder:rbac markers of its command and every internal/ package it
// imports, and splits its rules into cluster-scoped.yaml, a ClusterRole of
// the cluster-scoped resources in clusterScoped, and namespaced.yaml, a Role
// of every other.
package main

import (
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/tools/go/packages"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const module = "github.com/mikluko/nats-operator"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "rbac:", err)
		os.Exit(1)
	}
}

func run() error {
	owned, err := ownedPackages(".")
	if err != nil {
		return err
	}
	for _, c := range slices.Sorted(maps.Keys(owned)) {
		cmd := exec.Command("go", "tool", "controller-gen",
			"rbac:roleName="+c,
			"paths="+strings.Join(owned[c], ";"),
			"output:rbac:artifacts:config="+filepath.Join("config", "rbac", c))
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("%s: %w", c, err)
		}
		if err := split(filepath.Join("config", "rbac", c)); err != nil {
			return fmt.Errorf("%s: %w", c, err)
		}
	}
	return nil
}

// clusterScoped is every cluster-scoped "group/resource" a controller's
// ClusterRole may name; any other is namespaced.
var clusterScoped = map[string]bool{
	"authentication.k8s.io/tokenreviews":        true,
	"authorization.k8s.io/subjectaccessreviews": true,
}

// split writes the rules of dir/role.yaml over clusterScoped resources to
// dir/cluster-scoped.yaml and the rest to dir/namespaced.yaml, each under the
// role's name.
func split(dir string) error {
	b, err := os.ReadFile(filepath.Join(dir, "role.yaml"))
	if err != nil {
		return err
	}
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal(b, &role); err != nil {
		return fmt.Errorf("read role: %w", err)
	}
	cluster, namespaced := splitRules(role.Rules)
	for file, obj := range map[string]any{
		"cluster-scoped.yaml": &rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: role.Name},
			Rules:      cluster,
		},
		"namespaced.yaml": &rbacv1.Role{
			TypeMeta:   metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "Role"},
			ObjectMeta: metav1.ObjectMeta{Name: role.Name},
			Rules:      namespaced,
		},
	} {
		out, err := yaml.Marshal(obj)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, file), append([]byte("---\n"), out...), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// splitRules partitions rules by clusterScoped, splitting a rule that names
// both kinds of resource.
func splitRules(rules []rbacv1.PolicyRule) (cluster, namespaced []rbacv1.PolicyRule) {
	for _, r := range rules {
		var c, n []string
		for _, res := range r.Resources {
			scoped := false
			for _, g := range r.APIGroups {
				scoped = scoped || clusterScoped[g+"/"+res]
			}
			if scoped {
				c = append(c, res)
			} else {
				n = append(n, res)
			}
		}
		if len(c) > 0 {
			cr := *r.DeepCopy()
			cr.Resources = c
			cluster = append(cluster, cr)
		}
		if len(n) > 0 {
			nr := *r.DeepCopy()
			nr.Resources = n
			namespaced = append(namespaced, nr)
		}
	}
	return cluster, namespaced
}

// ownedPackages returns, by controller, the import paths whose markers make
// its ClusterRole: its command and the internal/ packages it imports. A
// marker in a package two controllers import grants both.
func ownedPackages(dir string) (map[string][]string, error) {
	pkgs, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps,
		Dir:  dir,
	}, "./cmd/...")
	if err != nil {
		return nil, err
	}
	deps := map[string][]string{}
	for _, p := range pkgs {
		c := path.Base(p.PkgPath)
		if p.Name != "main" || !strings.HasSuffix(c, "-controller") {
			continue
		}
		if len(p.Errors) > 0 {
			return nil, fmt.Errorf("%s: %w", p.PkgPath, p.Errors[0])
		}
		packages.Visit([]*packages.Package{p}, nil, func(d *packages.Package) {
			if d != p && strings.HasPrefix(d.PkgPath, module+"/internal/") {
				deps[c] = append(deps[c], d.PkgPath)
			}
		})
		deps[c] = append(deps[c], p.PkgPath)
	}
	if len(deps) == 0 {
		return nil, fmt.Errorf("no controller under %s/cmd", dir)
	}
	for c := range deps {
		slices.Sort(deps[c])
	}
	return deps, nil
}
