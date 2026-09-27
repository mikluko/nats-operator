// Command rbac writes each controller's ClusterRole to
// config/rbac/<controller>/role.yaml with controller-gen, from the
// +kubebuilder:rbac markers of the packages that controller owns.
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
)

// module is this repository's module path.
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
	}
	return nil
}

// ownedPackages returns, by controller, the import paths whose markers make
// its ClusterRole: its command under cmd/, and every package of this module
// that command imports and no other controller's does. A package two
// controllers import carries no markers; each command declares what it needs
// of it.
func ownedPackages(dir string) (map[string][]string, error) {
	pkgs, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps,
		Dir:  dir,
	}, "./cmd/...")
	if err != nil {
		return nil, err
	}
	deps := map[string][]string{}
	importers := map[string]int{}
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
				importers[d.PkgPath]++
			}
		})
		deps[c] = append(deps[c], p.PkgPath)
	}
	if len(deps) == 0 {
		return nil, fmt.Errorf("no controller under %s/cmd", dir)
	}
	owned := map[string][]string{}
	for c, ds := range deps {
		for _, d := range ds {
			if importers[d] <= 1 {
				owned[c] = append(owned[c], d)
			}
		}
		slices.Sort(owned[c])
	}
	return owned, nil
}
