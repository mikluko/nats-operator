// Package e2e runs the story bundles under docs/content/stories against a
// live Kubernetes cluster: it applies each bundle's manifests and waits for
// the live objects' status to contain every status-*.yaml the bundle states.
package e2e

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

// Bundle is one story directory: the objects its manifests declare and the
// statuses its status-*.yaml files expect.
type Bundle struct {
	// Name is the directory name, such as "01-quickstart".
	Name string
	// Number is the story number the directory name starts with.
	Number int
	// Objects holds one entry per distinct kind, namespace and name, in the
	// order first declared; a later declaration of the same object replaces
	// an earlier one in place.
	Objects []*unstructured.Unstructured
	// Expectations are the bundle's status files, in file name order.
	Expectations []Expectation
}

// Expectation is one status-<kind>[-<qualifier>].yaml file.
type Expectation struct {
	// File is the file's base name.
	File string
	// Kind is the lower-cased kind the file name names.
	Kind string
	// Qualifier is what follows the kind in the file name, or "".
	Qualifier string
	// Status is the file's status block.
	Status map[string]any
}

// LoadBundles reads every story directory under dir, ordered by story number.
// A directory whose name does not start with a number is not a story.
func LoadBundles(dir string) ([]Bundle, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read stories: %w", err)
	}
	var bundles []Bundle
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		prefix, _, _ := strings.Cut(e.Name(), "-")
		n, err := strconv.Atoi(prefix)
		if err != nil {
			continue
		}
		b, err := loadBundle(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		b.Name, b.Number = e.Name(), n
		bundles = append(bundles, b)
	}
	slices.SortFunc(bundles, func(a, b Bundle) int { return a.Number - b.Number })
	return bundles, nil
}

func loadBundle(dir string) (Bundle, error) {
	var b Bundle
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return b, err
	}
	slices.Sort(files)
	for _, path := range files {
		base := filepath.Base(path)
		if strings.HasPrefix(base, "status-") {
			exp, err := loadExpectation(path)
			if err != nil {
				return b, err
			}
			b.Expectations = append(b.Expectations, exp)
			continue
		}
		objs, err := loadObjects(path)
		if err != nil {
			return b, err
		}
		for _, obj := range objs {
			b.Objects = upsert(b.Objects, obj)
		}
	}
	return b, nil
}

func upsert(objs []*unstructured.Unstructured, obj *unstructured.Unstructured) []*unstructured.Unstructured {
	for i, o := range objs {
		if o.GroupVersionKind() == obj.GroupVersionKind() && o.GetNamespace() == obj.GetNamespace() && o.GetName() == obj.GetName() {
			objs[i] = obj
			return objs
		}
	}
	return append(objs, obj)
}

func loadObjects(path string) ([]*unstructured.Unstructured, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	r := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
	var objs []*unstructured.Unstructured
	for {
		doc, err := r.Read()
		if errors.Is(err, io.EOF) {
			return objs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		var m map[string]any
		if err := yaml.Unmarshal(doc, &m); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if len(m) == 0 {
			continue
		}
		objs = append(objs, &unstructured.Unstructured{Object: m})
	}
}

func loadExpectation(path string) (Expectation, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Expectation{}, err
	}
	var doc struct {
		Status map[string]any `json:"status"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return Expectation{}, fmt.Errorf("%s: %w", path, err)
	}
	if doc.Status == nil {
		return Expectation{}, fmt.Errorf("%s: no status block", path)
	}
	base := filepath.Base(path)
	kind, qualifier, _ := strings.Cut(strings.TrimSuffix(strings.TrimPrefix(base, "status-"), ".yaml"), "-")
	return Expectation{File: base, Kind: kind, Qualifier: qualifier, Status: doc.Status}, nil
}

// Target returns the object in b whose status e describes: the only object of
// e's kind, or else the one of that kind named e's qualifier. It fails when
// neither rule picks exactly one object.
func (b Bundle) Target(e Expectation) (*unstructured.Unstructured, error) {
	var ofKind []*unstructured.Unstructured
	for _, o := range b.Objects {
		if strings.ToLower(o.GetKind()) == e.Kind {
			ofKind = append(ofKind, o)
		}
	}
	if len(ofKind) == 1 {
		return ofKind[0], nil
	}
	for _, o := range ofKind {
		if o.GetName() == e.Qualifier {
			return o, nil
		}
	}
	names := make([]string, 0, len(ofKind))
	for _, o := range ofKind {
		names = append(names, o.GetName())
	}
	return nil, fmt.Errorf("%s names no object of the bundle: %d objects of kind %q (%s), none named %q",
		e.File, len(ofKind), e.Kind, strings.Join(names, ", "), e.Qualifier)
}

// Namespaces returns the namespaces b's objects are declared in, sorted.
func (b Bundle) Namespaces() []string {
	var ns []string
	for _, o := range b.Objects {
		if n := o.GetNamespace(); n != "" && !slices.Contains(ns, n) {
			ns = append(ns, n)
		}
	}
	slices.Sort(ns)
	return ns
}
