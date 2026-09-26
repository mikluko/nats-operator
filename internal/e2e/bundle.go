// Package e2e runs the story bundles under docs/content/stories against a
// live Kubernetes cluster: step by step, it applies each step's manifests,
// deletes what the step deletes, and waits for the live objects to contain
// every status and live file of the step.
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

// Bundle is one story directory: its steps, and how the harness treats it
// as declared in its index.md front matter under params.e2e.
type Bundle struct {
	// Name is the directory name, such as "01-quickstart".
	Name string
	// Number is the story number the directory name starts with.
	Number int
	// After is the number of the story whose end state this one starts
	// from, or 0.
	After int
	// Base is the bundle After names; nil when After is 0.
	Base *Bundle
	// Skip is why the harness does not run the story, or "".
	Skip string
	// Steps are ordered by step number.
	Steps []Step
}

// Step is every file of a bundle that carries one step number: the
// manifests to apply, the objects to delete, and what to wait for after.
type Step struct {
	Number int
	// Apply and Delete are in file name order, then document order.
	Apply        []*unstructured.Unstructured
	Delete       []*unstructured.Unstructured
	Expectations []Expectation
}

// Role is what a bundle file is for.
type Role int

// The roles a file name can give a file.
const (
	// RoleApply is a manifest to apply: NN-<anything>.yaml.
	RoleApply Role = iota
	// RoleDelete names objects to delete: NN-delete-<anything>.yaml.
	RoleDelete
	// RoleStatus is the status block the object must reach:
	// NN-status-<kind>[-<qualifier>].yaml.
	RoleStatus
	// RoleLive is the object as kubectl get -o yaml shows it, spec
	// included: NN-live-<kind>[-<qualifier>].yaml.
	RoleLive
)

// FileName is what a bundle file's name says about it.
type FileName struct {
	Step int
	Role Role
	// Kind is the lower-cased kind a status or live file names.
	Kind string
	// Qualifier is what follows the kind, or "".
	Qualifier string
}

// ParseFileName reads a bundle YAML file's base name. Every such name
// starts with a step number and a dash.
func ParseFileName(base string) (FileName, error) {
	prefix, rest, _ := strings.Cut(strings.TrimSuffix(base, ".yaml"), "-")
	step, err := strconv.Atoi(prefix)
	if err != nil || rest == "" {
		return FileName{}, fmt.Errorf("%s: a bundle file name starts with its step number, as in 01-%s", base, base)
	}
	f := FileName{Step: step}
	role, named, _ := strings.Cut(rest, "-")
	switch role {
	case "status":
		f.Role = RoleStatus
	case "live":
		f.Role = RoleLive
	case "delete":
		f.Role = RoleDelete
		return f, nil
	default:
		return f, nil
	}
	if named == "" {
		return FileName{}, fmt.Errorf("%s: names no kind", base)
	}
	f.Kind, f.Qualifier, _ = strings.Cut(named, "-")
	return f, nil
}

// Expectation is one status or live file: the object it names must come to
// contain Want.
type Expectation struct {
	// File is the file's base name.
	File string
	FileName
	// Want is the file's document; a status file's is its status block
	// alone. Values the file tags PlaceholderTag match anything.
	Want map[string]any
}

// LoadBundles reads every story directory under dir, ordered by story number.
// A directory whose name does not start with a number is not a story.
func LoadBundles(dir string) ([]*Bundle, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read stories: %w", err)
	}
	var bundles []*Bundle
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
	slices.SortFunc(bundles, func(a, b *Bundle) int { return a.Number - b.Number })
	for _, b := range bundles {
		if b.After == 0 {
			continue
		}
		i := slices.IndexFunc(bundles, func(o *Bundle) bool { return o.Number == b.After })
		if i < 0 || b.After >= b.Number {
			return nil, fmt.Errorf("%s: after names story %d, which is not an earlier story", b.Name, b.After)
		}
		b.Base = bundles[i]
	}
	return bundles, nil
}

func loadBundle(dir string) (*Bundle, error) {
	b := &Bundle{}
	if err := readFrontMatter(filepath.Join(dir, "index.md"), b); err != nil {
		return nil, err
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	slices.Sort(files)
	for _, path := range files {
		base := filepath.Base(path)
		name, err := ParseFileName(base)
		if err != nil {
			return nil, err
		}
		step := b.step(name.Step)
		switch name.Role {
		case RoleStatus, RoleLive:
			exp, err := loadExpectation(path, name)
			if err != nil {
				return nil, err
			}
			step.Expectations = append(step.Expectations, exp)
		case RoleDelete:
			objs, err := loadObjects(path)
			if err != nil {
				return nil, err
			}
			step.Delete = append(step.Delete, objs...)
		default:
			objs, err := loadObjects(path)
			if err != nil {
				return nil, err
			}
			step.Apply = append(step.Apply, objs...)
		}
	}
	slices.SortFunc(b.Steps, func(x, y Step) int { return x.Number - y.Number })
	return b, nil
}

func (b *Bundle) step(n int) *Step {
	for i := range b.Steps {
		if b.Steps[i].Number == n {
			return &b.Steps[i]
		}
	}
	b.Steps = append(b.Steps, Step{Number: n})
	return &b.Steps[len(b.Steps)-1]
}

// readFrontMatter sets b's After and Skip from the YAML front matter
// opening path, if path exists and has any.
func readFrontMatter(path string, b *Bundle) error {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	const fence = "---"
	s := bufio.NewScanner(bytes.NewReader(raw))
	if !s.Scan() || s.Text() != fence {
		return nil
	}
	var block bytes.Buffer
	for s.Scan() {
		if s.Text() == fence {
			var fm struct {
				Params struct {
					E2E struct {
						After int    `json:"after"`
						Skip  string `json:"skip"`
					} `json:"e2e"`
				} `json:"params"`
			}
			if err := yaml.Unmarshal(block.Bytes(), &fm); err != nil {
				return fmt.Errorf("%s: front matter: %w", path, err)
			}
			b.After, b.Skip = fm.Params.E2E.After, fm.Params.E2E.Skip
			return nil
		}
		block.Write(s.Bytes())
		block.WriteByte('\n')
	}
	if err := s.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%s: front matter is not closed", path)
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

func loadExpectation(path string, name FileName) (Expectation, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Expectation{}, err
	}
	doc, err := decodeExpected(raw)
	if err != nil {
		return Expectation{}, fmt.Errorf("%s: %w", path, err)
	}
	base := filepath.Base(path)
	e := Expectation{File: base, FileName: name, Want: doc}
	if name.Role == RoleStatus {
		status, ok := doc["status"].(map[string]any)
		if !ok {
			return Expectation{}, fmt.Errorf("%s: no status block", path)
		}
		e.Want = map[string]any{"status": status}
	}
	if len(e.Want) == 0 {
		return Expectation{}, fmt.Errorf("%s: empty", path)
	}
	return e, nil
}

// Objects returns every object b's steps up to and including step apply or
// delete, once each, as last declared.
func (b *Bundle) Objects(step int) []*unstructured.Unstructured {
	var objs []*unstructured.Unstructured
	for _, s := range b.Steps {
		if s.Number > step {
			break
		}
		for _, o := range slices.Concat(s.Apply, s.Delete) {
			objs = upsert(objs, o)
		}
	}
	return objs
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

// Target returns the object whose state e, a file of step, describes: among
// the objects of b's steps up to step, the only one of e's kind, or else the
// one of that kind named e's qualifier. It fails when neither rule picks
// exactly one object.
func (b *Bundle) Target(step int, e Expectation) (*unstructured.Unstructured, error) {
	var ofKind []*unstructured.Unstructured
	for _, o := range b.Objects(step) {
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

// Chain returns the bundles b starts from, oldest first, then b.
func (b *Bundle) Chain() []*Bundle {
	if b.Base == nil {
		return []*Bundle{b}
	}
	return append(b.Base.Chain(), b)
}

// Namespaces returns the namespaces the objects of b and every bundle it
// starts from are declared in, sorted.
func (b *Bundle) Namespaces() []string {
	var ns []string
	for _, c := range b.Chain() {
		for _, s := range c.Steps {
			for _, o := range s.Apply {
				if n := o.GetNamespace(); n != "" && !slices.Contains(ns, n) {
					ns = append(ns, n)
				}
			}
		}
	}
	slices.Sort(ns)
	return ns
}

// SkipReason returns why b is not run: its own Skip, or that of a bundle
// it starts from. It returns "" when b runs.
func (b *Bundle) SkipReason() string {
	if b.Skip != "" {
		return b.Skip
	}
	if b.Base == nil {
		return ""
	}
	if r := b.Base.SkipReason(); r != "" {
		return fmt.Sprintf("starts from %s, which is skipped: %s", b.Base.Name, r)
	}
	return ""
}
