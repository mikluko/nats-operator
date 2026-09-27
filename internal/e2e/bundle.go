// Package e2e runs the story bundles under docs/content/docs/stories against
// live Kubernetes clusters: step by step, it applies each step's manifests,
// deletes what the step deletes, and waits for the live objects to contain
// every status and live file of the step, each file in the cluster its
// story places it in.
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

// FixtureDir is the directory in a story's bundle holding files the harness
// loads as the story's own and the story's page does not show: what a
// story assumes exists before its first step. They are named as the
// story's files are, and placed by their path from the bundle, as in
// "e2e/00-cluster.yaml".
const FixtureDir = "e2e"

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
	// Clusters places the bundle's files in Kubernetes clusters, the home
	// cluster first; empty means the story runs in one Kubernetes cluster.
	Clusters []Placement
	// Substitutions are merged into the bundle's files as they load.
	Substitutions []Substitution
	// Steps are ordered by step number.
	Steps []Step

	files []bundleFile
}

// Placement names the files of a bundle that are applied to, deleted from,
// and whose expectations are read from, one Kubernetes cluster.
type Placement struct {
	// Name is the story's name for the Kubernetes cluster.
	Name  string   `json:"name"`
	Files []string `json:"files"`
}

// Substitution is a change the harness makes to files of a bundle, where
// what the story shows the reader cannot run as written on the harness's
// Kubernetes clusters.
type Substitution struct {
	Files []string `json:"files"`
	// Kind and Name, where set, narrow the substitution to the manifests
	// of that kind and name; either set, it reaches no status or live file.
	Kind string `json:"kind,omitempty"`
	Name string `json:"name,omitempty"`
	// Reason says what the harness lacks; it is required.
	Reason string `json:"reason"`
	// Patch is a JSON merge patch (RFC 7386) applied to every document of
	// each file it reaches; a status file's document is its status block
	// alone, under the status key.
	Patch map[string]any `json:"patch"`
}

// selects reports whether s reaches o, a manifest of a file s names.
func (s Substitution) selects(o *unstructured.Unstructured) bool {
	return (s.Kind == "" || o.GetKind() == s.Kind) && (s.Name == "" || o.GetName() == s.Name)
}

// bundleFile is one loaded bundle file.
type bundleFile struct {
	base string
	name FileName
	objs []*unstructured.Unstructured
	exp  Expectation
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
	fixtures, err := filepath.Glob(filepath.Join(dir, FixtureDir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	files = append(files, fixtures...)
	slices.Sort(files)
	for _, path := range files {
		f := bundleFile{base: filepath.Base(path)}
		if filepath.Base(filepath.Dir(path)) == FixtureDir {
			f.base = FixtureDir + "/" + f.base
		}
		if f.name, err = ParseFileName(filepath.Base(path)); err != nil {
			return nil, err
		}
		switch f.name.Role {
		case RoleStatus, RoleLive:
			f.exp, err = loadExpectation(path, f.name)
		default:
			f.objs, err = loadObjects(path)
		}
		if err != nil {
			return nil, err
		}
		f.substitute(b.Substitutions)
		b.add(f)
	}
	if err := checkSubstitutions(dir, files, b.Substitutions); err != nil {
		return nil, err
	}
	return b, checkPlacement(dir, files, b.Clusters)
}

// substitute applies to f every substitution that names it, in order.
func (f *bundleFile) substitute(subs []Substitution) {
	for _, s := range subs {
		if !slices.Contains(s.Files, f.base) {
			continue
		}
		for _, o := range f.objs {
			if s.selects(o) {
				o.Object = mergePatch(o.Object, s.Patch)
			}
		}
		if f.exp.Want != nil && s.Kind == "" && s.Name == "" {
			f.exp.Want = mergePatch(f.exp.Want, s.Patch)
		}
	}
}

// mergePatch returns target with patch merged in per RFC 7386: a null in
// patch deletes the key, a map merges into a map, anything else replaces.
func mergePatch(target, patch map[string]any) map[string]any {
	if target == nil {
		target = map[string]any{}
	}
	for k, pv := range patch {
		switch pv := pv.(type) {
		case nil:
			delete(target, k)
		case map[string]any:
			tv, _ := target[k].(map[string]any)
			target[k] = mergePatch(tv, pv)
		default:
			target[k] = pv
		}
	}
	return target
}

// checkSubstitutions fails when a substitution gives no reason, patches
// nothing, or names a file the bundle lacks.
func checkSubstitutions(dir string, files []string, subs []Substitution) error {
	for i, s := range subs {
		if s.Reason == "" || len(s.Patch) == 0 || len(s.Files) == 0 {
			return fmt.Errorf("%s: substitution %d needs files, a reason and a patch", dir, i)
		}
		for _, f := range s.Files {
			if !slices.Contains(files, filepath.Join(dir, f)) {
				return fmt.Errorf("%s: substitution %d names %s, which the bundle does not have", dir, i, f)
			}
		}
	}
	return nil
}

// add files f under its step, keeping the steps ordered.
func (b *Bundle) add(f bundleFile) {
	b.files = append(b.files, f)
	step := b.step(f.name.Step)
	switch f.name.Role {
	case RoleStatus, RoleLive:
		step.Expectations = append(step.Expectations, f.exp)
	case RoleDelete:
		step.Delete = append(step.Delete, f.objs...)
	default:
		step.Apply = append(step.Apply, f.objs...)
	}
	slices.SortFunc(b.Steps, func(x, y Step) int { return x.Number - y.Number })
}

// checkPlacement fails when a placement names a file the bundle lacks, or
// leaves one of its files in no Kubernetes cluster.
func checkPlacement(dir string, files []string, clusters []Placement) error {
	if len(clusters) == 0 {
		return nil
	}
	placed := map[string]bool{}
	for _, p := range clusters {
		for _, f := range p.Files {
			if !slices.Contains(files, filepath.Join(dir, f)) {
				return fmt.Errorf("%s: cluster %s places %s, which the bundle does not have", dir, p.Name, f)
			}
			placed[f] = true
		}
	}
	for _, path := range files {
		if rel, _ := filepath.Rel(dir, path); !placed[filepath.ToSlash(rel)] {
			return fmt.Errorf("%s: in none of the Kubernetes clusters index.md places files in", path)
		}
	}
	return nil
}

// Parts returns one bundle per Kubernetes cluster b is placed in, in the
// order of Clusters, each holding the steps of its placement's files; a
// bundle with no placement is its own only part. A part's Clusters holds its
// own placement alone, and a part starts from no other bundle.
func (b *Bundle) Parts() []*Bundle {
	if len(b.Clusters) == 0 {
		return []*Bundle{b}
	}
	parts := make([]*Bundle, 0, len(b.Clusters))
	for _, p := range b.Clusters {
		part := &Bundle{Name: b.Name, Number: b.Number, Skip: b.Skip, Clusters: []Placement{p}}
		for _, f := range b.files {
			if slices.Contains(p.Files, f.base) {
				part.add(f)
			}
		}
		parts = append(parts, part)
	}
	return parts
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

// readFrontMatter sets b's After, Skip, Clusters and Substitutions from the YAML front matter
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
						After         int            `json:"after"`
						Skip          string         `json:"skip"`
						Clusters      []Placement    `json:"clusters"`
						Substitutions []Substitution `json:"substitutions"`
					} `json:"e2e"`
				} `json:"params"`
			}
			if err := yaml.Unmarshal(block.Bytes(), &fm); err != nil {
				return fmt.Errorf("%s: front matter: %w", path, err)
			}
			e := fm.Params.E2E
			b.After, b.Skip, b.Clusters, b.Substitutions = e.After, e.Skip, e.Clusters, e.Substitutions
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
