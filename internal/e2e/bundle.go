// Package e2e runs the story bundles under docs/content/docs/stories against
// live Kubernetes clusters. It is not Kyverno Chainsaw because Chainsaw
// cannot fail a step on a terminal signal, a CrashLoopBackOff or a Terminal
// condition, before the step's timeout.
package e2e

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

// FixtureDir is the directory in a story's bundle holding files the harness
// loads as the story's own and the story's page does not show; placements
// name them by their path from the bundle, as in "e2e/00-cluster.yaml".
const FixtureDir = "e2e"

// Bundle is one story directory: its steps, and how the harness treats it
// as declared in its index.md front matter under params.e2e.
type Bundle struct {
	Name   string
	Number int
	// After is the number of the story whose end state this one starts
	// from, or 0.
	After int
	// Base is the bundle After names; nil when After is 0.
	Base *Bundle
	Skip string
	// Clusters places the bundle's files in Kubernetes clusters, the home
	// cluster first; empty means the story runs in one Kubernetes cluster.
	Clusters []Placement
	// Substitutions are merged into the bundle's files as they load.
	Substitutions []Substitution
	// Waits are the steps that wait longer than the run's default for
	// their expectations, by step number.
	Waits map[int]StepWait
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

// StepWait is how long one step of a bundle waits for its expectations.
type StepWait struct {
	Step int           `json:"step"`
	Wait time.Duration `json:"-"`
	// Reason says what makes the step slow; it is required.
	Reason string `json:"reason"`
}

// UnmarshalJSON reads a StepWait whose wait is a Go duration string.
func (w *StepWait) UnmarshalJSON(b []byte) error {
	var raw struct {
		Step   int    `json:"step"`
		Wait   string `json:"wait"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	d, err := time.ParseDuration(raw.Wait)
	if err != nil {
		return fmt.Errorf("wait of step %d: %w", raw.Step, err)
	}
	*w = StepWait{Step: raw.Step, Wait: d, Reason: raw.Reason}
	return nil
}

// selects reports whether s reaches o, a manifest of a file s names.
func (s Substitution) selects(o *unstructured.Unstructured) bool {
	return (s.Kind == "" || o.GetKind() == s.Kind) && (s.Name == "" || o.GetName() == s.Name)
}

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
	if err := b.checkWaits(dir); err != nil {
		return nil, err
	}
	return b, checkPlacement(dir, files, b.Clusters)
}

// checkWaits fails where a wait names no step of b or lacks a reason or a
// positive wait.
func (b *Bundle) checkWaits(dir string) error {
	for n, w := range b.Waits {
		if w.Reason == "" || w.Wait <= 0 {
			return fmt.Errorf("%s: the wait of step %d needs a positive wait and a reason", dir, n)
		}
		if !slices.ContainsFunc(b.Steps, func(s Step) bool { return s.Number == n }) {
			return fmt.Errorf("%s: a wait names step %d, which has no files", dir, n)
		}
	}
	return nil
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

// mergePatch returns target with patch merged in per RFC 7386.
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

func (b *Bundle) add(f bundleFile) {
	b.files = append(b.files, f)
	b.Steps = addFile(b.Steps, f)
}

// addFile returns steps with f filed under its step, ordered by step number.
func addFile(steps []Step, f bundleFile) []Step {
	i := slices.IndexFunc(steps, func(s Step) bool { return s.Number == f.name.Step })
	if i < 0 {
		steps = append(steps, Step{Number: f.name.Step})
		i = len(steps) - 1
	}
	step := &steps[i]
	switch f.name.Role {
	case RoleStatus, RoleLive:
		step.Expectations = append(step.Expectations, f.exp)
	case RoleDelete:
		step.Delete = append(step.Delete, f.objs...)
	default:
		step.Apply = append(step.Apply, f.objs...)
	}
	slices.SortFunc(steps, func(x, y Step) int { return x.Number - y.Number })
	return steps
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

// Part is what one bundle places in one Kubernetes cluster.
type Part struct {
	// Cluster is the story's name for the Kubernetes cluster, or "" for a
	// bundle that places no files.
	Cluster string
	// Steps hold the files placed there, ordered by step number.
	Steps []Step
}

// Parts returns one part per Kubernetes cluster b is placed in, in the
// order of Clusters; a bundle with no placement has one part, holding all
// its steps.
func (b *Bundle) Parts() []Part {
	if len(b.Clusters) == 0 {
		return []Part{{Steps: b.Steps}}
	}
	parts := make([]Part, 0, len(b.Clusters))
	for _, p := range b.Clusters {
		part := Part{Cluster: p.Name}
		for _, f := range b.files {
			if slices.Contains(p.Files, f.base) {
				part.Steps = addFile(part.Steps, f)
			}
		}
		parts = append(parts, part)
	}
	return parts
}

// Objects returns the objects p's steps up to step apply or delete, each as
// last declared.
func (p Part) Objects(step int) []*unstructured.Unstructured {
	return objects(p.Steps, step)
}

// Target is Bundle.Target over p's steps alone.
func (p Part) Target(step int, e Expectation) (*unstructured.Unstructured, error) {
	return target(p.Steps, step, e)
}

// readFrontMatter sets b's After, Skip, Clusters, Substitutions and Waits
// from the YAML front matter opening path, if path exists and has any.
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
						Waits         []StepWait     `json:"waits"`
					} `json:"e2e"`
				} `json:"params"`
			}
			if err := yaml.Unmarshal(block.Bytes(), &fm); err != nil {
				return fmt.Errorf("%s: front matter: %w", path, err)
			}
			e := fm.Params.E2E
			b.After, b.Skip, b.Clusters, b.Substitutions = e.After, e.Skip, e.Clusters, e.Substitutions
			for _, w := range e.Waits {
				if b.Waits == nil {
					b.Waits = map[int]StepWait{}
				}
				if _, dup := b.Waits[w.Step]; dup {
					return fmt.Errorf("%s: front matter: two waits for step %d", path, w.Step)
				}
				b.Waits[w.Step] = w
			}
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
	objs, err := DecodeObjects(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return objs, nil
}

// DecodeObjects returns the objects of the YAML documents in raw, skipping
// empty ones.
func DecodeObjects(raw []byte) ([]*unstructured.Unstructured, error) {
	r := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
	var objs []*unstructured.Unstructured
	for {
		doc, err := r.Read()
		if errors.Is(err, io.EOF) {
			return objs, nil
		}
		if err != nil {
			return nil, err
		}
		var m map[string]any
		if err := yaml.Unmarshal(doc, &m); err != nil {
			return nil, err
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

// Objects returns the objects b's steps up to step apply or delete, each as
// last declared.
func (b *Bundle) Objects(step int) []*unstructured.Unstructured {
	return objects(b.Steps, step)
}

func objects(steps []Step, step int) []*unstructured.Unstructured {
	var objs []*unstructured.Unstructured
	for _, s := range steps {
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
	return target(b.Steps, step, e)
}

func target(steps []Step, step int, e Expectation) (*unstructured.Unstructured, error) {
	var ofKind []*unstructured.Unstructured
	for _, o := range objects(steps, step) {
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
		ns = namespaces(ns, c.Steps)
	}
	return ns
}

func namespaces(ns []string, steps []Step) []string {
	for _, s := range steps {
		for _, o := range s.Apply {
			if n := o.GetNamespace(); n != "" && !slices.Contains(ns, n) {
				ns = append(ns, n)
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
