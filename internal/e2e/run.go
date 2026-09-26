package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Outcome is how one story ended.
type Outcome string

// The outcomes a story can have.
const (
	Pass Outcome = "PASS"
	Fail Outcome = "FAIL"
	Skip Outcome = "SKIP"
)

// Result is one story's outcome; Detail is the skip reason, the error, or
// the status diff at the deadline.
type Result struct {
	Story   string
	Outcome Outcome
	Elapsed time.Duration
	Detail  string
}

// Runner runs story bundles through Client.
type Runner struct {
	Client client.Client
	// Timeout bounds the wait for each step's expectations, and separately
	// the wait for a previous story's namespace to finish deleting.
	Timeout  time.Duration
	Interval time.Duration
	// Log receives one line per phase of each story.
	Log io.Writer
}

const fieldOwner = "nats-operator-e2e"

// Run runs one bundle, after every bundle it starts from: it deletes and
// recreates every namespace their objects are declared in, then, bundle by
// bundle and step by step, server-side applies the step's manifests, deletes
// the objects it deletes, and polls until each of its expectations' target
// objects contains it or Timeout passes. An expectation that names no object
// fails the story before anything is applied, and a bundle whose
// SkipReason is not "" is skipped.
func (r *Runner) Run(ctx context.Context, b *Bundle) Result {
	start := time.Now()
	res := func(o Outcome, detail string) Result {
		return Result{Story: b.Name, Outcome: o, Elapsed: time.Since(start).Round(time.Second), Detail: detail}
	}
	if reason := b.SkipReason(); reason != "" {
		r.logf("%s: skipped: %s", b.Name, reason)
		return res(Skip, reason)
	}
	type stage struct {
		bundle  *Bundle
		step    Step
		targets []*unstructured.Unstructured
	}
	var stages []stage
	for _, c := range b.Chain() {
		for _, s := range c.Steps {
			st := stage{bundle: c, step: s}
			for _, e := range s.Expectations {
				t, err := c.Target(s.Number, e)
				if err != nil {
					return res(Fail, fmt.Sprintf("%s: %v", c.Name, err))
				}
				st.targets = append(st.targets, t)
			}
			stages = append(stages, st)
		}
	}
	for _, ns := range b.Namespaces() {
		r.logf("%s: fresh namespace %s", b.Name, ns)
		if err := r.freshNamespace(ctx, ns); err != nil {
			return res(Fail, err.Error())
		}
	}
	for _, st := range stages {
		at := fmt.Sprintf("%s step %d", st.bundle.Name, st.step.Number)
		for _, o := range st.step.Apply {
			r.logf("%s: apply %s %s", at, o.GetKind(), key(o))
			if err := r.Client.Apply(ctx, client.ApplyConfigurationFromUnstructured(o.DeepCopy()), client.FieldOwner(fieldOwner), client.ForceOwnership); err != nil {
				return res(Fail, fmt.Sprintf("%s: apply %s %s: %v", at, o.GetKind(), key(o), err))
			}
		}
		for _, o := range st.step.Delete {
			r.logf("%s: delete %s %s", at, o.GetKind(), key(o))
			if err := r.Client.Delete(ctx, o.DeepCopy()); client.IgnoreNotFound(err) != nil {
				return res(Fail, fmt.Sprintf("%s: delete %s %s: %v", at, o.GetKind(), key(o), err))
			}
		}
		if len(st.step.Expectations) == 0 {
			continue
		}
		r.logf("%s: polling %d files, timeout %s", at, len(st.step.Expectations), r.Timeout)
		diff, err := r.poll(ctx, st.step.Expectations, st.targets)
		if err != nil {
			return res(Fail, fmt.Sprintf("%s: %v", at, err))
		}
		if diff != "" {
			return res(Fail, fmt.Sprintf("%s: timed out after %s\n%s", at, r.Timeout, diff))
		}
	}
	return res(Pass, "")
}

// poll returns "" once every expectation holds, or the diff of the last
// check when Timeout passes.
func (r *Runner) poll(ctx context.Context, exps []Expectation, targets []*unstructured.Unstructured) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	tick := time.NewTicker(r.Interval)
	defer tick.Stop()
	var diff string
	for {
		d, err := r.check(ctx, exps, targets)
		switch {
		case err == nil && d == "":
			return "", nil
		case err == nil:
			diff = d
		case ctx.Err() == nil:
			return "", err
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return diff, nil
			}
			return "", ctx.Err()
		case <-tick.C:
		}
	}
}

// check reads every target once and renders the expectations it fails; a
// target that does not exist yet fails its expectation rather than the check.
func (r *Runner) check(ctx context.Context, exps []Expectation, targets []*unstructured.Unstructured) (string, error) {
	var b strings.Builder
	for i, e := range exps {
		t := targets[i]
		live := &unstructured.Unstructured{}
		live.SetGroupVersionKind(t.GroupVersionKind())
		head := fmt.Sprintf("  %s -> %s %s\n", e.File, t.GetKind(), key(t))
		err := r.Client.Get(ctx, client.ObjectKeyFromObject(t), live)
		switch {
		case apierrors.IsNotFound(err):
			b.WriteString(head + "    object not found\n")
			continue
		case err != nil:
			return "", fmt.Errorf("get %s %s: %w", t.GetKind(), key(t), err)
		}
		if ms := Diff(e.Want, live.Object); len(ms) > 0 {
			b.WriteString(head + FormatMismatches(ms, "    "))
		}
	}
	return b.String(), nil
}

func (r *Runner) freshNamespace(ctx context.Context, name string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := r.Client.Delete(ctx, ns)
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return fmt.Errorf("delete namespace %s: %w", name, err)
	default:
		if err := r.awaitGone(ctx, ns); err != nil {
			return err
		}
	}
	if err := r.Client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		return fmt.Errorf("create namespace %s: %w", name, err)
	}
	return nil
}

func (r *Runner) awaitGone(ctx context.Context, ns *corev1.Namespace) error {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	tick := time.NewTicker(r.Interval)
	defer tick.Stop()
	for {
		err := r.Client.Get(ctx, client.ObjectKeyFromObject(ns), &corev1.Namespace{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("namespace %s still terminating after %s", ns.Name, r.Timeout)
		case <-tick.C:
		}
	}
}

func (r *Runner) logf(format string, args ...any) {
	if r.Log != nil {
		_, _ = fmt.Fprintf(r.Log, format+"\n", args...)
	}
}

func key(o client.Object) string {
	return o.GetNamespace() + "/" + o.GetName()
}

// WriteTable writes one row per result, then every failed story's detail.
func WriteTable(w io.Writer, results []Result) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "STORY\tRESULT\tTIME\tNOTE")
	for _, r := range results {
		note := ""
		switch r.Outcome {
		case Skip:
			note = r.Detail
		case Fail:
			note, _, _ = strings.Cut(r.Detail, "\n")
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.Story, r.Outcome, r.Elapsed, note)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, r := range results {
		if r.Outcome == Fail && strings.Contains(r.Detail, "\n") {
			if _, err := fmt.Fprintf(w, "\n%s: %s", r.Story, r.Detail); err != nil {
				return err
			}
		}
	}
	return nil
}
