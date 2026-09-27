package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
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

// Runner runs story bundles through Clients.
type Runner struct {
	// Clients reach one Kubernetes cluster each, the home cluster first; a
	// story's placements are taken by these in order.
	Clients []client.Client
	// Timeout bounds the wait for each step's expectations, and separately
	// the wait for a previous story's namespace to finish deleting.
	Timeout  time.Duration
	Interval time.Duration
	// Log receives one line per phase of each story.
	Log io.Writer
}

const fieldOwner = "nats-operator-e2e"

// share is one Kubernetes cluster's part of one step: the client reaching
// it, the step's files placed there, and the object each expectation reads.
type share struct {
	client  client.Client
	where   string
	step    Step
	targets []*unstructured.Unstructured
}

// stage is one step of one bundle across every Kubernetes cluster.
type stage struct {
	at     string
	shares []share
}

// Run runs one bundle, after every bundle it starts from. In each Kubernetes
// cluster the bundles place files in, the home cluster when they place none,
// it deletes and recreates every namespace the cluster's objects are
// declared in; then, bundle by bundle and step by step, it server-side
// applies the step's manifests and deletes the objects it deletes in their
// clusters, and polls until each of its expectations' target objects, read
// in the expectation's cluster, contains it or Timeout passes. An
// expectation that names no object of its cluster fails the story before
// anything is applied; a bundle whose SkipReason is not "", or that places
// files in more clusters than Clients reach, is skipped.
func (r *Runner) Run(ctx context.Context, b *Bundle) Result {
	start := time.Now()
	res := func(o Outcome, detail string) Result {
		return Result{Story: b.Name, Outcome: o, Elapsed: time.Since(start).Round(time.Second), Detail: detail}
	}
	if reason := b.SkipReason(); reason != "" {
		r.logf("%s: skipped: %s", b.Name, reason)
		return res(Skip, reason)
	}
	namespaces := make([][]string, len(r.Clients))
	wheres := make([]string, len(r.Clients))
	var stages []stage
	for _, c := range b.Chain() {
		parts := c.Parts()
		if len(parts) > len(r.Clients) {
			reason := fmt.Sprintf("needs %d Kubernetes clusters, the run has %d", len(parts), len(r.Clients))
			r.logf("%s: skipped: %s", b.Name, reason)
			return res(Skip, reason)
		}
		var numbers []int
		for i, p := range parts {
			if wheres[i] == "" {
				wheres[i] = where(p)
			}
			for _, ns := range p.Namespaces() {
				if !slices.Contains(namespaces[i], ns) {
					namespaces[i] = append(namespaces[i], ns)
				}
			}
			for _, s := range p.Steps {
				if !slices.Contains(numbers, s.Number) {
					numbers = append(numbers, s.Number)
				}
			}
		}
		slices.Sort(numbers)
		for _, n := range numbers {
			st := stage{at: fmt.Sprintf("%s step %d", c.Name, n)}
			for i, p := range parts {
				j := slices.IndexFunc(p.Steps, func(s Step) bool { return s.Number == n })
				if j < 0 {
					continue
				}
				sh := share{client: r.Clients[i], where: where(p), step: p.Steps[j]}
				for _, e := range sh.step.Expectations {
					t, err := p.Target(n, e)
					if err != nil {
						return res(Fail, fmt.Sprintf("%s: %v", c.Name+sh.where, err))
					}
					sh.targets = append(sh.targets, t)
				}
				st.shares = append(st.shares, sh)
			}
			stages = append(stages, st)
		}
	}
	for i, nss := range namespaces {
		for _, ns := range nss {
			if err := releaseGuards(ctx, r.Clients[i], ns); err != nil {
				return res(Fail, err.Error())
			}
		}
	}
	for i, nss := range namespaces {
		slices.Sort(nss)
		for _, ns := range nss {
			r.logf("%s: fresh namespace %s%s", b.Name, ns, wheres[i])
			if err := r.freshNamespace(ctx, r.Clients[i], ns); err != nil {
				return res(Fail, err.Error())
			}
		}
	}
	for _, st := range stages {
		n := 0
		for _, sh := range st.shares {
			for _, o := range sh.step.Apply {
				r.logf("%s: apply %s %s%s", st.at, o.GetKind(), key(o), sh.where)
				if err := sh.client.Apply(ctx, client.ApplyConfigurationFromUnstructured(o.DeepCopy()), client.FieldOwner(fieldOwner), client.ForceOwnership); err != nil {
					return res(Fail, fmt.Sprintf("%s: apply %s %s%s: %v", st.at, o.GetKind(), key(o), sh.where, err))
				}
			}
			for _, o := range sh.step.Delete {
				r.logf("%s: delete %s %s%s", st.at, o.GetKind(), key(o), sh.where)
				if err := sh.client.Delete(ctx, o.DeepCopy()); client.IgnoreNotFound(err) != nil {
					return res(Fail, fmt.Sprintf("%s: delete %s %s%s: %v", st.at, o.GetKind(), key(o), sh.where, err))
				}
			}
			n += len(sh.step.Expectations)
		}
		if n == 0 {
			continue
		}
		r.logf("%s: polling %d files, timeout %s", st.at, n, r.Timeout)
		diff, err := r.poll(ctx, st.shares)
		if err != nil {
			return res(Fail, fmt.Sprintf("%s: %v", st.at, err))
		}
		if diff != "" {
			return res(Fail, fmt.Sprintf("%s: timed out after %s\n%s", st.at, r.Timeout, diff))
		}
	}
	return res(Pass, "")
}

// where names p's Kubernetes cluster in log lines and diffs, or is "" for a
// bundle that places no files.
func where(p *Bundle) string {
	if len(p.Clusters) != 1 {
		return ""
	}
	return " in " + p.Clusters[0].Name
}

// poll returns "" once every expectation holds, or else the diff of the
// last check when Timeout passes, which is never "" when no check completed,
// followed by the last error a round met. A round failing on an API error
// is retried until the deadline. Each round first publishes the external
// hostnames of every cluster's LoadBalancer Services to all of them.
func (r *Runner) poll(ctx context.Context, shares []share) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	tick := time.NewTicker(r.Interval)
	defer tick.Stop()
	diff := "  no status read before the deadline\n"
	var lastErr error
	for {
		err := PublishHosts(ctx, r.Clients)
		var d string
		if err == nil {
			d, err = r.check(ctx, shares)
		}
		switch {
		case err == nil && d == "":
			return "", nil
		case err == nil:
			diff, lastErr = d, nil
		default:
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return "", ctx.Err()
			}
			if lastErr != nil {
				diff += fmt.Sprintf("  last error: %v\n", lastErr)
			}
			return diff, nil
		case <-tick.C:
		}
	}
}

// check reads every target once and renders the expectations it fails; a
// target that does not exist yet fails its expectation rather than the check.
func (r *Runner) check(ctx context.Context, shares []share) (string, error) {
	var b strings.Builder
	for _, sh := range shares {
		for i, e := range sh.step.Expectations {
			t := sh.targets[i]
			live := &unstructured.Unstructured{}
			live.SetGroupVersionKind(t.GroupVersionKind())
			head := fmt.Sprintf("  %s -> %s %s%s\n", e.File, t.GetKind(), key(t), sh.where)
			err := sh.client.Get(ctx, client.ObjectKeyFromObject(t), live)
			switch {
			case apierrors.IsNotFound(err):
				b.WriteString(head + "    object not found\n")
				continue
			case err != nil:
				return "", fmt.Errorf("get %s %s%s: %w", t.GetKind(), key(t), sh.where, err)
			}
			if ms := Diff(e.Want, live.Object); len(ms) > 0 {
				b.WriteString(head + FormatMismatches(ms, "    "))
			}
		}
	}
	return b.String(), nil
}

// freshNamespace deletes namespace name, waits for it to go, and creates it
// again.
func (r *Runner) freshNamespace(ctx context.Context, c client.Client, name string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := c.Delete(ctx, ns)
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return fmt.Errorf("delete namespace %s: %w", name, err)
	default:
		if err := r.awaitGone(ctx, c, ns); err != nil {
			return err
		}
	}
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		return fmt.Errorf("create namespace %s: %w", name, err)
	}
	return nil
}

// releaseGuards lets every object in namespace ns that guards its deletion
// on a NATS server go without reaching one: a story's namespaces are deleted
// together, NATS servers and connections with the rest. NatsClusters get
// forceDeleteAnnotation; JetStream resources that delete their server
// object are set to retain it.
func releaseGuards(ctx context.Context, c client.Client, ns string) error {
	if err := forceDeletes(ctx, c, ns); err != nil {
		return err
	}
	return retainJetStream(ctx, c, ns)
}

// jetStreamKinds are the kinds whose deletionPolicy Delete reaches a server.
var jetStreamKinds = []string{"NatsStream", "NatsConsumer", "NatsKeyValue", "NatsObjectStore"}

// retainJetStream sets spec.deletionPolicy to Retain on every JetStream
// resource in namespace ns whose policy is Delete; an API server without
// the kinds has none.
func retainJetStream(ctx context.Context, c client.Client, ns string) error {
	for _, kind := range jetStreamKinds {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{Group: "jetstream.nats.mikluko.io", Version: "v1beta1", Kind: kind + "List"})
		err := c.List(ctx, list, client.InNamespace(ns))
		switch {
		case meta.IsNoMatchError(err):
			return nil
		case err != nil:
			return fmt.Errorf("list %ss in %s: %w", kind, ns, err)
		}
		for i := range list.Items {
			o := &list.Items[i]
			if p, _, _ := unstructured.NestedString(o.Object, "spec", "deletionPolicy"); p != "Delete" {
				continue
			}
			patch := []byte(`{"spec":{"deletionPolicy":"Retain"}}`)
			if err := c.Patch(ctx, o, client.RawPatch(types.MergePatchType, patch)); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("retain %s %s: %w", kind, key(o), err)
			}
		}
	}
	return nil
}

// forceDeleteAnnotation lets a NatsCluster's deletion proceed while its
// JetStream data remains or cannot be observed.
const forceDeleteAnnotation = "cluster.nats.mikluko.io/force-delete"

// forceDeletes sets forceDeleteAnnotation on every NatsCluster in namespace
// ns; an API server without the kind has none to set it on.
func forceDeletes(ctx context.Context, c client.Client, ns string) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: "cluster.nats.mikluko.io", Version: "v1beta1", Kind: "NatsClusterList"})
	err := c.List(ctx, list, client.InNamespace(ns))
	switch {
	case meta.IsNoMatchError(err):
		return nil
	case err != nil:
		return fmt.Errorf("list NatsClusters in %s: %w", ns, err)
	}
	for i := range list.Items {
		nc := &list.Items[i]
		if _, ok := nc.GetAnnotations()[forceDeleteAnnotation]; ok {
			continue
		}
		patch := fmt.Appendf(nil, `{"metadata":{"annotations":{%q:""}}}`, forceDeleteAnnotation)
		if err := c.Patch(ctx, nc, client.RawPatch(types.MergePatchType, patch)); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("annotate NatsCluster %s: %w", key(nc), err)
		}
	}
	return nil
}

func (r *Runner) awaitGone(ctx context.Context, c client.Client, ns *corev1.Namespace) error {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	tick := time.NewTicker(r.Interval)
	defer tick.Stop()
	for {
		err := c.Get(ctx, client.ObjectKeyFromObject(ns), &corev1.Namespace{})
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
