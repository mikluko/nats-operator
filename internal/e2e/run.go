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

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/authctl"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/natscluster"
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
	// Timeout bounds the wait for a step's expectations where its bundle
	// sets no wait of its own.
	Timeout time.Duration
	// Teardown bounds the wait for a previous story's namespace to finish
	// deleting; 0 is Timeout.
	Teardown time.Duration
	// Release is how long a terminating namespace with no Pod left keeps
	// the controllers' finalizers before the runner removes them.
	Release  time.Duration
	Interval time.Duration
	// Report is how often a step still waiting logs what it waits for; 0
	// logs nothing.
	Report time.Duration
	// Namespaces are watched for stuck pods and failed Jobs, besides the
	// story's own, in each Kubernetes cluster a step reaches.
	Namespaces []string
	Log        io.Writer
	// Publish, where set, runs with Clients at the start of every round of
	// a step's wait; its error fails the round as an API error does.
	Publish func(ctx context.Context, clients []client.Client) error
	// Fresh, where set, runs for each Kubernetes cluster, by its index in
	// Clients, once the story's namespaces there are fresh and before its
	// first step; its error fails the story.
	Fresh func(ctx context.Context, cluster int) error
}

const fieldOwner = "nats-operator-e2e"

// share is one Kubernetes cluster's part of one step: the client reaching
// it, the step's files placed there, and the object each expectation reads.
type share struct {
	client client.Client
	// cluster is the index of client in Runner.Clients.
	cluster int
	where   string
	step    Step
	targets []*unstructured.Unstructured
}

// stage is one step of one bundle across every Kubernetes cluster.
type stage struct {
	at     string
	wait   time.Duration
	shares []share
}

// Run runs b after every bundle it starts from, in namespaces it deletes and
// recreates first, waiting after each step for its expectations up to the
// step's Waits entry, else Timeout. A step fails before its wait on a signal
// that its expectations will not be met.
func (r *Runner) Run(ctx context.Context, b *Bundle) Result {
	start := time.Now()
	res := func(o Outcome, detail string) Result {
		return Result{Story: b.Name, Outcome: o, Elapsed: time.Since(start).Round(time.Second), Detail: detail}
	}
	pl, err := r.plan(b)
	var skip skipped
	switch {
	case errors.As(err, &skip):
		r.logf("%s: skipped: %s", b.Name, skip)
		return res(Skip, string(skip))
	case err != nil:
		return res(Fail, err.Error())
	}
	for i, nss := range pl.namespaces {
		for _, ns := range nss {
			if err := releaseGuards(ctx, r.Clients[i], ns); err != nil {
				return res(Fail, err.Error())
			}
		}
	}
	for i, nss := range pl.namespaces {
		for _, ns := range nss {
			r.logf("%s: fresh namespace %s%s", b.Name, ns, pl.wheres[i])
			if err := r.freshNamespace(ctx, r.Clients[i], ns); err != nil {
				return res(Fail, err.Error())
			}
		}
		if r.Fresh != nil {
			if err := r.Fresh(ctx, i); err != nil {
				return res(Fail, fmt.Sprintf("%s: fresh%s: %v", b.Name, pl.wheres[i], err))
			}
		}
	}
	for _, st := range pl.stages {
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
		r.logf("%s: polling %d files, timeout %s", st.at, n, st.wait)
		began := time.Now()
		o, err := r.poll(ctx, st, pl.namespaces)
		switch {
		case err != nil:
			return res(Fail, fmt.Sprintf("%s: %v", st.at, err))
		case o.signal != "":
			return res(Fail, fmt.Sprintf("%s: %s\n%s", st.at, o.signal, o.diff))
		case o.diff != "":
			return res(Fail, fmt.Sprintf("%s: timed out after %s\n%s", st.at, st.wait, o.diff))
		}
		r.logf("%s: matched after %s", st.at, time.Since(began).Round(time.Second))
	}
	return res(Pass, "")
}

// skipped is why a bundle is not run.
type skipped string

func (s skipped) Error() string { return string(s) }

// runPlan is what running one bundle takes.
type runPlan struct {
	// stages are the steps of the bundle and of every bundle it starts
	// from, bundle by bundle and step by step.
	stages []stage
	// namespaces and wheres are indexed as Runner.Clients: the namespaces,
	// sorted, the bundles declare objects in there, and how log lines name
	// the Kubernetes cluster.
	namespaces [][]string
	wheres     []string
}

// plan returns how b runs on r's Kubernetes clusters, reading none of
// them. It fails with skipped where b's SkipReason is not "" or b places
// files in more Kubernetes clusters than r.Clients reach, and otherwise
// where an expectation names no object of its Kubernetes cluster.
func (r *Runner) plan(b *Bundle) (runPlan, error) {
	if reason := b.SkipReason(); reason != "" {
		return runPlan{}, skipped(reason)
	}
	pl := runPlan{namespaces: make([][]string, len(r.Clients)), wheres: make([]string, len(r.Clients))}
	for _, c := range b.Chain() {
		parts := c.Parts()
		if len(parts) > len(r.Clients) {
			return runPlan{}, skipped(fmt.Sprintf("needs %d Kubernetes clusters, the run has %d", len(parts), len(r.Clients)))
		}
		var numbers []int
		for i, p := range parts {
			if pl.wheres[i] == "" {
				pl.wheres[i] = where(p)
			}
			pl.namespaces[i] = namespaces(pl.namespaces[i], p.Steps)
			for _, s := range p.Steps {
				if !slices.Contains(numbers, s.Number) {
					numbers = append(numbers, s.Number)
				}
			}
		}
		slices.Sort(numbers)
		for _, n := range numbers {
			st := stage{at: fmt.Sprintf("%s step %d", c.Name, n), wait: r.Timeout}
			if w, ok := c.Waits[n]; ok {
				st.wait = w.Wait
			}
			for i, p := range parts {
				j := slices.IndexFunc(p.Steps, func(s Step) bool { return s.Number == n })
				if j < 0 {
					continue
				}
				sh := share{client: r.Clients[i], cluster: i, where: where(p), step: p.Steps[j]}
				for _, e := range sh.step.Expectations {
					t, err := p.Target(n, e)
					if err != nil {
						return runPlan{}, fmt.Errorf("%s%s: %w", c.Name, sh.where, err)
					}
					sh.targets = append(sh.targets, t)
				}
				st.shares = append(st.shares, sh)
			}
			pl.stages = append(pl.stages, st)
		}
	}
	return pl, nil
}

// where names p's Kubernetes cluster in log lines and diffs, or is "" for a
// bundle that places no files.
func where(p Part) string {
	if p.Cluster == "" {
		return ""
	}
	return " in " + p.Cluster
}

// outcome is how a step's wait ended: diff is "" only once every expectation
// holds; signal is why the step stopped before its deadline, or "".
type outcome struct {
	diff   string
	signal string
}

// poll waits up to st.wait for every expectation of st to hold. A round
// failing on an API error is retried until the deadline; a round finding a
// signal ends the wait.
func (r *Runner) poll(ctx context.Context, st stage, namespaces [][]string) (outcome, error) {
	ctx, cancel := context.WithTimeout(ctx, st.wait)
	defer cancel()
	tick := time.NewTicker(r.Interval)
	defer tick.Stop()
	began, reported := time.Now(), time.Now()
	diff := "  no status read before the deadline\n"
	var lastErr error
	for {
		var err error
		if r.Publish != nil {
			err = r.Publish(ctx, r.Clients)
		}
		var d string
		if err == nil {
			d, err = r.check(ctx, st.shares)
		}
		switch {
		case err == nil && d == "":
			return outcome{}, nil
		case err == nil:
			diff, lastErr = d, nil
			sig, err := r.signal(ctx, st.shares, namespaces)
			if err != nil {
				lastErr = err
			} else if sig != "" {
				return outcome{diff: diff, signal: sig}, nil
			}
		default:
			lastErr = err
		}
		if r.Report > 0 && time.Since(reported) >= r.Report {
			reported = time.Now()
			r.logf("%s: waiting %s\n%s", st.at, time.Since(began).Round(time.Second), strings.TrimSuffix(diff, "\n"))
		}
		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return outcome{}, ctx.Err()
			}
			if lastErr != nil {
				diff += fmt.Sprintf("  last error: %v\n", lastErr)
			}
			return outcome{diff: diff}, nil
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
// together, NATS servers and connections with the rest.
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
		list.SetGroupVersionKind(schema.GroupVersionKind{Group: "jetstream.nats-operator.io", Version: "v1beta1", Kind: kind + "List"})
		err := c.List(ctx, list, client.InNamespace(ns))
		switch {
		case meta.IsNoMatchError(err):
			continue
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

// forceDeletes sets clusterv1beta1.AnnotationForceDelete on every NatsCluster
// in namespace ns; an API server without the kind has none to set it on.
func forceDeletes(ctx context.Context, c client.Client, ns string) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: "cluster.nats-operator.io", Version: "v1beta1", Kind: "NatsClusterList"})
	err := c.List(ctx, list, client.InNamespace(ns))
	switch {
	case meta.IsNoMatchError(err):
		return nil
	case err != nil:
		return fmt.Errorf("list NatsClusters in %s: %w", ns, err)
	}
	for i := range list.Items {
		nc := &list.Items[i]
		if _, ok := nc.GetAnnotations()[clusterv1beta1.AnnotationForceDelete]; ok {
			continue
		}
		patch := fmt.Appendf(nil, `{"metadata":{"annotations":{%q:""}}}`, clusterv1beta1.AnnotationForceDelete)
		if err := c.Patch(ctx, nc, client.RawPatch(types.MergePatchType, patch)); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("annotate NatsCluster %s: %w", key(nc), err)
		}
	}
	return nil
}

// awaitGone waits for namespace ns to go, removing the controllers'
// finalizers from what it holds once it has had no Pod for r.Release.
func (r *Runner) awaitGone(ctx context.Context, c client.Client, ns *corev1.Namespace) error {
	limit := r.Teardown
	if limit == 0 {
		limit = r.Timeout
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	tick := time.NewTicker(r.Interval)
	defer tick.Stop()
	var podless time.Time
	for {
		err := c.Get(ctx, client.ObjectKeyFromObject(ns), &corev1.Namespace{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		var pods corev1.PodList
		switch err := c.List(ctx, &pods, client.InNamespace(ns.Name)); {
		case err != nil || len(pods.Items) > 0:
			podless = time.Time{}
		case podless.IsZero():
			podless = time.Now()
		}
		if !podless.IsZero() && time.Since(podless) >= r.Release {
			if err := r.releaseFinalizers(ctx, c, ns.Name); err != nil {
				r.logf("namespace %s: %v", ns.Name, err)
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("namespace %s still terminating after %s", ns.Name, limit)
		case <-tick.C:
		}
	}
}

// heldBy is a kind of object whose controller holds its deletion, and the
// finalizer it holds it by.
type heldBy struct {
	gvk       schema.GroupVersionKind
	finalizer string
}

// controllerFinalizers are the finalizers the controllers add, in the order
// the runner removes them: users before their accounts, JetStream resources
// before the NatsCluster whose data they hold.
var controllerFinalizers = []heldBy{
	{schema.GroupVersionKind{Group: "auth.nats-operator.io", Version: "v1beta1", Kind: "NatsUser"}, authctl.UserFinalizer},
	{schema.GroupVersionKind{Group: "auth.nats-operator.io", Version: "v1beta1", Kind: "NatsAccount"}, authctl.AccountFinalizer},
	{schema.GroupVersionKind{Group: "jetstream.nats-operator.io", Version: "v1beta1", Kind: "NatsConsumer"}, lifecycle.Finalizer},
	{schema.GroupVersionKind{Group: "jetstream.nats-operator.io", Version: "v1beta1", Kind: "NatsStream"}, lifecycle.Finalizer},
	{schema.GroupVersionKind{Group: "jetstream.nats-operator.io", Version: "v1beta1", Kind: "NatsKeyValue"}, lifecycle.Finalizer},
	{schema.GroupVersionKind{Group: "jetstream.nats-operator.io", Version: "v1beta1", Kind: "NatsObjectStore"}, lifecycle.Finalizer},
	{schema.GroupVersionKind{Group: "jetstream.nats-operator.io", Version: "v1beta1", Kind: "NatsClusterEvacuation"}, lifecycle.Finalizer},
	{schema.GroupVersionKind{Group: "cluster.nats-operator.io", Version: "v1beta1", Kind: "NatsCluster"}, natscluster.FinalizerJetStreamData},
}

// releaseFinalizers removes controllerFinalizers from every object being
// deleted in namespace ns, logging each; a kind the API server lacks has
// none.
func (r *Runner) releaseFinalizers(ctx context.Context, c client.Client, ns string) error {
	for _, h := range controllerFinalizers {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(h.gvk.GroupVersion().WithKind(h.gvk.Kind + "List"))
		err := c.List(ctx, list, client.InNamespace(ns))
		switch {
		case meta.IsNoMatchError(err):
			continue
		case err != nil:
			return fmt.Errorf("list %ss: %w", h.gvk.Kind, err)
		}
		for i := range list.Items {
			o := &list.Items[i]
			if o.GetDeletionTimestamp() == nil || !slices.Contains(o.GetFinalizers(), h.finalizer) {
				continue
			}
			orig := o.DeepCopy()
			o.SetFinalizers(slices.DeleteFunc(o.GetFinalizers(), func(f string) bool { return f == h.finalizer }))
			if err := c.Patch(ctx, o, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("release %s %s: %w", h.gvk.Kind, key(o), err)
			}
			r.logf("namespace %s: released %s from %s %s", ns, h.finalizer, h.gvk.Kind, o.GetName())
		}
	}
	return nil
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
