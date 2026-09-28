package e2e

import (
	"context"
	"fmt"
	"slices"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// stuckWaiting are the container waiting reasons that no wait ends.
var stuckWaiting = []string{"CrashLoopBackOff", "ImagePullBackOff", "ErrImageNeverPull", "InvalidImageName"}

// terminalReasons are the Ready=False reasons the controllers give a spec
// they will not act on until it is edited.
var terminalReasons = []string{
	"Terminal", "Rejected", "ImmutableField", "ExistsUnowned", "OwnedByOther", "NotABucket",
	"UnsupportedSpec", "GatewayWithoutTLS", "DuplicateBalancer", "InvalidPool", "StreamsInSeveralPools", "TargetTagsInSource",
	"InvalidKeys", "InvalidJWT", "TrustInvalid", "LeafRemoteInvalid", "InvalidSecret", "SecretConflict",
}

// podSignal returns why a pod of pods will not become ready, or "": a
// container, init containers included, waiting for a reason in
// stuckWaiting. A Job's pod is left out: a Job retries a crashing pod while
// what it needs comes up, and jobSignal reports it once it gives up.
func podSignal(pods []corev1.Pod) string {
	for _, p := range pods {
		if slices.ContainsFunc(p.OwnerReferences, func(o metav1.OwnerReference) bool { return o.Kind == "Job" }) {
			continue
		}
		for _, cs := range slices.Concat(p.Status.InitContainerStatuses, p.Status.ContainerStatuses) {
			if w := cs.State.Waiting; w != nil && slices.Contains(stuckWaiting, w.Reason) {
				return fmt.Sprintf("pod %s/%s container %s: %s: %s", p.Namespace, p.Name, cs.Name, w.Reason, w.Message)
			}
		}
	}
	return ""
}

// jobSignal returns why a Job of jobs has failed, or "".
func jobSignal(jobs []batchv1.Job) string {
	for _, j := range jobs {
		for _, c := range j.Status.Conditions {
			if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
				return fmt.Sprintf("job %s/%s failed: %s: %s", j.Namespace, j.Name, c.Reason, c.Message)
			}
		}
	}
	return ""
}

// conditionSignal returns why live, whose expectation is want, will not
// reach it, or "": its Terminal condition is True, or its Ready condition is
// False for a reason in terminalReasons. A condition want itself holds at
// that status is no signal.
func conditionSignal(live *unstructured.Unstructured, want map[string]any) string {
	wanted := map[string]string{}
	for _, w := range conditionsOf(want) {
		if m, ok := w.(map[string]any); ok {
			t, _ := m["type"].(string)
			s, _ := m["status"].(string)
			wanted[t] = s
		}
	}
	for _, c := range conditionsOf(live.Object) {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		t, _ := m["type"].(string)
		s, _ := m["status"].(string)
		reason, _ := m["reason"].(string)
		msg, _ := m["message"].(string)
		terminal := (t == "Terminal" && s == "True") || (t == "Ready" && s == "False" && slices.Contains(terminalReasons, reason))
		if terminal && wanted[t] != s {
			return fmt.Sprintf("%s %s: %s=%s %s: %s", live.GetKind(), key(live), t, s, reason, msg)
		}
	}
	return ""
}

// signal returns why the shares' expectations will not be met, or "";
// namespaces is indexed by a share's Kubernetes cluster.
func (r *Runner) signal(ctx context.Context, shares []share, namespaces [][]string) (string, error) {
	for _, sh := range shares {
		for i, e := range sh.step.Expectations {
			t := sh.targets[i]
			live := &unstructured.Unstructured{}
			live.SetGroupVersionKind(t.GroupVersionKind())
			if err := sh.client.Get(ctx, client.ObjectKeyFromObject(t), live); err != nil {
				continue
			}
			if s := conditionSignal(live, e.Want); s != "" {
				return s + sh.where, nil
			}
		}
	}
	seen := map[int]bool{}
	for _, sh := range shares {
		if seen[sh.cluster] {
			continue
		}
		seen[sh.cluster] = true
		for _, ns := range slices.Concat(namespaces[sh.cluster], r.Namespaces) {
			var pods corev1.PodList
			if err := sh.client.List(ctx, &pods, client.InNamespace(ns)); err != nil {
				return "", fmt.Errorf("list pods in %s%s: %w", ns, sh.where, err)
			}
			if s := podSignal(pods.Items); s != "" {
				return s + sh.where, nil
			}
			var jobs batchv1.JobList
			if err := sh.client.List(ctx, &jobs, client.InNamespace(ns)); err != nil {
				return "", fmt.Errorf("list jobs in %s%s: %w", ns, sh.where, err)
			}
			if s := jobSignal(jobs.Items); s != "" {
				return s + sh.where, nil
			}
		}
	}
	return "", nil
}

// conditionsOf returns status.conditions of obj without copying it: an
// expectation's values include placeholders, which cannot be deep-copied.
func conditionsOf(obj map[string]any) []any {
	status, _ := obj["status"].(map[string]any)
	cs, _ := status["conditions"].([]any)
	return cs
}
