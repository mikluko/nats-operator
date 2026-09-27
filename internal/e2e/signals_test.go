package e2e

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func waitingPod(ns, name, reason string, init bool) corev1.Pod {
	cs := []corev1.ContainerStatus{{Name: "nats", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: "back-off 10s"}}}}
	p := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	if init {
		p.Status.InitContainerStatuses = cs
	} else {
		p.Status.ContainerStatuses = cs
	}
	return p
}

func TestPodSignal(t *testing.T) {
	for _, tt := range []struct {
		name string
		pods []corev1.Pod
		want string
	}{
		{name: "crash loop", pods: []corev1.Pod{waitingPod("a", "p", "CrashLoopBackOff", false)}, want: "pod a/p container nats: CrashLoopBackOff: back-off 10s"},
		{name: "image pull back-off in an init container", pods: []corev1.Pod{waitingPod("a", "p", "ImagePullBackOff", true)}, want: "pod a/p container nats: ImagePullBackOff: back-off 10s"},
		{name: "image never pulled", pods: []corev1.Pod{waitingPod("a", "p", "ErrImageNeverPull", false)}, want: "pod a/p container nats: ErrImageNeverPull: back-off 10s"},
		{name: "a Job's pod", pods: []corev1.Pod{jobPod(waitingPod("a", "p", "CrashLoopBackOff", false))}},
		{name: "creating", pods: []corev1.Pod{waitingPod("a", "p", "ContainerCreating", false)}},
		{name: "a first pull error", pods: []corev1.Pod{waitingPod("a", "p", "ErrImagePull", false)}},
		{name: "none"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, podSignal(tt.pods))
		})
	}
}

func TestJobSignal(t *testing.T) {
	job := func(typ batchv1.JobConditionType, status corev1.ConditionStatus) batchv1.Job {
		return batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Namespace: "messaging", Name: "streams"},
			Status:     batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: typ, Status: status, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"}}},
		}
	}
	require.Equal(t, "job messaging/streams failed: BackoffLimitExceeded: Job has reached the specified backoff limit",
		jobSignal([]batchv1.Job{job(batchv1.JobFailed, corev1.ConditionTrue)}))
	require.Empty(t, jobSignal([]batchv1.Job{job(batchv1.JobFailed, corev1.ConditionFalse)}))
	require.Empty(t, jobSignal([]batchv1.Job{job(batchv1.JobComplete, corev1.ConditionTrue)}))
}

func TestConditionSignal(t *testing.T) {
	live := func(conds ...map[string]any) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{"conditions": toAny(conds)}}}
		u.SetKind("NatsStream")
		u.SetNamespace("nats-system")
		u.SetName("orders")
		return u
	}
	cond := func(typ, status, reason string) map[string]any {
		return map[string]any{"type": typ, "status": status, "reason": reason, "message": "m"}
	}
	want := func(conds ...map[string]any) map[string]any {
		return map[string]any{"status": map[string]any{"conditions": toAny(conds)}}
	}
	readyTrue := want(map[string]any{"type": "Ready", "status": "True"})
	for _, tt := range []struct {
		name string
		live *unstructured.Unstructured
		want map[string]any
		sig  string
	}{
		{name: "Terminal True", live: live(cond("Ready", "False", "Terminal"), cond("Terminal", "True", "Rejected")), want: readyTrue,
			sig: "NatsStream nats-system/orders: Ready=False Terminal: m"},
		{name: "Ready False unsupported", live: live(cond("Ready", "False", "UnsupportedSpec")), want: readyTrue,
			sig: "NatsStream nats-system/orders: Ready=False UnsupportedSpec: m"},
		{name: "Ready False while it waits", live: live(cond("Ready", "False", "Creating")), want: readyTrue},
		{name: "Terminal False", live: live(cond("Terminal", "False", "Resolved")), want: readyTrue},
		{name: "Terminal the expectation holds", live: live(cond("Ready", "False", "Terminal"), cond("Terminal", "True", "ExistsUnowned")),
			want: want(map[string]any{"type": "Ready", "status": "False"}, map[string]any{"type": "Terminal", "status": "True"})},
		{name: "no conditions", live: live(), want: readyTrue},
		{name: "placeholders in the expectation", live: live(cond("Ready", "False", "Rejected")),
			want: want(map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": placeholder{}}),
			sig:  "NatsStream nats-system/orders: Ready=False Rejected: m"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.sig, conditionSignal(tt.live, tt.want))
		})
	}
}

func toAny(ms []map[string]any) []any {
	out := make([]any, len(ms))
	for i, m := range ms {
		out[i] = m
	}
	return out
}

// TestPoll_Signals pins that a step stops waiting within a round of a
// stuck pod in its story's namespaces or the runner's own, and that a step
// still waiting logs its diff every Report.
func TestPoll_Signals(t *testing.T) {
	target := &unstructured.Unstructured{}
	target.SetAPIVersion("v1")
	target.SetKind("ConfigMap")
	target.SetNamespace("story")
	target.SetName("x")
	exp := Expectation{File: "01-live-configmap.yaml", Want: map[string]any{"data": map[string]any{"v": "2"}}}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "story", Name: "x"}, Data: map[string]string{"v": "1"}}
	for _, tt := range []struct {
		name       string
		pod        corev1.Pod
		namespaces []string
		signal     string
	}{
		{name: "story namespace", pod: waitingPod("story", "nats-0", "CrashLoopBackOff", false), signal: "pod story/nats-0 container nats: CrashLoopBackOff: back-off 10s"},
		{name: "runner namespace", pod: waitingPod("nats-operator", "ctl", "ErrImageNeverPull", false), namespaces: []string{"nats-operator"},
			signal: "pod nats-operator/ctl container nats: ErrImageNeverPull: back-off 10s"},
		{name: "unwatched namespace", pod: waitingPod("elsewhere", "p", "CrashLoopBackOff", false)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pod := tt.pod
			c := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(cm.DeepCopy(), &pod).Build()
			var log bytes.Buffer
			r := &Runner{Clients: []client.Client{c}, Timeout: 300 * time.Millisecond, Interval: 10 * time.Millisecond,
				Report: 100 * time.Millisecond, Namespaces: tt.namespaces, Log: &log}
			st := stage{at: "01-x step 1", wait: r.Timeout, shares: []share{{client: c, step: Step{Expectations: []Expectation{exp}},
				targets: []*unstructured.Unstructured{target}}}}
			start := time.Now()
			o, err := r.poll(t.Context(), st, [][]string{{"story"}})
			require.NoError(t, err)
			require.Equal(t, tt.signal, o.signal)
			require.Contains(t, o.diff, `.data.v: want "2", got "1"`)
			if tt.signal != "" {
				require.Less(t, time.Since(start), 100*time.Millisecond)
				return
			}
			require.Contains(t, log.String(), "01-x step 1: waiting ")
			require.Contains(t, log.String(), `.data.v: want "2", got "1"`)
		})
	}
}

func jobPod(p corev1.Pod) corev1.Pod {
	p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "streams", UID: "u"}}
	return p
}
