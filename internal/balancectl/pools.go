package balancectl

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/balance"
	"github.com/mikluko/nats-operator/internal/streamctl"
)

// A member is a resource a pool can select: a NatsStream, NatsKeyValue or
// NatsObjectStore, by the stream it stands for on the server.
type member struct {
	Labels labels.Set
	// Stream is the server-side stream, as [streamctl.ServerStream] names it.
	Stream string
	// Cluster is the NATS cluster its spec pins it to, "" where none.
	Cluster string
	Ready   bool
}

// members lists the NatsStream, NatsKeyValue and NatsObjectStore resources in
// namespace whose connectionRef names conn.
func members(ctx context.Context, c client.Reader, namespace string, conn natsv1beta1.ObjectReference) ([]member, error) {
	same := func(ref natsv1beta1.ObjectReference) bool {
		return ref.ObjectKey(namespace) == conn.ObjectKey(namespace)
	}
	var out []member
	var streams js.NatsStreamList
	if err := c.List(ctx, &streams, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list NatsStreams: %w", err)
	}
	for i := range streams.Items {
		s := &streams.Items[i]
		if same(s.Spec.ConnectionRef) {
			out = append(out, newMember(s, s.Spec.Placement, s.Status.Conditions))
		}
	}
	var kvs js.NatsKeyValueList
	if err := c.List(ctx, &kvs, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list NatsKeyValues: %w", err)
	}
	for i := range kvs.Items {
		s := &kvs.Items[i]
		if same(s.Spec.ConnectionRef) {
			out = append(out, newMember(s, s.Spec.Placement, s.Status.Conditions))
		}
	}
	var objs js.NatsObjectStoreList
	if err := c.List(ctx, &objs, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list NatsObjectStores: %w", err)
	}
	for i := range objs.Items {
		s := &objs.Items[i]
		if same(s.Spec.ConnectionRef) {
			out = append(out, newMember(s, s.Spec.Placement, s.Status.Conditions))
		}
	}
	return out, nil
}

func newMember(o client.Object, p *js.Placement, conds []metav1.Condition) member {
	out := member{Labels: o.GetLabels(), Stream: streamctl.ServerStream(o), Ready: meta.IsStatusConditionTrue(conds, ConditionReady)}
	if p != nil {
		out.Cluster = p.Cluster
	}
	return out
}

// assign is the declared pools of account over ms: each stream in the first
// pool whose selector matches a resource standing for it. overlaps says, per
// stream matched by several, which pools match it and which it is balanced in.
func assign(account string, pools []js.Pool, ms []member) (declared []balance.Pool, overlaps []string, err error) {
	selectors := make([]labels.Selector, len(pools))
	for i, p := range pools {
		if selectors[i], err = metav1.LabelSelectorAsSelector(&p.Selector); err != nil {
			return nil, nil, fmt.Errorf("pool %s: %w", p.Name, err)
		}
	}
	matched := map[string][]int{}
	var order []string
	for _, m := range ms {
		for i, s := range selectors {
			if !s.Matches(m.Labels) {
				continue
			}
			if _, seen := matched[m.Stream]; !seen {
				order = append(order, m.Stream)
			}
			if !slices.Contains(matched[m.Stream], i) {
				matched[m.Stream] = append(matched[m.Stream], i)
			}
		}
	}
	slices.Sort(order)
	declared = make([]balance.Pool, len(pools))
	for i, p := range pools {
		declared[i].Name = p.Name
	}
	for _, stream := range order {
		in := matched[stream]
		slices.Sort(in)
		first := in[0]
		declared[first].Streams = append(declared[first].Streams, balance.StreamID{Account: account, Stream: stream})
		if len(in) > 1 {
			names := make([]string, len(in))
			for j, i := range in {
				names[j] = pools[i].Name
			}
			overlaps = append(overlaps, fmt.Sprintf("%s matches %s; balanced in %s", stream, andList(names), pools[first].Name))
		}
	}
	return declared, overlaps, nil
}

// andList is names joined as English prose: "a", "a and b", "a, b and c".
func andList(names []string) string {
	if len(names) < 2 {
		return strings.Join(names, "")
	}
	return strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
}

// expected is the streams of ms whose absence from an observation of cluster
// holds the pass: those Ready and pinned to it.
func expected(ms []member, cluster string) []string {
	var out []string
	for _, m := range ms {
		if m.Ready && m.Cluster == cluster && !slices.Contains(out, m.Stream) {
			out = append(out, m.Stream)
		}
	}
	slices.Sort(out)
	return out
}

// poolStatus is each declared pool's evenness as reports read it, a pool
// holding no observed stream reading zero, then the default pool where a
// report carries it.
func poolStatus(pools []js.Pool, reports []balance.PoolReport) []js.PoolStatus {
	byName := map[string]balance.PoolReport{}
	for _, r := range reports {
		byName[r.Name] = r
	}
	var out []js.PoolStatus
	for _, p := range pools {
		r := byName[p.Name]
		out = append(out, js.PoolStatus{Name: p.Name, Streams: int32(r.Streams), LeaderSkew: int32(r.LeaderSkew)})
	}
	if r, ok := byName[balance.DefaultPool]; ok {
		out = append(out, js.PoolStatus{Name: r.Name, Streams: int32(r.Streams), LeaderSkew: int32(r.LeaderSkew)})
	}
	return out
}
