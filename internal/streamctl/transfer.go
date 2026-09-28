package streamctl

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/jsapi"
	"github.com/mikluko/nats-operator/internal/lifecycle"
)

// ReasonMoving is Synced's reason, False, while a NatsStream's spec is
// applied and its stream is moving to another NATS cluster.
const ReasonMoving = "Moving"

// clusterWire is the part of a stream or consumer info's cluster that a
// transfer is read from.
type clusterWire struct {
	// Name is the NATS cluster of the server that answered, the group
	// leader's.
	Name     string     `json:"name"`
	Leader   string     `json:"leader"`
	Replicas []peerWire `json:"replicas"`
	// Desired is the placement the group is moving to; nil when it is
	// where its config puts it.
	Desired *struct {
		Created  time.Time `json:"created"`
		Name     string    `json:"name"`
		Replicas []struct {
			Name string `json:"name"`
		} `json:"replicas"`
		Origin *struct {
			Placement *struct {
				Cluster string `json:"cluster"`
			} `json:"placement"`
		} `json:"origin"`
	} `json:"desired"`
}

// peerWire is one follower of a group, as a cluster info reports it.
type peerWire struct {
	Name    string `json:"name"`
	Current bool   `json:"current"`
	Lag     uint64 `json:"lag"`
}

// streamTransfer returns the move to another NATS cluster c reports, with
// no consumer counts, or nil when there is none.
func streamTransfer(c *clusterWire) *js.StreamTransfer {
	if c == nil || c.Desired == nil || c.Desired.Name == "" {
		return nil
	}
	from := c.Name
	if o := c.Desired.Origin; o != nil && o.Placement != nil && o.Placement.Cluster != "" {
		from = o.Placement.Cluster
	}
	if from == c.Desired.Name {
		return nil
	}
	t := &js.StreamTransfer{From: from, To: c.Desired.Name}
	if !c.Desired.Created.IsZero() {
		t.Started = ptrTo(metav1.NewTime(c.Desired.Created))
	}
	for _, d := range c.Desired.Replicas {
		r := js.ReplicaStatus{Name: d.Name, Current: d.Name == c.Leader}
		if i := slices.IndexFunc(c.Replicas, func(p peerWire) bool { return p.Name == d.Name }); i >= 0 {
			r.Current, r.Lag = c.Replicas[i].Current, int64(c.Replicas[i].Lag) //nolint:gosec // lag fits.
		}
		t.Replicas = append(t.Replicas, r)
	}
	slices.SortFunc(t.Replicas, func(a, b js.ReplicaStatus) int { return strings.Compare(a.Name, b.Name) })
	return t
}

// consumersMoved counts, of consumers, those in cluster to and at their
// placement, out of total.
func consumersMoved(consumers []clusterWire, total int, to string) *js.TransferConsumers {
	out := &js.TransferConsumers{Total: int32(total)} //nolint:gosec // consumer counts fit.
	for _, c := range consumers {
		if c.Desired == nil && c.Name == to {
			out.Moved++
		}
	}
	return out
}

// observeTransfer records on s the move to another NATS cluster info
// reports and, while there is one, turns a Synced that matches spec False
// with ReasonMoving.
func observeTransfer(ctx context.Context, o lifecycle.Object, s *js.NatsStream, info *lifecycle.Info) error {
	var reply struct {
		Cluster *clusterWire `json:"cluster"`
	}
	if err := json.Unmarshal(info.Raw, &reply); err != nil {
		return fmt.Errorf("decode stream info: %w", err)
	}
	t := streamTransfer(reply.Cluster)
	s.Status.Transfer = t
	if t == nil {
		return nil
	}
	synced := meta.FindStatusCondition(s.Status.Conditions, lifecycle.ConditionSynced)
	if synced != nil && synced.Status == metav1.ConditionTrue && synced.Reason == lifecycle.ReasonMatchesSpec {
		current := 0
		for _, r := range t.Replicas {
			if r.Current {
				current++
			}
		}
		conditions.Set(&s.Status.Conditions, s.Generation, metav1.Condition{
			Type: lifecycle.ConditionSynced, Status: metav1.ConditionFalse, Reason: ReasonMoving,
			Message: fmt.Sprintf("moving to cluster %s; %d of %d new replicas current", t.To, current, len(t.Replicas)),
		})
	}
	so, ok := o.(*streamObject)
	if !ok {
		return nil
	}
	consumers, total, err := so.consumerClusters(ctx)
	if err != nil {
		return err
	}
	t.Consumers = consumersMoved(consumers, total, t.To)
	return nil
}

// consumerListTimeout bounds one CONSUMER.LIST request above the four
// seconds nats-server waits on the consumers' leaders before it answers.
const consumerListTimeout = 10 * time.Second

// consumerClusters returns the cluster info of every consumer on the stream that
// answered, and how many consumers the stream has.
func (o *streamObject) consumerClusters(ctx context.Context) ([]clusterWire, int, error) {
	subject := "$JS.API.CONSUMER.LIST." + streamName(o.obj)
	var out []clusterWire
	for offset := 0; ; {
		ctx, cancel := context.WithTimeout(ctx, consumerListTimeout)
		raw, err := jsapi.Request(ctx, o.api.Conn, subject, map[string]int{"offset": offset})
		cancel()
		if err != nil {
			return nil, 0, err
		}
		page, err := decodeConsumerPage(raw)
		if err != nil {
			return nil, 0, fmt.Errorf("decode %s reply: %w", subject, err)
		}
		out = append(out, page.clusters...)
		offset += page.covered
		if page.covered == 0 || offset >= page.total {
			return out, page.total, nil
		}
	}
}

// consumerPage is one CONSUMER.LIST reply: the cluster info of the consumers it
// carries, how many consumers it covers, counting those listed as missing,
// and how many the stream has.
type consumerPage struct {
	clusters []clusterWire
	covered  int
	total    int
}

func decodeConsumerPage(raw []byte) (consumerPage, error) {
	var reply struct {
		Total     int `json:"total"`
		Consumers []struct {
			Name    string       `json:"name"`
			Cluster *clusterWire `json:"cluster"`
		} `json:"consumers"`
		Missing []string `json:"missing"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil {
		return consumerPage{}, err
	}
	p := consumerPage{total: reply.Total}
	names := map[string]bool{}
	for _, c := range reply.Consumers {
		names[c.Name] = true
		if c.Cluster == nil {
			c.Cluster = &clusterWire{}
		}
		p.clusters = append(p.clusters, *c.Cluster)
	}
	for _, n := range reply.Missing {
		names[n] = true
	}
	p.covered = len(names)
	return p, nil
}
