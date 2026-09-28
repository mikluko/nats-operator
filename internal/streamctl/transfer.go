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
// placement.
func consumersMoved(consumers []clusterWire, to string) *js.TransferConsumers {
	out := &js.TransferConsumers{Total: int32(len(consumers))} //nolint:gosec // consumer counts fit.
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
	consumers, err := so.consumerClusters(ctx)
	if err != nil {
		return err
	}
	t.Consumers = consumersMoved(consumers, t.To)
	return nil
}

// consumerClusters returns the cluster of every consumer on the stream.
func (o *streamObject) consumerClusters(ctx context.Context) ([]clusterWire, error) {
	subject := "$JS.API.CONSUMER.LIST." + streamName(o.obj)
	var out []clusterWire
	for {
		raw, err := jsapi.Request(ctx, o.api.Conn, subject, map[string]int{"offset": len(out)})
		if err != nil {
			return nil, err
		}
		var page struct {
			Total     int `json:"total"`
			Consumers []struct {
				Cluster *clusterWire `json:"cluster"`
			} `json:"consumers"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, fmt.Errorf("decode %s reply: %w", subject, err)
		}
		for _, c := range page.Consumers {
			if c.Cluster == nil {
				c.Cluster = &clusterWire{}
			}
			out = append(out, *c.Cluster)
		}
		if len(page.Consumers) == 0 || len(out) >= page.Total {
			return out, nil
		}
	}
}
