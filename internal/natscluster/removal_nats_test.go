package natscluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// removalCluster is story 2's NatsCluster at five servers booted
// in-process, with its StatefulSets as the reconciler would hold them and
// the cluster controller's system connection.
type removalCluster struct {
	nc    *clusterv1beta1.NatsCluster
	plan  *Plan
	sys   *SystemConnections
	admin ServerAdmin
	srvs  map[string]*server.Server
	files map[string]string
	sets  map[string]*appsv1.StatefulSet
	js    nats.JetStreamContext
}

// startRemovalCluster boots five servers rendered at the revision of
// nc at replicas, so that the servers beyond it are surplus, pushes an
// account named orders with JetStream, and returns once the system user
// observes all five Settled.
func startRemovalCluster(t *testing.T, replicas int32) *removalCluster {
	t.Helper()
	p := mintPlane(t)
	rc := &removalCluster{nc: storyAuthCluster(t), srvs: map[string]*server.Server{}, files: map[string]string{}, sets: map[string]*appsv1.StatefulSet{}}
	rc.nc.Spec.Replicas = 5
	all, err := Render(rc.nc, Inputs{Trust: p.trust})
	require.NoError(t, err)
	rc.nc.Spec.Replicas = replicas
	rc.plan, err = Render(rc.nc, Inputs{Trust: p.trust})
	require.NoError(t, err)
	for i, s := range all.Servers {
		sts := s.StatefulSet.DeepCopy()
		if i < int(replicas) {
			sts = rc.plan.Servers[i].StatefulSet.DeepCopy()
		}
		sts.Status.ObservedGeneration, sts.Status.UpdatedReplicas, sts.Status.ReadyReplicas = sts.Generation, 1, 1
		rc.sets[s.Name] = sts
	}

	rc.nc.Spec.Replicas = 5
	_, srvs, url, files := startRendered(t, rc.nc, p.trust, rc.plan.Revision)
	rc.nc.Spec.Replicas = replicas
	for i, s := range srvs {
		rc.srvs[s.Name()], rc.files[s.Name()] = s, files[i]
	}

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: rc.nc.Namespace, Name: rc.nc.Spec.Auth.SystemCredentials.SecretKeyRef.Name},
		Data:       map[string][]byte{natsconn.DefaultCredentialsKey: p.systemCreds(t, jwtplane.PresetClusterController)},
	}
	pool := natsconn.NewPool()
	t.Cleanup(pool.Close)
	rc.sys = &SystemConnections{
		Client:  fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build(),
		Pool:    pool,
		Servers: func(*clusterv1beta1.NatsCluster) []string { return []string{url} },
		Wait:    time.Second,
	}
	rc.admin, err = rc.sys.Admin(t.Context(), rc.nc)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		s, err := rc.sys.Observe(t.Context(), rc.nc)
		return err == nil && len(s.Servers) == 5 && s.Verdict().Settled() && len(s.Groups) == 1 && len(s.Groups[0].Members) == 5
	}, 30*time.Second, 200*time.Millisecond, "five servers not observed Settled over $SYS")

	account := newTestKeys(t, nkeys.PrefixByteAccount)
	accJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: "orders", Keys: account,
		Limits: jwtplane.Limits{JetStream: &jwtplane.JetStreamLimits{}}}, p.op, time.Now())
	require.NoError(t, err)
	admin, err := natsconn.Dial(natsconn.Endpoint{Servers: []string{url}, Creds: p.systemCreds(t, jwtplane.PresetAuthController)})
	require.NoError(t, err)
	defer admin.Close()
	reply, err := admin.Request("$SYS.REQ.CLAIMS.UPDATE", []byte(accJWT), 2*time.Second)
	require.NoError(t, err)
	var resp server.ServerAPIClaimUpdateResponse
	require.NoError(t, json.Unmarshal(reply.Data, &resp))
	require.Nil(t, resp.Error)
	user, err := natsconn.Dial(natsconn.Endpoint{Servers: []string{url}, Creds: p.creds(t, account, jwtplane.User{Name: "orders"})})
	require.NoError(t, err)
	t.Cleanup(user.Close)
	rc.js, err = user.JetStream()
	require.NoError(t, err)
	return rc
}

// addStream creates an R replicas stream holding n messages.
func (rc *removalCluster) addStream(t *testing.T, name string, replicas, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := rc.js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{name + ".>"}, Replicas: replicas})
		return err == nil
	}, 30*time.Second, 200*time.Millisecond, "stream %s not created", name)
	for i := range n {
		_, err := rc.js.Publish(fmt.Sprintf("%s.%d", name, i), []byte("m"))
		require.NoError(t, err)
	}
}

// observe is one reconcile's view: the snapshot, and the rollout state the
// reconciler would decide on.
func (rc *removalCluster) observe(t *testing.T) (*sysobs.Snapshot, rolloutState) {
	t.Helper()
	snap, st, err := rc.state(t.Context())
	require.NoError(t, err)
	return snap, st
}

// state is observe for a caller that cannot fail the test.
func (rc *removalCluster) state(ctx context.Context) (*sysobs.Snapshot, rolloutState, error) {
	snap, err := rc.sys.Observe(ctx, rc.nc)
	if err != nil {
		return nil, rolloutState{}, err
	}
	r := &Reconciler{}
	return snap, r.rolloutState(rc.nc, rc.plan, Observed{StatefulSets: rc.sets, Snapshot: snap}), nil
}

// step takes one decision and carries out its removal action against the
// NATS cluster, stopping a deleted server.
func (rc *removalCluster) step(t *testing.T, st rolloutState, log *[]string) rolloutDecision {
	t.Helper()
	d := decide(st)
	require.Empty(t, d.Step)
	if d.Remove == nil {
		return d
	}
	x := d.Remove.Server
	*log = append(*log, string(d.Remove.Action)+" "+x)
	ctx := t.Context()
	switch d.Remove.Action {
	case actionEvacuate:
		require.NoError(t, rc.admin.Evacuate(ctx, x))
		rc.sets[x].Annotations[AnnotationRemoval] = string(phaseEvacuating)
	case actionStepDown:
		require.NoError(t, rc.admin.StepDownMeta(ctx))
	case actionRemovePeer:
		require.NoError(t, rc.admin.RemovePeer(ctx, x))
		rc.sets[x].Annotations[AnnotationRemoval] = string(phaseRemoved)
	case actionDelete:
		rc.srvs[x].Shutdown()
		rc.srvs[x].WaitForShutdown()
		delete(rc.sets, x)
	}
	return d
}

// TestScaleDown_InProcess pins scale-down against nats-server 2.15.0: from
// five servers to three, a stream with five replicas blocks it; without
// it, each surplus server is evacuated, removed from the meta group and
// stopped in turn, and an R3 and an R1 stream end on the three that stay
// with every message.
func TestScaleDown_InProcess(t *testing.T) {
	rc := startRemovalCluster(t, 3)
	rc.addStream(t, "ORDERS", 3, 100)
	rc.addStream(t, "WIDE", 5, 10)
	var log []string
	require.Eventually(t, func() bool {
		snap, _ := rc.observe(t)
		return snap.Verdict().Settled() && len(snap.Groups) == 3
	}, 30*time.Second, 200*time.Millisecond)
	_, st := rc.observe(t)
	d := rc.step(t, st, &log)
	require.NotNil(t, d.Blocked)
	require.Equal(t, "cannot remove demo-4, demo-3: orders/WIDE has 5 replicas; 3 servers cannot hold them", d.Blocked.Message)

	require.NoError(t, rc.js.DeleteStream("WIDE"))
	rc.addStream(t, "LOCAL", 1, 50)

	require.Eventually(t, func() bool {
		_, st := rc.observe(t)
		d := rc.step(t, st, &log)
		return d.Status == nil && d.Blocked == nil
	}, 90*time.Second, 300*time.Millisecond, "scale-down did not finish: %v", log)

	var want []string
	for _, x := range []string{"demo-4", "demo-3"} {
		want = append(want, "Evacuate "+x)
		if slices.Contains(log, "StepDownMeta "+x) {
			want = append(want, "StepDownMeta "+x)
		}
		want = append(want, "RemovePeer "+x, "Delete "+x)
	}
	require.Equal(t, want, log)

	snap, _ := rc.observe(t)
	require.True(t, snap.Verdict().Settled())
	plan := []string{"demo-0", "demo-1", "demo-2"}
	for _, g := range snap.Groups {
		var on []string
		for _, m := range g.Members {
			on = append(on, m.Server)
		}
		require.Subset(t, plan, on, "%s %s", g.Kind, g.Stream)
		if g.Kind == sysobs.KindStream && g.Stream == "ORDERS" {
			require.Len(t, on, 3)
			require.Equal(t, "orders", g.AccountName)
		}
	}
	for name, msgs := range map[string]uint64{"ORDERS": 100, "LOCAL": 50} {
		info, err := rc.js.StreamInfo(name)
		require.NoError(t, err)
		require.Equal(t, msgs, info.State.Msgs, name)
	}
}

// TestReplace_InProcess pins a replacement against nats-server 2.15.0 up to
// the removal tombstone: the replaced server, restarted under its own name
// on an empty store, answers but stays outside the meta group, and the
// gate holds on it.
func TestReplace_InProcess(t *testing.T) {
	rc := startRemovalCluster(t, 5)
	rc.addStream(t, "ORDERS", 3, 100)
	rc.sets["demo-3"].Annotations[AnnotationRemoval] = string(phaseRequested)
	var log []string
	var deleted bool
	require.Eventually(t, func() bool {
		_, st := rc.observe(t)
		d := rc.step(t, st, &log)
		deleted = d.Remove != nil && d.Remove.Action == actionDelete
		return deleted
	}, 60*time.Second, 300*time.Millisecond, "replacement did not reach deletion: %v", log)

	store := filepath.Join(filepath.Dir(rc.files["demo-3"]), "jetstream")
	require.NoError(t, os.RemoveAll(store))
	rc.srvs["demo-3"] = startFile(t, rc.files["demo-3"])
	sts := rc.plan.Servers[3].StatefulSet.DeepCopy()
	sts.Status.ObservedGeneration, sts.Status.UpdatedReplicas, sts.Status.ReadyReplicas = sts.Generation, 1, 1
	rc.sets["demo-3"] = sts

	require.Eventually(t, func() bool {
		snap, _ := rc.observe(t)
		return slices.ContainsFunc(snap.Servers, func(s sysobs.Server) bool { return s.Name == "demo-3" }) && !slices.Contains(snap.Silent, "demo-3")
	}, 30*time.Second, 200*time.Millisecond, "the replaced server did not answer")
	require.Never(t, func() bool {
		_, st, err := rc.state(t.Context())
		return err != nil || !slices.Equal([]string{"demo-3"}, st.NotInMeta) ||
			judgeGate(st) != gateState{GateSettled, "demo-3 not in the meta group"}
	}, 5*time.Second, time.Second, "the gate did not hold on demo-3 outside the meta group")
}
