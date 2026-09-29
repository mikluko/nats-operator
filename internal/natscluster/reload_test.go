package natscluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// TestReloadServer drives reloadServer through sysobs against a server
// running story 1's rendered config with a system user added.
func TestReloadServer(t *testing.T) {
	nc := limitedStoryCluster(t)
	m := renderedMap(t, nc, writeRouteCert(t, nc))
	m["accounts"] = map[string]any{"SYS": map[string]any{"users": []any{map[string]any{"user": "sys", "password": "sys"}}}}
	m["system_account"] = "SYS"
	f := filepath.Join(t.TempDir(), "nats.conf")
	require.NoError(t, os.WriteFile(f, encode(t, m), 0o600))
	o, err := server.ProcessConfigFile(f)
	require.NoError(t, err)
	o.NoLog, o.NoSigs = true, true
	s, err := server.NewServer(o)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	require.True(t, s.ReadyForConnections(10*time.Second))

	conn, err := nats.Connect(fmt.Sprintf("nats://sys:sys@%s", s.Addr()))
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	obs := sysobs.New(conn, "demo")
	snap := &sysobs.Snapshot{Servers: []sysobs.Server{{Name: "demo-0", ID: s.ID()}}}
	ctx := context.Background()

	rendered := func(mutate func(map[string]any)) (Server, []byte) {
		mutate(m)
		b := encode(t, m)
		return Server{Name: "demo-0", ConfigMap: &corev1.ConfigMap{Data: map[string]string{configFile: string(b)}}}, b
	}
	tagged, taggedFile := rendered(func(m map[string]any) { m["server_tags"] = []any{"az:b"} })

	t.Run("file not yet updated", func(t *testing.T) {
		applied, err := reloadServer(ctx, obs, snap, tagged, Certs{})
		require.NoError(t, err)
		require.False(t, applied)
	})
	t.Run("server unobserved", func(t *testing.T) {
		applied, err := reloadServer(ctx, obs, &sysobs.Snapshot{Silent: []string{"demo-0"}, Servers: snap.Servers}, tagged, Certs{})
		require.NoError(t, err)
		require.False(t, applied)
	})
	t.Run("reloads", func(t *testing.T) {
		require.NoError(t, os.WriteFile(f, taggedFile, 0o600))
		applied, err := reloadServer(ctx, obs, snap, tagged, Certs{})
		require.NoError(t, err)
		require.True(t, applied)
		require.Equal(t, []string{"az:b"}, varzTags(t, s))
	})
	t.Run("already loaded", func(t *testing.T) {
		applied, err := reloadServer(ctx, obs, snap, tagged, Certs{})
		require.NoError(t, err)
		require.True(t, applied)
	})
	t.Run("restart-only change fails", func(t *testing.T) {
		domain, domainFile := rendered(func(m map[string]any) { m["jetstream"].(map[string]any)["domain"] = "hub" })
		require.NoError(t, os.WriteFile(f, domainFile, 0o600))
		applied, err := reloadServer(ctx, obs, snap, domain, Certs{})
		require.ErrorIs(t, err, sysobs.ErrServer)
		require.False(t, applied)
	})
}

func varzTags(t *testing.T, s *server.Server) []string {
	t.Helper()
	v, err := s.Varz(nil)
	require.NoError(t, err)
	return v.Tags
}

// TestReloadServerCertRotation rotates the route certificate under a
// running server as kubelet refreshes a mounted Secret, apart from its
// ConfigMap, and pins that a reload is confirmed only once the server
// serves the rotated certificate.
func TestReloadServerCertRotation(t *testing.T) {
	nc := limitedStoryCluster(t)
	tlsDir := t.TempDir()
	first, err := selfSignedRouteSecret(nc, []string{"127.0.0.1"}, time.Now())
	require.NoError(t, err)
	writeSecretFiles(t, tlsDir, first)
	m := renderedMap(t, nc, tlsDir)
	m["accounts"] = map[string]any{"SYS": map[string]any{"users": []any{map[string]any{"user": "sys", "password": "sys"}}}}
	m["system_account"] = "SYS"
	f := filepath.Join(t.TempDir(), "nats.conf")
	require.NoError(t, os.WriteFile(f, encode(t, m), 0o600))
	o, err := server.ProcessConfigFile(f)
	require.NoError(t, err)
	o.NoLog, o.NoSigs = true, true
	s, err := server.NewServer(o)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	require.True(t, s.ReadyForConnections(10*time.Second))

	conn, err := nats.Connect(fmt.Sprintf("nats://sys:sys@%s", s.Addr()))
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	obs := sysobs.New(conn, "demo")
	snap := &sysobs.Snapshot{Servers: []sysobs.Server{{Name: "demo-0", ID: s.ID()}}}
	ctx := context.Background()
	routeAddr := s.ClusterAddr().String()
	require.Equal(t, leafSerial(t, first), servedSerial(t, routeAddr, tlsDir))

	rotated, err := selfSignedRouteSecret(nc, []string{"127.0.0.1"}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	certs := Certs{Routes: mountedCert(rotated)}
	require.NotEqual(t, mountedCert(first).NotAfter, certs.Routes.NotAfter)
	m["server_metadata"] = map[string]any{MetadataConfigRevision: "r2"}
	b := encode(t, m)
	next := Server{Name: "demo-0", ConfigMap: &corev1.ConfigMap{Data: map[string]string{configFile: string(b)}}}
	require.NoError(t, os.WriteFile(f, b, 0o600))

	t.Run("certificate not yet refreshed", func(t *testing.T) {
		applied, err := reloadServer(ctx, obs, snap, next, certs)
		require.NoError(t, err)
		require.False(t, applied)
		require.Equal(t, leafSerial(t, first), servedSerial(t, routeAddr, tlsDir))
	})
	t.Run("rotated certificate served", func(t *testing.T) {
		writeSecretFiles(t, tlsDir, rotated)
		applied, err := reloadServer(ctx, obs, snap, next, certs)
		require.NoError(t, err)
		require.True(t, applied)
		require.Equal(t, leafSerial(t, rotated), servedSerial(t, routeAddr, tlsDir))
	})
}

func writeSecretFiles(t *testing.T, dir string, s *corev1.Secret) {
	t.Helper()
	for k, v := range s.Data {
		require.NoError(t, os.WriteFile(filepath.Join(dir, k), v, 0o600))
	}
}

func leafSerial(t *testing.T, s *corev1.Secret) string {
	t.Helper()
	b, _ := pem.Decode(s.Data[corev1.TLSCertKey])
	require.NotNil(t, b)
	c, err := x509.ParseCertificate(b.Bytes)
	require.NoError(t, err)
	return c.SerialNumber.String()
}

// servedSerial is the serial of the certificate the route listener at addr
// serves, dialled with the client certificate in dir.
func servedSerial(t *testing.T, addr, dir string) string {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, corev1.TLSCertKey), filepath.Join(dir, corev1.TLSPrivateKeyKey))
	require.NoError(t, err)
	c, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}}) //nolint:gosec // the test reads the served certificate, it does not trust it
	require.NoError(t, err)
	serial := c.ConnectionState().PeerCertificates[0].SerialNumber.String()
	require.NoError(t, c.Close())
	return serial
}

// TestRunningVersion pins the tag read off each form the nats image takes.
func TestRunningVersion(t *testing.T) {
	const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name, image, want string
	}{
		{"tag", "nats:2.15.0", "2.15.0"},
		{"tag and digest", "nats:2.15.0@" + digest, "2.15.0"},
		{"registry port, tag and digest", "registry.example:5000/nats:2.14.1@" + digest, "2.14.1"},
		{"digest only", "nats@" + digest, ""},
		{"registry port, no tag", "registry.example:5000/nats", ""},
		{"unparseable", "NATS:", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{}
			sts.Spec.Template.Spec.Containers = []corev1.Container{{Name: "exporter", Image: "x:9"}, {Name: "nats", Image: tc.image}}
			require.Equal(t, tc.want, runningVersion(sts))
		})
	}
}

// TestChangeRestartReasonDigest pins the version pair named when a
// digest-pinned server's version changes.
func TestChangeRestartReasonDigest(t *testing.T) {
	nc := &clusterv1beta1.NatsCluster{}
	nc.Spec.Version = "2.15.0"
	nc.Spec.Image = &clusterv1beta1.Image{Digest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	cur := &appsv1.StatefulSet{}
	cur.Annotations = map[string]string{AnnotationSpecDigest: "old"}
	cur.Spec.Template.Spec.Containers = []corev1.Container{{Name: "nats", Image: "nats:2.14.1@" + nc.Spec.Image.Digest}}
	s := Server{StatefulSet: &appsv1.StatefulSet{}}
	s.StatefulSet.Annotations = map[string]string{AnnotationSpecDigest: "new"}
	require.Equal(t, "version 2.14.1 -> 2.15.0 is restart-only", changeRestartReason(nc, cur, &corev1.ConfigMap{}, s))
}

// reloadEnv is plan's servers created on a fake client, controlled by nc,
// with a reconciler reloading them through server.
type reloadEnv struct {
	c      client.Client
	r      *Reconciler
	server *fakeReloader
	names  []string
	snap   *sysobs.Snapshot
	nc     *clusterv1beta1.NatsCluster
}

func reloadFixture(t *testing.T, nc *clusterv1beta1.NatsCluster, plan *Plan) *reloadEnv {
	t.Helper()
	e := &reloadEnv{c: fake.NewClientBuilder().WithScheme(leafScheme(t)).Build(), snap: &sysobs.Snapshot{}, nc: nc}
	for _, s := range plan.Servers {
		for _, obj := range []client.Object{s.StatefulSet.DeepCopy(), s.ConfigMap.DeepCopy()} {
			require.NoError(t, controllerutil.SetControllerReference(nc, obj, e.c.Scheme()))
			require.NoError(t, e.c.Create(t.Context(), obj))
		}
		e.snap.Servers = append(e.snap.Servers, sysobs.Server{Name: s.Name, ID: s.Name, Metadata: map[string]string{}})
		e.names = append(e.names, s.Name)
	}
	e.server = &fakeReloader{c: e.c}
	e.server.reset(nc.Namespace)
	e.r = &Reconciler{Client: e.c, Now: func() time.Time { return time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC) },
		Reloader: func(context.Context, *clusterv1beta1.NatsCluster) (ServerReloader, error) { return e.server, nil }}
	return e
}

func (e *reloadEnv) statefulSets(t *testing.T) map[string]*appsv1.StatefulSet {
	t.Helper()
	sts := map[string]*appsv1.StatefulSet{}
	for _, name := range e.names {
		sts[name] = &appsv1.StatefulSet{}
		require.NoError(t, e.c.Get(t.Context(), types.NamespacedName{Namespace: e.nc.Namespace, Name: name}, sts[name]))
	}
	return sts
}

func (e *reloadEnv) apply(t *testing.T, plan *Plan) configApply {
	t.Helper()
	got, err := e.r.applyConfig(t.Context(), e.nc, plan, e.statefulSets(t), e.snap)
	require.NoError(t, err)
	return got
}

func (e *reloadEnv) configMap(t *testing.T, name string) *corev1.ConfigMap {
	t.Helper()
	cm := &corev1.ConfigMap{}
	require.NoError(t, e.c.Get(t.Context(), types.NamespacedName{Namespace: e.nc.Namespace, Name: configMapName(name)}, cm))
	return cm
}

// TestRestartServerErrors pins that restartServer names the server it
// restarts on a failed ConfigMap write and a failed StatefulSet write alike.
func TestRestartServerErrors(t *testing.T) {
	nc := storyCluster(t)
	nc.UID = "demo-uid"
	a, err := Render(nc, Inputs{})
	require.NoError(t, err)
	s := a.Servers[0]
	refused := errors.New("refused")
	for _, tt := range []struct {
		name string
		fail client.Object
	}{
		{name: "configmap", fail: &corev1.ConfigMap{}},
		{name: "statefulset", fail: &appsv1.StatefulSet{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := reloadFixture(t, nc, a)
			cur := e.statefulSets(t)[s.Name]
			e.r.Client = interceptor.NewClient(e.c.(client.WithWatch), interceptor.Funcs{
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if reflect.TypeOf(obj) == reflect.TypeOf(tt.fail) {
						return refused
					}
					return c.Update(ctx, obj, opts...)
				},
			})
			_, err := e.r.restartServer(t.Context(), nc, s, cur, "tls changed")
			require.ErrorIs(t, err, refused)
			require.ErrorContains(t, err, "restart server "+s.Name+": ")
		})
	}
}

// TestApplyConfigRevertDuringPendingReload pins that a server whose config
// returns to its StatefulSet's revision while its reload to another is
// unconfirmed gets that revision written back to its ConfigMap and reloaded.
func TestApplyConfigRevertDuringPendingReload(t *testing.T) {
	ctx := t.Context()
	nc := storyCluster(t)
	nc.UID = "demo-uid"
	a, err := Render(nc, Inputs{})
	require.NoError(t, err)
	retagged := nc.DeepCopy()
	retagged.Spec.ServerTags = map[string]string{"az": "b"}
	b, err := Render(retagged, Inputs{})
	require.NoError(t, err)
	require.NotEqual(t, a.Revision, b.Revision)

	e := reloadFixture(t, nc, a)
	e.server.lag = true

	require.ElementsMatch(t, e.names, e.apply(t, b).Reloading)
	for _, name := range e.names {
		require.Equal(t, b.Revision, e.configMap(t, name).Annotations[AnnotationConfigRevision])
	}

	calls := len(e.server.reloaded())
	got := e.apply(t, a)
	require.ElementsMatch(t, e.names, got.Reloading)
	require.Empty(t, got.Restart)
	require.Len(t, e.server.reloaded(), calls+len(e.names), "the reverted revision was not reloaded")
	for _, s := range a.Servers {
		cm := e.configMap(t, s.Name)
		require.Equal(t, a.Revision, cm.Annotations[AnnotationConfigRevision], s.Name)
		require.Equal(t, s.ConfigMap.Data, cm.Data, s.Name)
	}

	e.server.lag = false
	got = e.apply(t, a)
	require.ElementsMatch(t, e.names, got.Reloaded)
	require.Empty(t, got.Reloading)
	for _, s := range a.Servers {
		want, err := configDigest([]byte(s.ConfigMap.Data[configFile]))
		require.NoError(t, err)
		state, err := e.server.Config(ctx, s.Name)
		require.NoError(t, err)
		require.Equal(t, want, state.Digest, s.Name)
		sts := &appsv1.StatefulSet{}
		require.NoError(t, e.c.Get(ctx, client.ObjectKeyFromObject(s.StatefulSet), sts))
		require.Equal(t, a.Revision, sts.Annotations[AnnotationConfigRevision], s.Name)
	}

	calls = len(e.server.reloaded())
	require.Empty(t, e.apply(t, a))
	require.Len(t, e.server.reloaded(), calls, "a confirmed revision was reloaded again")
}

// TestApplyConfigRecreatesConfigMap pins that a server on the plan's
// revision whose ConfigMap was deleted gets it back with the plan's data and
// is reloaded until its server confirms that data.
func TestApplyConfigRecreatesConfigMap(t *testing.T) {
	nc := storyCluster(t)
	nc.UID = "demo-uid"
	a, err := Render(nc, Inputs{})
	require.NoError(t, err)
	e := reloadFixture(t, nc, a)
	e.server.lag = true
	require.Empty(t, e.apply(t, a))

	gone := a.Servers[0]
	require.NoError(t, e.c.Delete(t.Context(), gone.ConfigMap.DeepCopy()))

	got := e.apply(t, a)
	require.Equal(t, []string{gone.Name}, got.Reloading)
	require.Empty(t, got.Restart)
	cm := e.configMap(t, gone.Name)
	require.Equal(t, gone.ConfigMap.Data, cm.Data)
	require.Equal(t, a.Revision, cm.Annotations[AnnotationConfigRevision])
	require.True(t, metav1.IsControlledBy(cm, nc))
	require.NotEqual(t, a.Revision, e.statefulSets(t)[gone.Name].Annotations[AnnotationConfigRevision])

	require.Equal(t, []string{gone.Name}, e.apply(t, a).Reloading, "an unconfirmed reload was dropped")

	e.server.lag = false
	require.Equal(t, []string{gone.Name}, e.apply(t, a).Reloaded)
	require.Equal(t, a.Revision, e.statefulSets(t)[gone.Name].Annotations[AnnotationConfigRevision])
	require.Empty(t, e.apply(t, a))
}

// TestRestartServerToTemplateRevision pins that a restart fallback for a
// reload back to the revision the pod template already names still changes
// the template, and that the gate holds until the server reports that
// revision.
func TestRestartServerToTemplateRevision(t *testing.T) {
	ctx := t.Context()
	nc := storyCluster(t)
	nc.UID = "demo-uid"
	a, err := Render(nc, Inputs{})
	require.NoError(t, err)
	retagged := nc.DeepCopy()
	retagged.Spec.ServerTags = map[string]string{"az": "b"}
	b, err := Render(retagged, Inputs{})
	require.NoError(t, err)

	e := reloadFixture(t, nc, a)
	report := func(revision string) {
		for i := range e.snap.Servers {
			e.snap.Servers[i].Metadata[MetadataConfigRevision] = revision
		}
	}

	require.ElementsMatch(t, e.names, e.apply(t, b).Reloaded)
	report(b.Revision)

	e.server.reject = errors.New("refused")
	back := e.apply(t, a)
	require.ElementsMatch(t, e.names, slices.Collect(maps.Keys(back.Restart)))

	step := a.Servers[0]
	cur := e.statefulSets(t)[step.Name]
	require.Equal(t, a.Revision, cur.Spec.Template.Annotations[AnnotationConfigRevision], "the pod template left revision A")
	for gen := 1; gen <= 2; gen++ {
		was := cur.Spec.Template.DeepCopy()
		_, err := e.r.restartServer(ctx, nc, step, cur, back.Restart[step.Name])
		require.NoError(t, err)
		cur = e.statefulSets(t)[step.Name]
		require.NotEqual(t, was, &cur.Spec.Template, "the restart left the pod template unchanged")
		require.Equal(t, strconv.Itoa(gen), cur.Spec.Template.Annotations[AnnotationRestartGeneration])
	}

	gate := func(t *testing.T) gateState {
		t.Helper()
		sets := e.statefulSets(t)
		for _, sts := range sets {
			sts.Status = appsv1.StatefulSetStatus{ObservedGeneration: sts.Generation, UpdatedReplicas: 1, ReadyReplicas: 1}
		}
		return judgeGate(e.r.rolloutState(nc, a, Observed{StatefulSets: sets, Snapshot: e.snap, Apply: e.apply(t, a)}))
	}
	g := gate(t)
	require.Equal(t, GateTargetRevision, g.waitingFor)
	require.Contains(t, g.detail, step.Name)

	report(a.Revision)
	require.True(t, gate(t).open())
}

// TestServerConfigMapWrites pins what each write of a server's ConfigMap
// leaves on it, whether or not the ConfigMap exists: the rendered labels over
// any it did not render, and the revision and apply annotations its caller
// asks for.
func TestServerConfigMapWrites(t *testing.T) {
	nc := storyCluster(t)
	nc.UID = "demo-uid"
	a, err := Render(nc, Inputs{})
	require.NoError(t, err)
	s := a.Servers[0]

	create := func(e *reloadEnv) error {
		_, err := e.r.createServer(t.Context(), nc, s)
		return err
	}
	recreate := func(e *reloadEnv) error {
		_, err := e.r.applyServerConfigMap(t.Context(), nc, s, dropRevision)
		return err
	}
	restart := func(e *reloadEnv) error {
		_, err := e.r.restartServer(t.Context(), nc, s, e.statefulSets(t)[s.Name], "tls changed")
		return err
	}
	for _, tt := range []struct {
		name      string
		missing   bool
		write     func(e *reloadEnv) error
		wantApply string
		wantRev   string
	}{
		{name: "create", missing: true, wantRev: a.Revision, write: create},
		{name: "create over a leftover", wantRev: a.Revision, write: create},
		{name: "recreate for reload", missing: true, write: recreate},
		{name: "restart", wantApply: string(clusterv1beta1.ConfigAppliedByRestart), wantRev: a.Revision, write: restart},
		{name: "restart without a configmap", missing: true, wantApply: string(clusterv1beta1.ConfigAppliedByRestart), wantRev: a.Revision, write: restart},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := reloadFixture(t, nc, a)
			cm := e.configMap(t, s.Name)
			if tt.missing {
				require.NoError(t, e.c.Delete(t.Context(), cm))
			} else {
				cm.Labels["backup.example.com/skip"] = "true"
				cm.Labels[LabelServer] = "stale"
				cm.Annotations[AnnotationReloadSince] = "2026-09-29T11:00:00Z"
				require.NoError(t, e.c.Update(t.Context(), cm))
			}

			require.NoError(t, tt.write(e))
			cm = e.configMap(t, s.Name)
			require.True(t, metav1.IsControlledBy(cm, nc))
			if !tt.missing {
				require.Equal(t, "true", cm.Labels["backup.example.com/skip"])
			}
			require.Equal(t, s.Name, cm.Labels[LabelServer])
			require.Equal(t, s.ConfigMap.Data, cm.Data)
			require.Equal(t, tt.wantRev, cm.Annotations[AnnotationConfigRevision])
			require.Equal(t, tt.wantApply, cm.Annotations[AnnotationConfigApply])
			if tt.wantApply != "" {
				require.Equal(t, "tls changed", cm.Annotations[AnnotationRestartReason])
				require.NotContains(t, cm.Annotations, AnnotationReloadSince)
			}
		})
	}
}
