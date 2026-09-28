package natscluster

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/utils/ptr"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// Images the pods run unless spec.image overrides the NATS repository.
const (
	DefaultImage  = "nats"
	ExporterImage = "natsio/prometheus-nats-exporter:0.17.3"
)

// A server in lame-duck mode hands off every Raft leadership it holds, tells
// its clients after lameDuckGracePeriod, spreads their disconnects over the
// rest of lameDuckDuration and exits; terminationGracePeriod, in seconds,
// must outlast all of it.
const (
	terminationGracePeriod = 300
	lameDuckDuration       = 2 * time.Minute
	lameDuckGracePeriod    = 10 * time.Second
)

// podUser is the user and group every container of a server's pod runs as,
// and the group owning its volumes: the images run as root unless told
// otherwise.
const podUser = 1000

// Server is one server's rendered objects.
type Server struct {
	Name        string
	ConfigMap   *corev1.ConfigMap
	StatefulSet *appsv1.StatefulSet
}

// Plan is everything the cluster controller renders for a NatsCluster
// except route certificates.
type Plan struct {
	// Revision is the config revision: a digest of every server's config
	// and StatefulSet, the revision itself and the volume claim templates
	// excluded, and of the TLS Secrets they mount.
	Revision string
	Limits   Limits
	// Certs are the TLS Secrets the plan's servers mount.
	Certs Certs
	// LeafRemotes are the resolved remotes the plan was rendered with.
	LeafRemotes []LeafRemote

	Servers         []Server
	HeadlessService *corev1.Service
	ClientService   *corev1.Service
	// GatewayService is nil unless gateway.service is set.
	GatewayService *corev1.Service
	PDB            *policyv1.PodDisruptionBudget
	// NetworkPolicy is nil when monitor.networkPolicy is false.
	NetworkPolicy *networkingv1.NetworkPolicy
}

// Render renders nc from in, with remotes, nc's leafRemotes resolved. It
// fails only on a podTemplate that does not merge into the rendered pod.
func Render(nc *clusterv1beta1.NatsCluster, in Inputs, remotes ...LeafRemote) (*Plan, error) {
	p := &Plan{
		LeafRemotes:     remotes,
		Certs:           in.Certs,
		Limits:          deriveLimits(&nc.Spec),
		HeadlessService: headlessService(nc),
		ClientService:   clientService(nc),
		GatewayService:  gatewayService(nc),
		PDB:             pdb(nc),
		NetworkPolicy:   networkPolicy(nc, in.MonitorNamespace),
	}
	layout := podLayout(nc)
	h := sha256.New()
	specDigests, volumeDigests := map[string]string{}, map[string]string{}
	for _, name := range serverNames(nc) {
		cfg, err := serverConfig(nc, in, name, layout, "", remotes...).Render()
		if err != nil {
			return nil, err
		}
		sts, err := statefulSet(nc, name, p.Limits)
		if err != nil {
			return nil, err
		}
		spec := sts.Spec
		spec.VolumeClaimTemplates = nil
		stsJSON, err := json.Marshal(spec)
		if err != nil {
			return nil, err
		}
		h.Write(cfg)
		h.Write(stsJSON)
		sum := sha256.Sum256(stsJSON)
		specDigests[name] = hex.EncodeToString(sum[:])[:10]
		if volumeDigests[name], err = volumeDigest(sts); err != nil {
			return nil, err
		}
		p.Servers = append(p.Servers, Server{Name: name, StatefulSet: sts})
	}
	for _, c := range []MountedCert{in.Certs.Client, in.Certs.Routes, in.Certs.Gateway, in.Certs.Leafnodes} {
		h.Write([]byte(c.Digest))
	}
	p.Revision = hex.EncodeToString(h.Sum(nil))[:10]
	for i := range p.Servers {
		s := &p.Servers[i]
		cfg, err := serverConfig(nc, in, s.Name, layout, p.Revision, remotes...).Render()
		if err != nil {
			return nil, err
		}
		s.ConfigMap = configMap(nc, s.Name, cfg, p.Revision)
		s.StatefulSet.Annotations = map[string]string{
			AnnotationConfigRevision: p.Revision,
			AnnotationSpecDigest:     specDigests[s.Name],
			AnnotationVolumeDigest:   volumeDigests[s.Name],
		}
		tmpl := &s.StatefulSet.Spec.Template
		tmpl.Annotations = merged(tmpl.Annotations, map[string]string{AnnotationConfigRevision: p.Revision})
	}
	return p, nil
}

// volumeDigest is a digest of sts's volume claim templates, "" when it has
// none.
func volumeDigest(sts *appsv1.StatefulSet) (string, error) {
	if len(sts.Spec.VolumeClaimTemplates) == 0 {
		return "", nil
	}
	b, err := json.Marshal(sts.Spec.VolumeClaimTemplates)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:10], nil
}

func configMap(nc *clusterv1beta1.NatsCluster, server string, cfg []byte, revision string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        configMapName(server),
			Namespace:   nc.Namespace,
			Labels:      serverLabels(nc, server),
			Annotations: map[string]string{AnnotationConfigRevision: revision},
		},
		Data: map[string]string{configFile: string(cfg)},
	}
}

func image(nc *clusterv1beta1.NatsCluster) string {
	repo := nc.Spec.Image
	if repo == "" {
		repo = DefaultImage
	}
	return repo + ":" + nc.Spec.Version
}

// statefulSet renders server's one-replica StatefulSet.
func statefulSet(nc *clusterv1beta1.NatsCluster, server string, limits Limits) (*appsv1.StatefulSet, error) {
	pod, err := podTemplate(nc, server, limits)
	if err != nil {
		return nil, err
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: server, Namespace: nc.Namespace, Labels: serverLabels(nc, server)},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            ptr.To[int32](1),
			ServiceName:         headlessServiceName(nc),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: serverSelector(nc, server)},
			Template:            pod,
		},
	}
	if js := nc.Spec.JetStream; js != nil && js.VolumeClaimTemplate != nil {
		vct := js.VolumeClaimTemplate
		pvc := corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "data",
				Labels:      merged(vct.Metadata.Labels, serverSelector(nc, server)),
				Annotations: vct.Metadata.Annotations,
			},
			Spec: *vct.Spec.DeepCopy(),
		}
		if len(pvc.Spec.AccessModes) == 0 {
			pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		}
		sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}
	}
	return sts, nil
}

func podTemplate(nc *clusterv1beta1.NatsCluster, server string, limits Limits) (corev1.PodTemplateSpec, error) {
	t := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: serverLabels(nc, server)},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: ptr.To[int64](terminationGracePeriod),
			AutomountServiceAccountToken:  ptr.To(false),
			SecurityContext:               podSecurityContext(),
			Containers:                    []corev1.Container{natsContainer(nc, limits)},
			Volumes:                       volumes(nc, server),
		},
	}
	if exporterEnabled(&nc.Spec) {
		t.Spec.Containers = append(t.Spec.Containers, exporterContainer())
	}
	pt := nc.Spec.PodTemplate
	if pt == nil {
		return t, nil
	}
	t.Labels = merged(pt.Metadata.Labels, t.Labels)
	t.Annotations = merged(pt.Metadata.Annotations, nil)
	if pt.Spec != nil {
		spec, err := mergePodSpec(t.Spec, pt.Spec)
		if err != nil {
			return t, err
		}
		t.Spec = spec
	}
	return t, nil
}

// podSecurityContext and containerSecurityContext meet the restricted Pod
// Security Standard.
func podSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot:        ptr.To(true),
		RunAsUser:           ptr.To[int64](podUser),
		RunAsGroup:          ptr.To[int64](podUser),
		FSGroup:             ptr.To[int64](podUser),
		FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch),
		SeccompProfile:      &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

func containerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// mergePodSpec strategic-merges override over base. A field override does
// not set is left as base has it.
func mergePodSpec(base corev1.PodSpec, override *corev1.PodSpec) (corev1.PodSpec, error) {
	baseJSON, err := json.Marshal(base)
	if err != nil {
		return base, err
	}
	patch, err := setFieldsJSON(override)
	if err != nil {
		return base, err
	}
	out, err := strategicpatch.StrategicMergePatch(baseJSON, patch, corev1.PodSpec{})
	if err != nil {
		return base, fmt.Errorf("merge podTemplate.spec: %w", err)
	}
	var spec corev1.PodSpec
	if err := json.Unmarshal(out, &spec); err != nil {
		return base, fmt.Errorf("merge podTemplate.spec: %w", err)
	}
	return spec, nil
}

// setFieldsJSON encodes v with every null dropped, so that a field v leaves
// unset does not delete it in a merge patch.
func setFieldsJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return json.Marshal(dropNulls(m))
}

func dropNulls(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, e := range t {
			if e == nil {
				delete(t, k)
				continue
			}
			t[k] = dropNulls(e)
		}
	case []any:
		for i, e := range t {
			t[i] = dropNulls(e)
		}
	}
	return v
}

func natsContainer(nc *clusterv1beta1.NatsCluster, limits Limits) corev1.Container {
	c := corev1.Container{
		Name:            "nats",
		Image:           image(nc),
		Args:            []string{"--config", configDir + "/" + configFile},
		Resources:       *nc.Spec.Resources.DeepCopy(),
		SecurityContext: containerSecurityContext(),
		Ports: []corev1.ContainerPort{
			{Name: "client", ContainerPort: PortClient},
			{Name: "route", ContainerPort: PortRoute},
			{Name: "monitor", ContainerPort: PortMonitor},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "config", MountPath: configDir},
			{Name: "pid", MountPath: pidDir},
		},
		StartupProbe: &corev1.Probe{
			ProbeHandler:     healthz("/healthz"),
			PeriodSeconds:    10,
			FailureThreshold: 90,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:     healthz("/healthz?js-server-only=true"),
			PeriodSeconds:    10,
			FailureThreshold: 3,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:     healthz("/healthz?js-enabled-only=true"),
			PeriodSeconds:    30,
			FailureThreshold: 3,
		},
		Lifecycle: &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{
			Exec: &corev1.ExecAction{Command: []string{"/nats-server", "--signal", "ldm=" + pidDir + "/nats.pid"}},
		}},
	}
	if limits.GoMemLimit > 0 {
		c.Env = append(c.Env, corev1.EnvVar{Name: "GOMEMLIMIT", Value: strconv.FormatInt(limits.GoMemLimit, 10)})
	}
	if hasData(nc) {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "data", MountPath: dataDir})
	}
	if clientCertSecret(nc) != "" {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "client-tls", MountPath: clientTLSDir, ReadOnly: true})
	}
	if routesSecret(nc) != "" {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "routes-tls", MountPath: routesTLSDir, ReadOnly: true})
	}
	if nc.Spec.Gateway != nil {
		c.Ports = append(c.Ports, corev1.ContainerPort{Name: "gateway", ContainerPort: PortGateway})
	}
	if gatewaySecret(nc) != "" {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "gateway-tls", MountPath: gatewayTLSDir, ReadOnly: true})
	}
	addLeafnodesListener(nc, &c)
	return c
}

func healthz(path string) corev1.ProbeHandler {
	return corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString("monitor")}}
}

// exporterEnabled reports whether servers run the exporter sidecar: on
// unless exporter.enabled is false.
func exporterEnabled(spec *clusterv1beta1.NatsClusterSpec) bool {
	return spec.Exporter == nil || spec.Exporter.Enabled == nil || *spec.Exporter.Enabled
}

func exporterContainer() corev1.Container {
	return corev1.Container{
		Name:  "exporter",
		Image: ExporterImage,
		Args: []string{
			"-port=" + strconv.Itoa(PortMetrics),
			"-connz", "-routez", "-subz", "-varz", "-healthz", "-jsz=all",
			fmt.Sprintf("http://localhost:%d", PortMonitor),
		},
		Ports:           []corev1.ContainerPort{{Name: "metrics", ContainerPort: PortMetrics}},
		SecurityContext: containerSecurityContext(),
	}
}

func volumes(nc *clusterv1beta1.NatsCluster, server string) []corev1.Volume {
	vs := []corev1.Volume{
		configVolume(nc, server),
		{Name: "pid", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	vs = append(vs, leafnodesVolumes(nc)...)
	if s := clientCertSecret(nc); s != "" {
		vs = append(vs, corev1.Volume{Name: "client-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: s}}})
	}
	if s := routesSecret(nc); s != "" {
		vs = append(vs, corev1.Volume{Name: "routes-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: s}}})
	}
	if s := gatewaySecret(nc); s != "" {
		vs = append(vs, corev1.Volume{Name: "gateway-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: s}}})
	}
	if hasData(nc) && (nc.Spec.JetStream == nil || nc.Spec.JetStream.VolumeClaimTemplate == nil) {
		vs = append(vs, corev1.Volume{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
	}
	return vs
}

// hasData reports whether a server mounts the data volume, which holds the
// JetStream store and the account resolver's directory: an emptyDir unless
// jetstream.volumeClaimTemplate is given.
func hasData(nc *clusterv1beta1.NatsCluster) bool {
	return nc.Spec.JetStream != nil || nc.Spec.Auth != nil
}

func headlessService(nc *clusterv1beta1.NatsCluster) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: headlessServiceName(nc), Namespace: nc.Namespace, Labels: labels(nc)},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 clusterSelector(nc),
			Ports: []corev1.ServicePort{
				servicePort("client", PortClient),
				servicePort("route", PortRoute),
				servicePort("monitor", PortMonitor),
			},
		},
	}
	if exporterEnabled(&nc.Spec) {
		svc.Spec.Ports = append(svc.Spec.Ports, servicePort("metrics", PortMetrics))
	}
	if nc.Spec.Gateway != nil {
		svc.Spec.Ports = append(svc.Spec.Ports, servicePort("gateway", PortGateway))
	}
	return svc
}

// gatewayService renders gateway.service over every server's gateway
// port, or nil when it is unset. It serves servers that are not Ready: a
// JetStream server is Ready once it reaches a meta leader, which a
// supercluster elects over these gateways.
func gatewayService(nc *clusterv1beta1.NatsCluster) *corev1.Service {
	g := nc.Spec.Gateway
	if g == nil || g.Service == nil {
		return nil
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        gatewayServiceName(nc),
			Namespace:   nc.Namespace,
			Labels:      labels(nc),
			Annotations: maps.Clone(g.Service.Annotations),
		},
		Spec: corev1.ServiceSpec{
			Type:                     g.Service.Type,
			Selector:                 clusterSelector(nc),
			Ports:                    []corev1.ServicePort{servicePort("gateway", PortGateway)},
			PublishNotReadyAddresses: true,
		},
	}
}

// clientService serves the client port alone: the monitoring port has no
// authentication and answers for every account.
func clientService(nc *clusterv1beta1.NatsCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: clientServiceName(nc), Namespace: nc.Namespace, Labels: labels(nc)},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: clusterSelector(nc),
			Ports:    []corev1.ServicePort{servicePort("client", PortClient)},
		},
	}
}

func servicePort(name string, port int32) corev1.ServicePort {
	return corev1.ServicePort{Name: name, Port: port, TargetPort: intstr.FromString(name), Protocol: corev1.ProtocolTCP}
}

func pdb(nc *clusterv1beta1.NatsCluster) *policyv1.PodDisruptionBudget {
	one := intstr.FromInt32(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: nc.Name, Namespace: nc.Namespace, Labels: labels(nc)},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &one,
			Selector:       &metav1.LabelSelector{MatchLabels: clusterSelector(nc)},
		},
	}
}

// merged returns a copy of base with over's entries on top.
func merged(base, over map[string]string) map[string]string {
	if len(base) == 0 && len(over) == 0 {
		return nil
	}
	out := maps.Clone(base)
	if out == nil {
		out = map[string]string{}
	}
	maps.Copy(out, over)
	return out
}
