{{/*
nats-operator.labels takes a dict of root (the chart context) and name (the
controller's name).
*/}}
{{- define "nats-operator.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version | replace "+" "_" }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
{{ include "nats-operator.selectorLabels" . }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
{{- end }}

{{- define "nats-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ .root.Chart.Name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .name }}
{{- end }}

{{- define "nats-operator.fullname" -}}
{{ printf "%s-%s" .root.Release.Name .name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
nats-operator.controller renders one controller's ServiceAccount, RBAC,
Deployment and, with telemetry.prometheus.enabled, its Prometheus Service and
ServiceMonitor. It takes a dict of root (the chart context), name (the
controller's name, which is also its binary and image), group (its API group,
which is also its leader election lease), values (its block of values),
rules (its ClusterRole rules as YAML) and, optionally, args (flags appended to
the controller's own).
*/}}
{{- define "nats-operator.controller" -}}
{{- $fullname := include "nats-operator.fullname" . -}}
{{- $telemetry := .root.Values.telemetry -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ $fullname }}
  namespace: {{ .root.Release.Namespace }}
  labels:
    {{- include "nats-operator.labels" . | nindent 4 }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ $fullname }}
  labels:
    {{- include "nats-operator.labels" . | nindent 4 }}
rules:
  {{- .rules | nindent 2 }}
  - apiGroups: [""]
    resources: [events]
    verbs: [create, patch]
  - apiGroups: [events.k8s.io]
    resources: [events]
    verbs: [create, patch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ $fullname }}
  labels:
    {{- include "nats-operator.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ $fullname }}
subjects:
  - kind: ServiceAccount
    name: {{ $fullname }}
    namespace: {{ .root.Release.Namespace }}
{{- if .root.Values.leaderElection.enabled }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ $fullname }}-leader-election
  namespace: {{ .root.Release.Namespace }}
  labels:
    {{- include "nats-operator.labels" . | nindent 4 }}
rules:
  - apiGroups: [coordination.k8s.io]
    resources: [leases]
    verbs: [get, list, watch, create, update, patch, delete]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ $fullname }}-leader-election
  namespace: {{ .root.Release.Namespace }}
  labels:
    {{- include "nats-operator.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ $fullname }}-leader-election
subjects:
  - kind: ServiceAccount
    name: {{ $fullname }}
    namespace: {{ .root.Release.Namespace }}
{{- end }}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ $fullname }}
  namespace: {{ .root.Release.Namespace }}
  labels:
    {{- include "nats-operator.labels" . | nindent 4 }}
spec:
  replicas: {{ .values.replicas }}
  selector:
    matchLabels:
      {{- include "nats-operator.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "nats-operator.labels" . | nindent 8 }}
      {{- if $telemetry.collector.enabled }}
      annotations:
        checksum/otel-collector: {{ toYaml $telemetry.collector.config | sha256sum }}
      {{- end }}
    spec:
      serviceAccountName: {{ $fullname }}
      {{- with .root.Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      securityContext:
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      {{- if $telemetry.collector.enabled }}
      initContainers:
        - name: otel-collector
          image: {{ printf "%s:%s" $telemetry.collector.image.repository $telemetry.collector.image.tag | quote }}
          imagePullPolicy: {{ $telemetry.collector.image.pullPolicy }}
          restartPolicy: Always
          args:
            - --config=/etc/otel-collector/config.yaml
          volumeMounts:
            - name: otel-collector
              mountPath: /etc/otel-collector
              readOnly: true
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: [ALL]
          {{- with $telemetry.collector.resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
      volumes:
        - name: otel-collector
          configMap:
            name: {{ include "nats-operator.fullname" (dict "root" .root "name" "otel-collector") }}
      {{- end }}
      containers:
        - name: {{ .name }}
          image: {{ printf "%s:%s" .values.image.repository (.values.image.tag | default .root.Chart.AppVersion) | quote }}
          imagePullPolicy: {{ .values.image.pullPolicy }}
          args:
            - --leader-elect={{ .root.Values.leaderElection.enabled }}
            - --leader-election-id={{ .group }}
            - --metrics-bind-address=:8080
            - --health-probe-bind-address=:8081
            {{- range .args }}
            - {{ . | quote }}
            {{- end }}
          {{- if or $telemetry.collector.enabled $telemetry.prometheus.enabled $telemetry.env }}
          env:
            {{- if $telemetry.collector.enabled }}
            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: http://localhost:4318
            {{- end }}
            {{- if $telemetry.prometheus.enabled }}
            - name: OTEL_METRICS_EXPORTER
              value: prometheus
            - name: OTEL_EXPORTER_PROMETHEUS_HOST
              value: 0.0.0.0
            - name: OTEL_EXPORTER_PROMETHEUS_PORT
              value: {{ $telemetry.prometheus.port | quote }}
            {{- end }}
            {{- with $telemetry.env }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
          {{- end }}
          ports:
            - name: metrics
              containerPort: 8080
            - name: probes
              containerPort: 8081
            {{- if $telemetry.prometheus.enabled }}
            - name: prometheus
              containerPort: {{ $telemetry.prometheus.port }}
            {{- end }}
          livenessProbe:
            httpGet:
              path: /healthz
              port: probes
          readinessProbe:
            httpGet:
              path: /readyz
              port: probes
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: [ALL]
          {{- with .values.resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
{{- if $telemetry.prometheus.enabled }}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ $fullname }}-prometheus
  namespace: {{ .root.Release.Namespace }}
  labels:
    {{- include "nats-operator.labels" . | nindent 4 }}
spec:
  selector:
    {{- include "nats-operator.selectorLabels" . | nindent 4 }}
  ports:
    - name: prometheus
      port: {{ $telemetry.prometheus.port }}
      targetPort: prometheus
{{- if $telemetry.prometheus.serviceMonitor.enabled }}
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ $fullname }}
  namespace: {{ .root.Release.Namespace }}
  labels:
    {{- include "nats-operator.labels" . | nindent 4 }}
    {{- with $telemetry.prometheus.serviceMonitor.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  selector:
    matchLabels:
      {{- include "nats-operator.selectorLabels" . | nindent 6 }}
  endpoints:
    - port: prometheus
{{- end }}
{{- end }}
{{- end }}

{{/*
nats-operator.test renders one controller's `helm test` hook: a Service on its
probes port and a Pod that GETs /healthz through it and, with
telemetry.prometheus.enabled, /metrics through the Prometheus Service. The Pod
tries each URL 30 times, two seconds apart, before it fails. It takes a dict
of root (the chart context) and name (the controller's name).
*/}}
{{- define "nats-operator.test" -}}
{{- $fullname := include "nats-operator.fullname" . -}}
{{- $telemetry := .root.Values.telemetry -}}
{{- $image := .root.Values.tests.image -}}
{{- $ns := .root.Release.Namespace -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ $fullname }}-test
  namespace: {{ $ns }}
  labels:
    {{- include "nats-operator.labels" . | nindent 4 }}
  annotations:
    helm.sh/hook: test
    helm.sh/hook-weight: "-1"
    helm.sh/hook-delete-policy: before-hook-creation
spec:
  selector:
    {{- include "nats-operator.selectorLabels" . | nindent 4 }}
  ports:
    - name: probes
      port: 8081
      targetPort: probes
---
apiVersion: v1
kind: Pod
metadata:
  name: {{ $fullname }}-test
  namespace: {{ $ns }}
  labels:
    {{- include "nats-operator.labels" . | nindent 4 }}
  annotations:
    helm.sh/hook: test
    helm.sh/hook-delete-policy: before-hook-creation
spec:
  restartPolicy: Never
  {{- with .root.Values.imagePullSecrets }}
  imagePullSecrets:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  securityContext:
    runAsNonRoot: true
    runAsUser: 65534
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: check
      image: {{ printf "%s:%s" $image.repository $image.tag | quote }}
      imagePullPolicy: {{ $image.pullPolicy }}
      command:
        - sh
        - -c
        - |
          for url in "$@"; do
            n=0
            until wget -q -O /dev/null -T 5 "$url"; do
              n=$((n + 1))
              [ "$n" -lt 30 ] || { echo "FAIL $url"; exit 1; }
              sleep 2
            done
            echo "ok $url"
          done
        - check
        - http://{{ $fullname }}-test.{{ $ns }}.svc:8081/healthz
        {{- if $telemetry.prometheus.enabled }}
        - http://{{ $fullname }}-prometheus.{{ $ns }}.svc:{{ $telemetry.prometheus.port }}/metrics
        {{- end }}
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: [ALL]
{{- end }}
