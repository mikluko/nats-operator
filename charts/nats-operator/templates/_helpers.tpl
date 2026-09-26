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
nats-operator.controller renders one controller's ServiceAccount, RBAC and
Deployment. It takes a dict of root (the chart context), name (the
controller's name, which is also its binary and image), group (its API group,
which is also its leader election lease), values (its block of values),
rules (its ClusterRole rules as YAML) and, optionally, args (flags appended to
the controller's own).
*/}}
{{- define "nats-operator.controller" -}}
{{- $fullname := include "nats-operator.fullname" . -}}
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
          ports:
            - name: metrics
              containerPort: 8080
            - name: probes
              containerPort: 8081
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
{{- end }}
