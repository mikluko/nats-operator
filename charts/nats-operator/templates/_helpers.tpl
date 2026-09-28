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
nats-operator.image takes a dict of image (a block of image values) and tag,
and returns repository:tag, followed by @digest where the block sets one.
*/}}
{{- define "nats-operator.image" -}}
{{ printf "%s:%s" .image.repository .tag }}{{ with .image.digest }}@{{ . }}{{ end }}
{{- end }}

{{/*
nats-operator.merged takes a list of a global map and a controller's map and
returns, as YAML, the global map with each of the controller's top-level keys
set over it: a key's value is taken whole, never merged below the top level.
Both empty, it returns nothing.
*/}}
{{- define "nats-operator.merged" -}}
{{- $out := dict -}}
{{- range $m := . -}}
{{- range $k, $v := $m -}}
{{- $_ := set $out $k $v -}}
{{- end -}}
{{- end -}}
{{- if $out -}}
{{- toYaml $out -}}
{{- end -}}
{{- end }}

{{/*
nats-operator.env takes a list of a global env and a controller's env and
returns, as YAML, the global entries the controller's does not name followed
by the controller's. Both empty, it returns nothing.
*/}}
{{- define "nats-operator.env" -}}
{{- $global := index . 0 -}}
{{- $own := index . 1 -}}
{{- $names := list -}}
{{- range $own -}}
{{- $names = append $names .name -}}
{{- end -}}
{{- $out := list -}}
{{- range $global -}}
{{- if not (has .name $names) -}}
{{- $out = append $out . -}}
{{- end -}}
{{- end -}}
{{- $out = concat $out $own -}}
{{- if $out -}}
{{- toYaml $out -}}
{{- end -}}
{{- end }}

{{/*
nats-operator.controller renders one controller's ServiceAccount, RBAC and
Deployment. It takes a dict of root (the chart context), name (the
controller's, and its image's), group (its API group and lease), values (its
block of values) and, optionally, args (flags appended to the controller's
own, before extraArgs). Its ClusterRole's rules are those of
files/rbac/<name>.yaml, a copy of the role controller-gen generates for it.
With watchNamespaces set, the ClusterRole holds only the rules of
files/rbac/<name>-cluster-scoped.yaml, and each namespace named gets a Role
and RoleBinding of those of files/rbac/<name>-namespaced.yaml.
*/}}
{{- define "nats-operator.controller" -}}
{{- $fullname := include "nats-operator.fullname" . -}}
{{- $namespaces := .root.Values.watchNamespaces -}}
{{- $role := printf "files/rbac/%s.yaml" .name -}}
{{- if $namespaces -}}
{{- $role = printf "files/rbac/%s-cluster-scoped.yaml" .name -}}
{{- end -}}
{{- $rules := required (printf "%s has no rules" $role) (.root.Files.Get $role | fromYaml).rules -}}
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
  {{- toYaml $rules | nindent 2 }}
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
{{- if $namespaces }}
{{- $nsRole := printf "files/rbac/%s-namespaced.yaml" .name }}
{{- $nsRules := required (printf "%s has no rules" $nsRole) (.root.Files.Get $nsRole | fromYaml).rules }}
{{- range $ns := $namespaces }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ $fullname }}
  namespace: {{ $ns }}
  labels:
    {{- include "nats-operator.labels" $ | nindent 4 }}
rules:
  {{- toYaml $nsRules | nindent 2 }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ $fullname }}
  namespace: {{ $ns }}
  labels:
    {{- include "nats-operator.labels" $ | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ $fullname }}
subjects:
  - kind: ServiceAccount
    name: {{ $fullname }}
    namespace: {{ $.root.Release.Namespace }}
{{- end }}
{{- end }}
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
  - apiGroups: [""]
    resources: [events]
    verbs: [create, patch]
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
  {{- with include "nats-operator.merged" (list .root.Values.annotations .values.annotations) }}
  annotations:
    {{- . | nindent 4 }}
  {{- end }}
spec:
  replicas: {{ .values.replicas }}
  selector:
    matchLabels:
      {{- include "nats-operator.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "nats-operator.labels" . | nindent 8 }}
      {{- with include "nats-operator.merged" (list .root.Values.podAnnotations .values.podAnnotations) }}
      annotations:
        {{- . | nindent 8 }}
      {{- end }}
    spec:
      serviceAccountName: {{ $fullname }}
      {{- with .root.Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with include "nats-operator.merged" (list .root.Values.nodeSelector .values.nodeSelector) }}
      nodeSelector:
        {{- . | nindent 8 }}
      {{- end }}
      {{- with include "nats-operator.merged" (list .root.Values.affinity .values.affinity) }}
      affinity:
        {{- . | nindent 8 }}
      {{- end }}
      {{- with .values.tolerations | default .root.Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .values.priorityClassName | default .root.Values.priorityClassName }}
      priorityClassName: {{ . | quote }}
      {{- end }}
      {{- with .values.topologySpreadConstraints | default .root.Values.topologySpreadConstraints }}
      topologySpreadConstraints:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      securityContext:
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: {{ .name }}
          image: {{ include "nats-operator.image" (dict "image" .values.image "tag" (.values.image.tag | default .root.Chart.AppVersion)) | quote }}
          imagePullPolicy: {{ .values.image.pullPolicy }}
          args:
            - --leader-elect={{ .root.Values.leaderElection.enabled }}
            - --leader-election-id={{ .group }}
            - --metrics-bind-address=:8080
            - --health-probe-bind-address=:8081
            {{- with $namespaces }}
            - --watch-namespaces={{ join "," . }}
            {{- end }}
            {{- range concat (.args | default list) .root.Values.extraArgs .values.extraArgs }}
            - {{ . | quote }}
            {{- end }}
          {{- with include "nats-operator.env" (list .root.Values.env .values.env) }}
          env:
            {{- . | nindent 12 }}
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

{{/*
nats-operator.test renders one controller's `helm test` hook. It takes a dict
of root (the chart context) and name (the controller's name).
*/}}
{{- define "nats-operator.test" -}}
{{- $fullname := include "nats-operator.fullname" . -}}
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
      image: {{ include "nats-operator.image" (dict "image" $image "tag" $image.tag) | quote }}
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
        - http://{{ $fullname }}-test.{{ $ns }}.svc:8081/readyz
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: [ALL]
{{- end }}
