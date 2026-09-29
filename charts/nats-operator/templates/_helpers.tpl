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
{{ printf "%s-%s" .root.Release.Name .name }}
{{- end }}

{{/*
nats-operator.serviceName takes a dict of root (the chart context), name (the
controller's name) and suffix, and returns <release>-<name>-<suffix>; it fails
when that is longer than the 63 characters a Service name allows.
*/}}
{{- define "nats-operator.serviceName" -}}
{{- $name := printf "%s-%s" (include "nats-operator.fullname" .) .suffix -}}
{{- if gt (len $name) 63 -}}
{{- fail (printf "Service name %s is %d characters, over the 63 allowed: the release name can be at most %d characters" $name (len $name) (sub 63 (sub (len $name) (len .root.Release.Name)))) -}}
{{- end -}}
{{ $name }}
{{- end }}

{{/*
nats-operator.image takes a dict of repository, tag and digest, and returns
repository:tag, followed by @digest where digest is set.
*/}}
{{- define "nats-operator.image" -}}
{{ printf "%s:%s" .repository .tag }}{{ with .digest }}@{{ . }}{{ end }}
{{- end }}

{{/*
nats-operator.controllerImage takes the chart context and a controller's image
values, and returns its image tagged image.tag, or appVersion where that is
empty, pinned to image.digest only while the tag is appVersion.
*/}}
{{- define "nats-operator.controllerImage" -}}
{{- $tag := .image.tag | default .root.Chart.AppVersion -}}
{{- $digest := ternary .image.digest "" (eq $tag .root.Chart.AppVersion) -}}
{{ include "nats-operator.image" (dict "repository" .image.repository "tag" $tag "digest" $digest) }}
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
nats-operator.env merges a list of env lists into one, as YAML, in which an
entry replaces every earlier list's entry of the same name; it renders nothing
when every list is empty.
*/}}
{{- define "nats-operator.env" -}}
{{- $out := list -}}
{{- range $env := . -}}
{{- $names := list -}}
{{- range $env -}}
{{- $names = append $names .name -}}
{{- end -}}
{{- $kept := list -}}
{{- range $out -}}
{{- if not (has .name $names) -}}
{{- $kept = append $kept . -}}
{{- end -}}
{{- end -}}
{{- $out = concat $kept $env -}}
{{- end -}}
{{- if $out -}}
{{- toYaml $out -}}
{{- end -}}
{{- end }}

{{/*
nats-operator.goMemLimit takes a dict of key (the value's path, for the error)
and quantity, and returns 90% of quantity in bytes. It fails on a quantity
other than an integer, optionally suffixed with one of k, M, G, T, Ki, Mi, Gi
or Ti.
*/}}
{{- define "nats-operator.goMemLimit" -}}
{{- $quantity := toString .quantity -}}
{{- if and (kindIs "float64" .quantity) (eq .quantity (float64 (int64 .quantity))) -}}
{{- $quantity = int64 .quantity | toString -}}
{{- end -}}
{{- if not (regexMatch "^[0-9]+(k|M|G|T|Ki|Mi|Gi|Ti)?$" $quantity) -}}
{{- fail (printf "%s is %q: GOMEMLIMIT is derived only from an integer, optionally suffixed with one of k, M, G, T, Ki, Mi, Gi or Ti" .key $quantity) -}}
{{- end -}}
{{- $suffix := regexFind "[A-Za-z]+$" $quantity -}}
{{- $units := dict "" 1 "k" 1000 "M" 1000000 "G" 1000000000 "T" 1000000000000 "Ki" 1024 "Mi" 1048576 "Gi" 1073741824 "Ti" 1099511627776 -}}
{{- div (mul (atoi (trimSuffix $suffix $quantity)) (get $units $suffix) 9) 10 -}}
{{- end }}

{{/*
nats-operator.controller renders one controller's ServiceAccount, RBAC and
Deployment. It takes a dict of root (the chart context), name (the
controller's, and its image's), group (its API group; its lease is
<release>-<group>), values (its block of values) and, optionally, args (flags
appended to the controller's own, before extraArgs). Its ClusterRole's rules
are those of files/rbac/<name>.yaml, a copy of the role controller-gen
generates for it.
With watchNamespaces set, the ClusterRole holds only the rules of
files/rbac/<name>-cluster-scoped.yaml, and each namespace named gets a Role
and RoleBinding of those of files/rbac/<name>-namespaced.yaml. It fails with
more than one replica while leaderElection.enabled is false, on an extraArgs
entry setting --leader-elect or --leader-election-id, and where
nats-operator.goMemLimit fails on its memory limit.
*/}}
{{- define "nats-operator.controller" -}}
{{- $fullname := include "nats-operator.fullname" . -}}
{{- $lease := printf "%s-%s" .root.Release.Name .group -}}
{{- $namespaces := .root.Values.watchNamespaces -}}
{{- $role := printf "files/rbac/%s.yaml" .name -}}
{{- if $namespaces -}}
{{- $role = printf "files/rbac/%s-cluster-scoped.yaml" .name -}}
{{- end -}}
{{- $rules := required (printf "%s has no rules" $role) (.root.Files.Get $role | fromYaml).rules -}}
{{- $prometheus := .root.Values.metrics.prometheus.enabled -}}
{{- $metricsTLS := (default dict .root.Values.metrics.tls).secretName -}}
{{- $prometheusEnv := list -}}
{{- if $prometheus -}}
{{- $prometheusEnv = list (dict "name" "OTEL_METRICS_EXPORTER" "value" "prometheus") (dict "name" "OTEL_EXPORTER_PROMETHEUS_HOST" "value" "0.0.0.0") -}}
{{- end -}}
{{- $memoryEnv := list -}}
{{- if and .values.resources .values.resources.limits .values.resources.limits.memory -}}
{{- $memoryEnv = list (dict "name" "GOMEMLIMIT" "value" (include "nats-operator.goMemLimit" (dict "key" (printf "%s.resources.limits.memory" (trimSuffix "-controller" .name)) "quantity" .values.resources.limits.memory))) -}}
{{- end -}}
{{- if and (not .root.Values.leaderElection.enabled) (gt (int .values.replicas) 1) -}}
{{- fail (printf "%s.replicas is %d: more than one replica needs leaderElection.enabled" (trimSuffix "-controller" .name) (int .values.replicas)) -}}
{{- end -}}
{{- range concat .root.Values.extraArgs .values.extraArgs -}}
{{- $flag := trimPrefix "-" (trimPrefix "-" .) -}}
{{- if or (eq $flag "leader-election-id") (hasPrefix "leader-election-id=" $flag) -}}
{{- fail (printf "extraArgs entry %q sets the lease name, which the chart fixes as %s" . $lease) -}}
{{- end -}}
{{- if or (eq $flag "leader-elect") (hasPrefix "leader-elect=" $flag) -}}
{{- fail (printf "extraArgs entry %q sets leader election, which the chart sets from leaderElection.enabled" .) -}}
{{- end -}}
{{- end -}}
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
    verbs: [create]
  - apiGroups: [coordination.k8s.io]
    resources: [leases]
    resourceNames: [{{ $lease }}]
    verbs: [get, update, patch]
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
          image: {{ include "nats-operator.controllerImage" (dict "root" .root "image" .values.image) | quote }}
          imagePullPolicy: {{ .values.image.pullPolicy }}
          args:
            - --leader-elect={{ .root.Values.leaderElection.enabled }}
            - --leader-election-id={{ $lease }}
            - --metrics-bind-address=:8080
            - --health-probe-bind-address=:8081
            {{- if $metricsTLS }}
            - --metrics-cert-dir=/var/run/secrets/nats-operator/metrics-tls
            {{- end }}
            {{- with $namespaces }}
            - --watch-namespaces={{ join "," . }}
            {{- end }}
            {{- range concat (.args | default list) .root.Values.extraArgs .values.extraArgs }}
            - {{ . | quote }}
            {{- end }}
          {{- with include "nats-operator.env" (list $memoryEnv $prometheusEnv .root.Values.env .values.env) }}
          env:
            {{- . | nindent 12 }}
          {{- end }}
          ports:
            - name: metrics
              containerPort: 8080
            - name: probes
              containerPort: 8081
            {{- if $prometheus }}
            - name: otel-metrics
              containerPort: 9464
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
          {{- if $metricsTLS }}
          volumeMounts:
            - name: metrics-tls
              mountPath: /var/run/secrets/nats-operator/metrics-tls
              readOnly: true
          {{- end }}
      {{- if $metricsTLS }}
      volumes:
        - name: metrics-tls
          secret:
            secretName: {{ $metricsTLS }}
            items:
              - key: tls.crt
                path: tls.crt
              - key: tls.key
                path: tls.key
      {{- end }}
{{- end }}

{{/*
nats-operator.test renders one controller's `helm test` hook. It takes a dict
of root (the chart context) and name (the controller's name).
*/}}
{{- define "nats-operator.test" -}}
{{- $name := include "nats-operator.serviceName" (dict "root" .root "name" .name "suffix" "test") -}}
{{- $image := .root.Values.tests.image -}}
{{- $ns := .root.Release.Namespace -}}
{{- $pod := dict "root" .root "name" (printf "%s-test" .name) -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ $name }}
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
  name: {{ $name }}
  namespace: {{ $ns }}
  labels:
    {{- include "nats-operator.labels" $pod | nindent 4 }}
  annotations:
    helm.sh/hook: test
    helm.sh/hook-delete-policy: before-hook-creation
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
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
      image: {{ include "nats-operator.image" (dict "repository" $image.repository "tag" $image.tag "digest" $image.digest) | quote }}
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
        - http://{{ $name }}.{{ $ns }}.svc:8081/readyz
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: [ALL]
{{- end }}
