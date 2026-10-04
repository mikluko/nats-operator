---
title: Chart and controller flags
weight: 4
description: The Helm chart's values, the flags of the controllers, their metrics endpoints, the RBAC each controller is granted, and the finalizers they hold.
---

The Helm chart `nats-operator` installs the CRDs of all four API groups and any subset of the three controllers: cluster, auth and JetStream.
Each enabled controller has its own ServiceAccount, ClusterRole and Deployment.
[Install]({{< relref "/docs/install" >}}) shows how to install, upgrade and uninstall the chart.

## Published chart and images

Each release publishes the chart at `oci://ghcr.io/mikluko/nats-operator/charts/nats-operator`.
The release's version is the chart's `version` and its `appVersion`.

Each release publishes the three images at `ghcr.io/mikluko/nats-operator/<controller>:<version>`.
The published chart pins each image by digest.
A controller whose `image.tag` is set to another tag runs that tag, unpinned.

## Release name

The chart does not render a Service whose name is longer than 63 characters.
The Services are named `<release>-<controller>-test` and `<release>-<controller>-metrics`.
With the JetStream controller enabled, the release name can have at most 37 characters, and at most 34 with `metrics.service.enabled`.

## Values

The chart has a values schema.
`helm install`, `helm upgrade` and `helm lint` fail on a key that the schema does not have.

### Top-level values

Table: The values that apply to every controller.

| Value | Default | Description |
|---|---|---|
| `imagePullSecrets` | `[]` | Pull secrets of every controller's pod. |
| `leaderElection.enabled` | `true` | Sets `--leader-elect` on every controller. Renders a Role in the release namespace that allows a controller to create Leases, and to get, update and patch its own Lease, `<release>-<API group>`. When the value is `false`, the chart does not render a controller whose `replicas` is above 1. |
| `watchNamespaces` | `[]` | The namespaces that every controller watches and reconciles in, passed as `--watch-namespaces`. The chart grants RBAC in these namespaces alone, as [RBAC](#rbac) describes. When the list is empty, the controllers watch every namespace. The list must include the namespace of `auth.systemConnection`. |
| `nodeSelector` | `{}` | Node selector of every controller's pod. |
| `annotations` | `{}` | Annotations of every controller's Deployment. |
| `podAnnotations` | `{}` | Annotations of every controller's pod. [The OpenTelemetry Operator]({{< relref "/docs/reference/telemetry#the-opentelemetry-operator" >}}) has the annotations that inject its SDK configuration. |
| `affinity` | `{}` | Affinity of every controller's pod. |
| `tolerations` | `[]` | Tolerations of every controller's pod. |
| `priorityClassName` | `""` | Priority class of every controller's pod. |
| `topologySpreadConstraints` | `[]` | Topology spread constraints of every controller's pod. The chart does not fill in the `labelSelector` of a constraint. |
| `extraArgs` | `[]` | Flags appended to the flags of every controller, after the chart's own. When a flag is given twice, the last one applies. An entry that sets `--leader-elect` or `--leader-election-id` fails the render. |
| `env` | `[]` | Environment of every controller's container. |

### Values under `cluster`

Table: The values of the cluster controller.

| Value | Default | Description |
|---|---|---|
| `cluster.enabled` | `true` | Installs the cluster controller. |
| `cluster.replicas` | `1` | Replicas of the Deployment. |
| `cluster.allowGatewayWithoutTLS` | `false` | Passes `--allow-gateway-without-tls` to the cluster controller. When the value is `false`, the chart does not pass the flag, and the cluster controller refuses a `NatsCluster` that has a gateway without `tls`. |
| `cluster.image.repository` | `ghcr.io/mikluko/nats-operator/cluster-controller` | Image repository. |
| `cluster.image.tag` | `""` | Image tag. When the value is empty, the tag is the chart's `appVersion`. |
| `cluster.image.digest` | The release's image digest. `""` in the source tree. | A digest, as `sha256:<hex>`, appended to the image reference as `@<digest>`. The chart appends it only while the tag is the chart's `appVersion`. |
| `cluster.image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `cluster.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Resources of the container. A memory limit also sets `GOMEMLIMIT` to 90% of the limit, unless an `env` entry of that name replaces it. The memory limit must be an integer, optionally followed by one of `k`, `M`, `G`, `T`, `Ki`, `Mi`, `Gi` or `Ti`. |
| `cluster.nodeSelector` | `{}` | Node selector of the pod. Each key overrides the same key of `nodeSelector`. |
| `cluster.annotations` | `{}` | Annotations of the Deployment. Each key overrides the same key of `annotations`. |
| `cluster.podAnnotations` | `{}` | Annotations of the pod. Each key overrides the same key of `podAnnotations`. |
| `cluster.affinity` | `{}` | Affinity of the pod. Each of `nodeAffinity`, `podAffinity` and `podAntiAffinity` replaces the whole of the same key of `affinity`. |
| `cluster.tolerations` | `[]` | Tolerations of the pod. When set, they replace the whole of `tolerations`. |
| `cluster.priorityClassName` | `""` | Priority class of the pod. When set, it replaces `priorityClassName`. |
| `cluster.topologySpreadConstraints` | `[]` | Topology spread constraints of the pod. When set, they replace the whole of `topologySpreadConstraints`. |
| `cluster.extraArgs` | `[]` | Flags appended after `extraArgs`. An entry that sets `--leader-elect` or `--leader-election-id` fails the render. |
| `cluster.env` | `[]` | Environment of the container. An entry replaces the entry of the same name in `env`. |

### Values under `auth`

Table: The values of the auth controller.

| Value | Default | Description |
|---|---|---|
| `auth.enabled` | `true` | Installs the auth controller. |
| `auth.replicas` | `1` | Replicas of the Deployment. |
| `auth.systemConnection` | `""` | Passes `--system-connection` to the auth controller, as `namespace/name`. When the value is empty, the chart does not pass the flag. No server then receives an account JWT, a deleted `NatsUser` keeps its connections, and accounts and users have the condition `Distributed` False with the reason `NoSystemConnection`. |
| `auth.image.repository` | `ghcr.io/mikluko/nats-operator/auth-controller` | Image repository. |
| `auth.image.tag` | `""` | Image tag. When the value is empty, the tag is the chart's `appVersion`. |
| `auth.image.digest` | The release's image digest. `""` in the source tree. | A digest, as `sha256:<hex>`, appended to the image reference as `@<digest>`. The chart appends it only while the tag is the chart's `appVersion`. |
| `auth.image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `auth.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Resources of the container. A memory limit also sets `GOMEMLIMIT` to 90% of the limit, unless an `env` entry of that name replaces it. The memory limit must be an integer, optionally followed by one of `k`, `M`, `G`, `T`, `Ki`, `Mi`, `Gi` or `Ti`. |
| `auth.nodeSelector` | `{}` | Node selector of the pod. Each key overrides the same key of `nodeSelector`. |
| `auth.annotations` | `{}` | Annotations of the Deployment. Each key overrides the same key of `annotations`. |
| `auth.podAnnotations` | `{}` | Annotations of the pod. Each key overrides the same key of `podAnnotations`. |
| `auth.affinity` | `{}` | Affinity of the pod. Each of `nodeAffinity`, `podAffinity` and `podAntiAffinity` replaces the whole of the same key of `affinity`. |
| `auth.tolerations` | `[]` | Tolerations of the pod. When set, they replace the whole of `tolerations`. |
| `auth.priorityClassName` | `""` | Priority class of the pod. When set, it replaces `priorityClassName`. |
| `auth.topologySpreadConstraints` | `[]` | Topology spread constraints of the pod. When set, they replace the whole of `topologySpreadConstraints`. |
| `auth.extraArgs` | `[]` | Flags appended after `extraArgs`. An entry that sets `--leader-elect` or `--leader-election-id` fails the render. |
| `auth.env` | `[]` | Environment of the container. An entry replaces the entry of the same name in `env`. |

### Values under `jetstream`

Table: The values of the JetStream controller.

| Value | Default | Description |
|---|---|---|
| `jetstream.enabled` | `true` | Installs the JetStream controller. |
| `jetstream.replicas` | `1` | Replicas of the Deployment. |
| `jetstream.image.repository` | `ghcr.io/mikluko/nats-operator/jetstream-controller` | Image repository. |
| `jetstream.image.tag` | `""` | Image tag. When the value is empty, the tag is the chart's `appVersion`. |
| `jetstream.image.digest` | The release's image digest. `""` in the source tree. | A digest, as `sha256:<hex>`, appended to the image reference as `@<digest>`. The chart appends it only while the tag is the chart's `appVersion`. |
| `jetstream.image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `jetstream.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Resources of the container. A memory limit also sets `GOMEMLIMIT` to 90% of the limit, unless an `env` entry of that name replaces it. The memory limit must be an integer, optionally followed by one of `k`, `M`, `G`, `T`, `Ki`, `Mi`, `Gi` or `Ti`. |
| `jetstream.nodeSelector` | `{}` | Node selector of the pod. Each key overrides the same key of `nodeSelector`. |
| `jetstream.annotations` | `{}` | Annotations of the Deployment. Each key overrides the same key of `annotations`. |
| `jetstream.podAnnotations` | `{}` | Annotations of the pod. Each key overrides the same key of `podAnnotations`. |
| `jetstream.affinity` | `{}` | Affinity of the pod. Each of `nodeAffinity`, `podAffinity` and `podAntiAffinity` replaces the whole of the same key of `affinity`. |
| `jetstream.tolerations` | `[]` | Tolerations of the pod. When set, they replace the whole of `tolerations`. |
| `jetstream.priorityClassName` | `""` | Priority class of the pod. When set, it replaces `priorityClassName`. |
| `jetstream.topologySpreadConstraints` | `[]` | Topology spread constraints of the pod. When set, they replace the whole of `topologySpreadConstraints`. |
| `jetstream.extraArgs` | `[]` | Flags appended after `extraArgs`. An entry that sets `--leader-elect` or `--leader-election-id` fails the render. |
| `jetstream.env` | `[]` | Environment of the container. An entry replaces the entry of the same name in `env`. |

### Values under `metrics`

Table: The values of the metrics endpoints. [Metrics](#metrics) describes the endpoints.

| Value | Default | Description |
|---|---|---|
| `metrics.scraper.serviceAccount` | `""` | A ServiceAccount, as `namespace/name`, that the chart allows to read the controllers' metrics, as [RBAC](#rbac) describes. When the value is empty, the chart grants that to no one. |
| `metrics.tls.secretName` | `""` | A `kubernetes.io/tls` Secret in the release namespace. Every controller mounts its `tls.crt` and `tls.key` and serves its metrics endpoint with them, through `--metrics-cert-dir`. When the value is empty, each controller serves a self-signed certificate. |
| `metrics.service.enabled` | `false` | Renders a Service `<release>-<controller>-metrics` for each enabled controller, with the port `metrics` (`8080`) on the controller's metrics endpoint. |
| `metrics.prometheus.enabled` | `false` | Serves each controller's OpenTelemetry metrics over plain HTTP on port `9464` of the pod IP. The port is named `otel-metrics` on the container, the metrics Service and the ServiceMonitor. Sets `OTEL_METRICS_EXPORTER=prometheus` and `OTEL_EXPORTER_PROMETHEUS_HOST=0.0.0.0`, unless an `env` entry of the same name replaces them. Requires `metrics.service.enabled`. |
| `metrics.serviceMonitor.enabled` | `false` | Renders a prometheus-operator `ServiceMonitor` `<release>-<controller>-metrics` for each enabled controller, over the metrics Service. Requires `metrics.service.enabled` and the `monitoring.coreos.com/v1` CRDs. |
| `metrics.serviceMonitor.labels` | `{}` | Labels of each ServiceMonitor, for a Prometheus that selects ServiceMonitors by label. |
| `metrics.serviceMonitor.interval` | `""` | Scrape interval of each ServiceMonitor. When the value is empty, Prometheus uses its own. |
| `metrics.serviceMonitor.authorization` | `{}` | `{credentials: {name, key}}`, as in the `authorization` of a ServiceMonitor endpoint: a key of a Secret in the release namespace. Each ServiceMonitor scrapes the port `metrics` with the bearer token in that key, in place of `bearerTokenFile`. The type is always `Bearer`. |
| `metrics.serviceMonitor.caSecret` | `{}` | `{name, key}`, a key of a Secret in the release namespace that contains the CA that each ServiceMonitor verifies the metrics certificate against. When the value is empty, the CA is `ca.crt` of `metrics.tls.secretName`. Requires `metrics.tls.secretName`. |

### Values under `networkPolicy`

Table: The values of the controllers' NetworkPolicies. [Network policy](#network-policy) describes the policies.

| Value | Default | Description |
|---|---|---|
| `networkPolicy.enabled` | `false` | Renders a NetworkPolicy `<release>-<controller>` for each enabled controller, over the controller's pods. The policy admits ports `8080` and `9464` from `networkPolicy.from` alone, and port `8081` from the controller's pods and its `helm test` pod. It admits nothing else inbound. |
| `networkPolicy.from` | `[]` | The NetworkPolicy peers admitted to ports `8080` and `9464`. When the list is empty, no peer is admitted. |
| `networkPolicy.egress` | `[]` | NetworkPolicy egress rules, each `{to, ports}`, rendered as given into the NetworkPolicy of each controller. The policy then admits no other egress, so the API server, DNS, and the client and monitoring ports of the NATS clusters each need a rule. Requires `networkPolicy.enabled`. When the list is empty, egress is unrestricted. |

### Values under `tests`

Table: The values of the `helm test` pods. [Helm test](#helm-test) describes the pods.

| Value | Default | Description |
|---|---|---|
| `tests.image.repository` | `busybox` | Image repository of the `helm test` pods. |
| `tests.image.tag` | `"1.37.0"` | Image tag. |
| `tests.image.digest` | `"sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e"` | A digest, as `sha256:<hex>`, appended to the image reference as `@<digest>`. |
| `tests.image.pullPolicy` | `IfNotPresent` | Image pull policy. |

## Controller flags

The chart runs every controller with these flags, followed by `extraArgs` and then the controller's own `extraArgs`:

- `--leader-elect`, set from `leaderElection.enabled`.
- `--leader-election-id`, set to `<release>-<API group>`, such as `nats-operator-cluster.nats.mikluko.io`.
- Metrics on `:8080`, the container port `metrics`.
  The chart renders a Service for that port only while `metrics.service.enabled` is set.
- Health probes on `:8081`, at `/healthz` and `/readyz`.

Of the other flags, the chart sets only these four:

- `--system-connection`, from `auth.systemConnection`.
- `--allow-gateway-without-tls`, from `cluster.allowGatewayWithoutTLS`.
- `--metrics-cert-dir`, from `metrics.tls.secretName`.
- `--watch-namespaces`, from `watchNamespaces`.

Each release elects its own leader, so two releases that watch one namespace both reconcile its objects.
Two releases must watch disjoint namespaces.
A release whose `watchNamespaces` is empty must be the only release in the Kubernetes cluster.

Table: The flags of the controller binaries.

| Flag | Controller | Default | Description |
|---|---|---|---|
| `--allow-gateway-without-tls` | cluster | `false` | Renders a `NatsCluster` whose gateway has no `tls`. Any peer that reaches the gateway port of such a NATS cluster joins the supercluster. When the flag is unset, such a `NatsCluster` has the condition `Ready` False with the reason `GatewayWithoutTLS`, and the cluster controller renders nothing for it. |
| `--metrics-bind-address` | all | `:8080` | The address that serves controller-runtime's Prometheus metrics at `/metrics`, over HTTPS, with the certificate of `--metrics-cert-dir`. A request needs a bearer token that the API server authenticates, of a user allowed `get` on the non-resource URL `/metrics`. The controller caches the identity of a token for one minute, an allow for five minutes and a denial for thirty seconds. `0` disables the endpoint. |
| `--metrics-cert-dir` | all | unset | The directory that contains the `tls.crt` and `tls.key` that the metrics endpoint serves. The controller reloads them when they change, and exits at start if they do not load. When the flag is unset, the controller serves a self-signed certificate that it generates at start. |
| `--health-probe-bind-address` | all | `:8081` | The address of `/healthz` and `/readyz`. `/healthz` always passes. `/readyz` passes once the controller has listed and watched everything it reconciles from, on every replica, elected or not. |
| `--leader-elect` | all | `false` | Turns on leader election, so that one replica reconciles. |
| `--leader-election-id` | all | the controller's API group | The name of the leader election Lease. |
| `--system-connection` | auth | unset | `namespace/name` of a `NatsConnection` whose creds are a user of the system account of a `NatsOperator`, with the `auth-controller` preset. Through it, the auth controller pushes account JWTs to the servers' resolvers, deletes them from the resolvers, and closes the connections of a deleted user. When the flag is unset, the auth controller signs JWTs and writes them to status, nothing reaches the servers, the connections of a deleted user stay open, and accounts and users have the condition `Distributed` False with the reason `NoSystemConnection`. |
| `--watch-namespaces` | all | unset | The namespaces that the controller watches and reconciles in, separated by commas. The controller never reconciles an object in another namespace, and a reference into another namespace does not resolve. When the flag is unset, the controller watches every namespace. |
| `--resync-period` | JetStream | `10m` | How often the JetStream controller compares a JetStream resource to its server object. |
| `--zap-log-level`, `--zap-encoder`, `--zap-devel`, `--zap-stacktrace-level`, `--zap-time-encoding` | all | production logging, JSON at `info` | Logging. |

## Metrics

Each controller serves controller-runtime's metrics over HTTPS on port `8080`, and no other metrics on that port.
A scrape is authenticated by its bearer token, which must be the token of a ServiceAccount that is allowed `get` on `/metrics`, such as `metrics.scraper.serviceAccount`.

The certificate of the endpoint depends on `metrics.tls.secretName`:

- When it is set, every controller serves the certificate in that Secret.
  The certificate must have the DNS name of the metrics Service of each enabled controller, `<release>-<controller>-metrics.<namespace>.svc`.
  A cert-manager `Certificate` with those `dnsNames` writes such a Secret.
  Each ServiceMonitor verifies the certificate with `tlsConfig.ca` from `ca.crt` of the Secret, or from `metrics.serviceMonitor.caSecret`, and with `serverName` set to the DNS name of the Service.
- When it is unset, each controller serves a self-signed certificate that it generates at start, which a scraper cannot verify.
  Each ServiceMonitor sets `tlsConfig.insecureSkipVerify: true`, so whatever answers on the Service receives the scraper's token.

With `metrics.serviceMonitor.enabled`, each ServiceMonitor scrapes port `8080` with `scheme: https` and the token of the Prometheus pod's own ServiceAccount, read from `/var/run/secrets/kubernetes.io/serviceaccount/token`.
`metrics.scraper.serviceAccount` must be that ServiceAccount.
A Prometheus that denies file access through ServiceMonitors (`arbitraryFSAccessThroughSMs.deny: true`) refuses `bearerTokenFile`.
For such a Prometheus, `metrics.serviceMonitor.authorization.credentials` must be a key of a Secret in the release namespace that contains the token of that ServiceAccount.

The controllers' own instruments, such as `nats_operator.account.jwt_expiry`, are OpenTelemetry metrics, which [Telemetry]({{< relref "/docs/reference/telemetry#metrics" >}}) lists.
With `metrics.prometheus.enabled`, each controller serves them at `/metrics` on port `9464`, over plain HTTP, to any client that reaches the pod.
Each ServiceMonitor scrapes that port with `scheme: http` and no token.
The page on port `9464` has the kind, namespace and name of every resource that the controller reconciles, in every namespace that it watches, with the type and reason of each of its conditions.

[Scrape the controllers' metrics over verified TLS]({{< relref "/docs/stories/12-metrics" >}}) shows how to set these values.

## Network policy

With `networkPolicy.enabled`, the NetworkPolicy of each controller admits ports `8080` and `9464` from the peers in `networkPolicy.from` alone.

`networkPolicy.egress` limits where the controllers connect.
Every controller connects to the NATS servers that it reconciles.
The JetStream controller also connects, from the release namespace, to every address in the `spec.servers` of a `NatsConnection`.
With egress rules that admit the NATS clusters, the API server and DNS, the controllers can connect nowhere else.

## RBAC

The ClusterRole of each controller is named `<release>-<controller>`, such as `nats-operator-cluster-controller`.
It is bound to the ServiceAccount of the same name in the release namespace.
No controller can write the API group of another controller.

While `leaderElection.enabled` is `true`, each controller also has the Role `<release>-<controller>-leader-election` in the release namespace.
The Role allows `create` on `coordination.k8s.io` `leases`, `get`, `update` and `patch` on the controller's own Lease `<release>-<API group>`, and `create` and `patch` on `""` `events`.

While `watchNamespaces` is set, the ClusterRole of each controller has only `create` on `authentication.k8s.io` `tokenreviews` and `authorization.k8s.io` `subjectaccessreviews`, which its metrics endpoint needs.
Every other rule in the tables on this page is then in a Role `<release>-<controller>`, with a RoleBinding of the same name, in each namespace in `watchNamespaces`.

While `metrics.scraper.serviceAccount` is set, the ClusterRole `<release>-metrics-scraper` allows `get` on the non-resource URL `/metrics` and is bound to that ServiceAccount.
A request with the token of that ServiceAccount is then allowed on the metrics endpoint of every controller.
The same grant allows `get` on the `/metrics` of the Kubernetes API server.

### Every controller

Table: The rules that every controller has.

| API group | Resources | Verbs |
|---|---|---|
| `authentication.k8s.io` | `tokenreviews` | `create` |
| `authorization.k8s.io` | `subjectaccessreviews` | `create` |
| `events.k8s.io` | `events` | `create`, `patch` |

### Cluster controller

Table: The further rules of the cluster controller.

| API group | Resources | Verbs |
|---|---|---|
| `""` | `configmaps` | `get`, `list`, `watch`, `create`, `update`, `patch`, `delete` |
| `""` | `persistentvolumeclaims` | `get`, `list`, `watch`, `delete` |
| `""` | `secrets`, `services` | `get`, `list`, `watch`, `create`, `update`, `delete` |
| `apps` | `statefulsets` | `get`, `list`, `watch`, `create`, `update`, `patch`, `delete` |
| `cert-manager.io` | `certificates` | `get`, `create`, `update`, `delete` |
| `cluster.nats.mikluko.io` | `natsclusters` | `get`, `list`, `watch`, `patch` |
| `cluster.nats.mikluko.io` | `natsclusters/finalizers` | `update` |
| `cluster.nats.mikluko.io` | `natsclusters/status` | `patch` |
| `nats.mikluko.io` | `natsaccounttrusts`, `natsconnections`, `natsoperatortrusts` | `get`, `list`, `watch` |
| `nats.mikluko.io` | `natsreferencegrants` | `list`, `watch` |
| `networking.k8s.io` | `networkpolicies` | `get`, `list`, `watch`, `create`, `update`, `delete` |
| `policy` | `poddisruptionbudgets` | `get`, `list`, `watch`, `create`, `update` |

### Auth controller

Table: The further rules of the auth controller.

| API group | Resources | Verbs |
|---|---|---|
| `""` | `secrets` | `get`, `list`, `watch`, `create`, `update`, `delete` |
| `auth.nats.mikluko.io` | `natsaccounts`, `natsusers` | `get`, `list`, `watch`, `patch` |
| `auth.nats.mikluko.io` | `natsaccounts/status`, `natsoperators/status`, `natssystemaccounts/status`, `natsusers/finalizers`, `natsusers/status` | `update` |
| `auth.nats.mikluko.io` | `natsoperators`, `natssystemaccounts` | `get`, `list`, `watch` |
| `nats.mikluko.io` | `natsaccounttrusts`, `natsconnections`, `natsoperatortrusts` | `get`, `list`, `watch` |
| `nats.mikluko.io` | `natsaccounttrusts/status`, `natsoperatortrusts/status` | `update` |
| `nats.mikluko.io` | `natsreferencegrants` | `list`, `watch` |

### JetStream controller

Table: The further rules of the JetStream controller.

| API group | Resources | Verbs |
|---|---|---|
| `""` | `secrets` | `get`, `list`, `watch` |
| `jetstream.nats.mikluko.io` | `natsbalancers`, `natssystembalancers` | `get`, `list`, `watch` |
| `jetstream.nats.mikluko.io` | `natsbalancers/status`, `natsclusterevacuations/status`, `natsconsumers/status`, `natskeyvalues/status`, `natsobjectstores/status`, `natsstreams/status`, `natssystembalancers/status` | `patch` |
| `jetstream.nats.mikluko.io` | `natsclusterevacuations`, `natsconsumers`, `natskeyvalues`, `natsobjectstores`, `natsstreams` | `get`, `list`, `watch`, `patch` |
| `nats.mikluko.io` | `natsconnections` | `get`, `list`, `watch` |
| `nats.mikluko.io` | `natsconnections/status` | `patch` |
| `nats.mikluko.io` | `natsreferencegrants` | `list`, `watch` |

## Helm test

For each enabled controller, `helm test` starts the Pod `<release>-<controller>-test`.
The Pod sends a GET to the controller's `/readyz` on port `8081`, through the Service `<release>-<controller>-test`.
It tries 30 times, two seconds apart.

## Finalizers

Only the controller that holds a finalizer removes it.

Table: The finalizers that the controllers put on their kinds.

| Kind | Finalizer | Held by |
|---|---|---|
| `NatsCluster` with JetStream | `cluster.nats.mikluko.io/jetstream-data` | cluster controller |
| `NatsAccount` | `auth.nats.mikluko.io/delete` | auth controller |
| `NatsUser` | `auth.nats.mikluko.io/revoke` | auth controller |
| `NatsStream`, `NatsConsumer`, `NatsKeyValue`, `NatsObjectStore`, `NatsClusterEvacuation` | `jetstream.nats.mikluko.io/finalizer` | JetStream controller |
