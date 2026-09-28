---
title: Install
weight: 1
description: Install the chart, pick the controllers, and upgrade or uninstall them.
---

The Helm chart `nats-operator` installs the CRDs of all four API groups and any subset of the three controllers: cluster, auth and JetStream. Each enabled controller gets its own ServiceAccount, ClusterRole and Deployment.

## Prerequisites

- **Kubernetes 1.29 or later.** The chart declares `kubeVersion: ">=1.29.0-0"`; Helm refuses to install it on an older cluster.
- **Helm**, to install from an OCI registry.
- **nats-server 2.15.0 or later.** The API server refuses a `NatsCluster` whose `spec.version` is below 2.15.0.
- **cert-manager, optional.** Only the cluster controller uses it, and only for a `NatsCluster` that names `certManager` under `tls`, `routes.tls`, `gateway.tls` or `leafnodes.tls`. Without cert-manager, such a `NatsCluster` reports `Progressing` with the message `cert-manager Certificate is not a known kind: cert-manager is not installed`, and its servers wait for the certificate. Route TLS with no certificate named is self-signed and needs no cert-manager.

A `NatsCluster` runs its client listener, port 4222, in the clear unless `spec.tls` names a certificate, from a Secret or from cert-manager. With it, `status.endpoints.client` reads `tls://`, and every client, the JetStream and auth controllers' `NatsConnection`s included, needs the CA that issued the certificate under `tls.ca`; the cluster controller reads it from the certificate Secret's `ca.crt` itself.

## Install

Each release publishes the chart at `oci://ghcr.io/mikluko/nats-operator/charts/nats-operator`, with the release's version as both its `version` and `appVersion`, and the three images at `ghcr.io/mikluko/nats-operator/<controller>:<version>`, which the published chart pins by digest.

```sh
helm install nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
  --version <version> \
  --namespace nats-operator --create-namespace
```

All three controllers are enabled by default. To install a subset, switch the others off; the CRDs are installed either way:

```sh
helm install nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
  --version <version> \
  --namespace nats-operator --create-namespace \
  --set auth.enabled=false --set jetstream.enabled=false
```

The chart ships a values schema: a key it does not know, misspelled or not, fails `helm install`, `helm upgrade` and `helm lint`.

To check the installed controllers:

```sh
helm test nats-operator --namespace nats-operator --logs
```

For each enabled controller this starts the Pod `<release>-<controller>-test`, which GETs the controller's `/readyz` on port `8081` through the Service `<release>-<controller>-test`, with 30 tries, two seconds apart.

The auth controller has to stay up for the accounts it signs to keep working. Each account JWT expires its `jwtTTL` after it was signed, 48h by default, and is re-signed at half that, so an auth controller down for half a `jwtTTL` may let an account's JWT expire, and one down for a whole `jwtTTL` has let every one expire; the servers then close that account's connections. The gauge `nats_operator.account.jwt_expiry`, under [Telemetry]({{< relref "/docs/reference/telemetry#metrics" >}}), says when each current account JWT expires. An account with `jwtTTL: 0s` never expires.

Then [the quickstart]({{< relref "/docs/stories/01-quickstart" >}}) deploys a NATS cluster with JetStream.

## Values

| Value | Default | What it sets |
|---|---|---|
| `imagePullSecrets` | `[]` | Pull secrets of every controller's pod. |
| `leaderElection.enabled` | `true` | `--leader-elect` on every controller, and a Role on Leases in the release namespace. Keep it on with more than one replica. |
| `watchNamespaces` | `[]` | Namespaces every controller watches and reconciles in, passed as `--watch-namespaces`, with RBAC granted in them alone; see [RBAC](#rbac). Empty, every namespace. The namespace of `auth.systemConnection` must be among them. |
| `nodeSelector` | `{}` | Node selector of every controller's pod. |
| `annotations` | `{}` | Annotations of every controller's Deployment. |
| `podAnnotations` | `{}` | Annotations of every controller's pod, such as the OpenTelemetry Operator's injection annotations under [Telemetry]({{< relref "/docs/reference/telemetry#the-opentelemetry-operator" >}}). |
| `affinity` | `{}` | Affinity of every controller's pod. |
| `tolerations` | `[]` | Tolerations of every controller's pod. |
| `priorityClassName` | `""` | Priority class of every controller's pod. |
| `topologySpreadConstraints` | `[]` | Topology spread constraints of every controller's pod; a constraint's `labelSelector` is not filled in. |
| `extraArgs` | `[]` | Flags appended to every controller's, after the chart's own; of a flag given twice, the last wins. |
| `env` | `[]` | Environment of every controller's container. |
| `cluster.enabled` | `true` | Installs the cluster controller. |
| `cluster.replicas` | `1` | Replicas of its Deployment. |
| `cluster.image.repository` | `ghcr.io/mikluko/nats-operator/cluster-controller` | Its image. |
| `cluster.image.tag` | `""` | Its image tag; empty is the chart's `appVersion`. |
| `cluster.image.digest` | the release's image digest; `""` in the source tree | `sha256:<hex>` appended to its image reference as `@<digest>`, pinning the image. |
| `cluster.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |
| `cluster.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Its container's resources. |
| `cluster.nodeSelector` | `{}` | Its pod's node selector, each key set over `nodeSelector`. |
| `cluster.annotations` | `{}` | Its Deployment's annotations, each key set over `annotations`. |
| `cluster.podAnnotations` | `{}` | Its pod's annotations, each key set over `podAnnotations`. |
| `cluster.affinity` | `{}` | Its pod's affinity, each of `nodeAffinity`, `podAffinity` and `podAntiAffinity` replacing the one under `affinity` whole. |
| `cluster.tolerations` | `[]` | Its pod's tolerations; set, they replace `tolerations` whole. |
| `cluster.priorityClassName` | `""` | Its pod's priority class; set, it replaces `priorityClassName`. |
| `cluster.topologySpreadConstraints` | `[]` | Its pod's topology spread constraints; set, they replace `topologySpreadConstraints` whole. |
| `cluster.extraArgs` | `[]` | Flags appended to its own after `extraArgs`. |
| `cluster.env` | `[]` | Its container's environment, an entry replacing the one of the same name under `env`. |
| `auth.enabled` | `true` | Installs the auth controller. |
| `auth.replicas` | `1` | Replicas of its Deployment. |
| `auth.systemConnection` | `""` | `--system-connection` of the auth controller, as `namespace/name`; empty, the flag is not passed, no server receives an account JWT and a deleted `NatsUser` keeps its connections; accounts and users then read `Distributed` `False`, reason `NoSystemConnection`. |
| `auth.image.repository` | `ghcr.io/mikluko/nats-operator/auth-controller` | Its image. |
| `auth.image.tag` | `""` | Its image tag; empty is the chart's `appVersion`. |
| `auth.image.digest` | the release's image digest; `""` in the source tree | `sha256:<hex>` appended to its image reference as `@<digest>`, pinning the image. |
| `auth.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |
| `auth.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Its container's resources. |
| `auth.nodeSelector` | `{}` | Its pod's node selector, each key set over `nodeSelector`. |
| `auth.annotations` | `{}` | Its Deployment's annotations, each key set over `annotations`. |
| `auth.podAnnotations` | `{}` | Its pod's annotations, each key set over `podAnnotations`. |
| `auth.affinity` | `{}` | Its pod's affinity, each of `nodeAffinity`, `podAffinity` and `podAntiAffinity` replacing the one under `affinity` whole. |
| `auth.tolerations` | `[]` | Its pod's tolerations; set, they replace `tolerations` whole. |
| `auth.priorityClassName` | `""` | Its pod's priority class; set, it replaces `priorityClassName`. |
| `auth.topologySpreadConstraints` | `[]` | Its pod's topology spread constraints; set, they replace `topologySpreadConstraints` whole. |
| `auth.extraArgs` | `[]` | Flags appended to its own after `extraArgs`. |
| `auth.env` | `[]` | Its container's environment, an entry replacing the one of the same name under `env`. |
| `jetstream.enabled` | `true` | Installs the JetStream controller. |
| `jetstream.replicas` | `1` | Replicas of its Deployment. |
| `jetstream.image.repository` | `ghcr.io/mikluko/nats-operator/jetstream-controller` | Its image. |
| `jetstream.image.tag` | `""` | Its image tag; empty is the chart's `appVersion`. |
| `jetstream.image.digest` | the release's image digest; `""` in the source tree | `sha256:<hex>` appended to its image reference as `@<digest>`, pinning the image. |
| `jetstream.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |
| `jetstream.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Its container's resources. |
| `jetstream.nodeSelector` | `{}` | Its pod's node selector, each key set over `nodeSelector`. |
| `jetstream.annotations` | `{}` | Its Deployment's annotations, each key set over `annotations`. |
| `jetstream.podAnnotations` | `{}` | Its pod's annotations, each key set over `podAnnotations`. |
| `jetstream.affinity` | `{}` | Its pod's affinity, each of `nodeAffinity`, `podAffinity` and `podAntiAffinity` replacing the one under `affinity` whole. |
| `jetstream.tolerations` | `[]` | Its pod's tolerations; set, they replace `tolerations` whole. |
| `jetstream.priorityClassName` | `""` | Its pod's priority class; set, it replaces `priorityClassName`. |
| `jetstream.topologySpreadConstraints` | `[]` | Its pod's topology spread constraints; set, they replace `topologySpreadConstraints` whole. |
| `jetstream.extraArgs` | `[]` | Flags appended to its own after `extraArgs`. |
| `jetstream.env` | `[]` | Its container's environment, an entry replacing the one of the same name under `env`. |
| `metrics.scraper.serviceAccount` | `""` | A ServiceAccount, as `namespace/name`, granted the controllers' metrics under [RBAC](#rbac); empty, the chart grants them to no one. |
| `metrics.service.enabled` | `false` | A Service `<release>-<controller>-metrics` per enabled controller, port `metrics` (`8080`) onto its metrics endpoint. |
| `metrics.prometheus.enabled` | `false` | Each controller's OpenTelemetry metrics served over plain HTTP on the pod IP's port `9464`, as port `otel-metrics` of its container, its metrics Service and its ServiceMonitor; sets `OTEL_METRICS_EXPORTER=prometheus` and `OTEL_EXPORTER_PROMETHEUS_HOST=0.0.0.0`, which an `env` entry of the same name replaces. Requires `metrics.service.enabled`. See [Metrics](#metrics). |
| `metrics.serviceMonitor.enabled` | `false` | A prometheus-operator `ServiceMonitor` `<release>-<controller>-metrics` per enabled controller, over that Service; requires `metrics.service.enabled` and the `monitoring.coreos.com/v1` CRDs. See [Metrics](#metrics). |
| `metrics.serviceMonitor.labels` | `{}` | Labels of each ServiceMonitor, for a Prometheus that selects them by label. |
| `metrics.serviceMonitor.interval` | `""` | Scrape interval of each ServiceMonitor; empty, Prometheus's own. |
| `tests.image.repository` | `busybox` | Image of the `helm test` pods. |
| `tests.image.tag` | `"1.37.0"` | Its image tag. |
| `tests.image.digest` | `""` | `sha256:<hex>` appended to its image reference as `@<digest>`. |
| `tests.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |

## Controller flags

The chart runs every controller with `--leader-elect` set from `leaderElection.enabled`, `--leader-election-id` set to its own API group, metrics on `:8080` (container port `metrics`) and health probes on `:8081` (`/healthz`, `/readyz`), followed by `extraArgs` and the controller's own `extraArgs`. It creates a Service for the metrics port only while `metrics.service.enabled` is set.

The flags below are the binaries' own. Of those not named above, the chart sets only `--system-connection`, from `auth.systemConnection`, and `--watch-namespaces`, from `watchNamespaces`:

| Flag | Controller | Default | What it does |
|---|---|---|---|
| `--metrics-bind-address` | all | `:8080` | Address controller-runtime's Prometheus metrics are served on over HTTPS, with a self-signed certificate, at `/metrics`. A request needs a bearer token the API server authenticates, of a user allowed `get` on the non-resource URL `/metrics`; a token's identity is cached for a minute, an allow for five and a denial for thirty seconds. `0` disables it. |
| `--health-probe-bind-address` | all | `:8081` | Address of `/healthz`, which always passes, and `/readyz`, which passes once the controller's cache is synced. |
| `--leader-elect` | all | `false` | Leader election, so that one replica reconciles. |
| `--leader-election-id` | all | the controller's API group | Name of the leader election lease. |
| `--system-connection` | auth | unset | `namespace/name` of a `NatsConnection` whose creds are a user of a `NatsOperator`'s system account holding the `auth-controller` preset. Through it, account JWTs are pushed to the servers' resolvers and deleted from them, and a deleted user's connections are kicked. Unset, JWTs are signed and written to status, nothing reaches the servers, a deleted user's connections stay open, and accounts and users read `Distributed` `False`, reason `NoSystemConnection`. |
| `--watch-namespaces` | all | unset | Comma-separated namespaces the controller watches and reconciles in; an object elsewhere is never reconciled, and a reference into a namespace outside them does not resolve. Unset, every namespace. |
| `--resync-period` | JetStream | `10m` | How often a JetStream resource is compared to its server object. |
| `--zap-log-level`, `--zap-encoder`, `--zap-devel`, `--zap-stacktrace-level`, `--zap-time-encoding` | all | production logging, JSON at `info` | Logging. |

## Metrics

Each controller serves its metrics over HTTPS on port `8080` under a self-signed certificate it generates at start, so a scraper cannot verify it and must skip verification; what authenticates the scrape is the bearer token, which must be that of a ServiceAccount allowed `get` on `/metrics`, such as `metrics.scraper.serviceAccount`. With `metrics.serviceMonitor.enabled`, each ServiceMonitor scrapes with `scheme: https`, `tlsConfig.insecureSkipVerify: true` and the Prometheus pod's own ServiceAccount token, read from `/var/run/secrets/kubernetes.io/serviceaccount/token`; set `metrics.scraper.serviceAccount` to that ServiceAccount.

Port `8080` serves controller-runtime's metrics only. The controllers' own instruments, `nats_operator.account.jwt_expiry` among them, are OpenTelemetry metrics under [Telemetry]({{< relref "/docs/reference/telemetry#metrics" >}}); with `metrics.prometheus.enabled`, each controller serves them at `/metrics` on port `9464` over plain HTTP to any client that reaches the pod, and each ServiceMonitor scrapes that port with `scheme: http` and no token.

## RBAC

Each controller's ClusterRole is named `<release>-<controller>`, for example `nats-operator-cluster-controller`, and is bound to the ServiceAccount of the same name in the release namespace. No controller can write another controller's API group. While `leaderElection.enabled` is on, each also gets the Role `<release>-<controller>-leader-election` in the release namespace: every verb on `coordination.k8s.io` `leases`, and `create` and `patch` on `""` `events`.

While `watchNamespaces` is set, each controller's ClusterRole holds only `create` on `authentication.k8s.io` `tokenreviews` and `authorization.k8s.io` `subjectaccessreviews`, which its metrics endpoint needs, and every other rule in the tables below goes to a Role `<release>-<controller>`, with a RoleBinding of the same name, in each namespace `watchNamespaces` names.

While `metrics.scraper.serviceAccount` is set, the ClusterRole `<release>-metrics-scraper` holds `get` on the non-resource URL `/metrics` and is bound to that ServiceAccount, whose token then reads every controller's metrics. The same grant reads the Kubernetes API server's own `/metrics`.

### Every controller

| API group | Resources | Verbs |
|---|---|---|
| `authentication.k8s.io` | `tokenreviews` | `create` |
| `authorization.k8s.io` | `subjectaccessreviews` | `create` |
| `events.k8s.io` | `events` | `create`, `patch` |

### Cluster controller

| API group | Resources | Verbs |
|---|---|---|
| `""` | `configmaps` | `get`, `list`, `watch`, `create`, `update`, `patch`, `delete` |
| `""` | `persistentvolumeclaims` | `get`, `list`, `watch`, `delete` |
| `""` | `secrets`, `services` | `get`, `list`, `watch`, `create`, `update`, `delete` |
| `apps` | `statefulsets` | `list`, `watch`, `create`, `update`, `patch`, `delete` |
| `cert-manager.io` | `certificates` | `get`, `create`, `update`, `delete` |
| `cluster.nats.mikluko.io` | `natsclusters` | `get`, `list`, `watch`, `patch` |
| `cluster.nats.mikluko.io` | `natsclusters/finalizers` | `update` |
| `cluster.nats.mikluko.io` | `natsclusters/status` | `patch` |
| `nats.mikluko.io` | `natsaccounttrusts`, `natsconnections`, `natsoperatortrusts` | `get`, `list`, `watch` |
| `nats.mikluko.io` | `natsreferencegrants` | `list`, `watch` |
| `networking.k8s.io` | `networkpolicies` | `get`, `list`, `watch`, `create`, `update`, `delete` |
| `policy` | `poddisruptionbudgets` | `get`, `list`, `watch`, `create`, `update` |

### Auth controller

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

| API group | Resources | Verbs |
|---|---|---|
| `""` | `secrets` | `get`, `list`, `watch` |
| `jetstream.nats.mikluko.io` | `natsbalancers`, `natssystembalancers` | `get`, `list`, `watch` |
| `jetstream.nats.mikluko.io` | `natsbalancers/status`, `natsclusterevacuations/status`, `natsconsumers/status`, `natskeyvalues/status`, `natsobjectstores/status`, `natsstreams/status`, `natssystembalancers/status` | `patch` |
| `jetstream.nats.mikluko.io` | `natsclusterevacuations`, `natsconsumers`, `natskeyvalues`, `natsobjectstores`, `natsstreams` | `get`, `list`, `watch`, `patch` |
| `nats.mikluko.io` | `natsconnections` | `get`, `list`, `watch` |
| `nats.mikluko.io` | `natsconnections/status` | `patch` |
| `nats.mikluko.io` | `natsreferencegrants` | `list`, `watch` |

## Upgrade

Helm installs the chart's `crds/` on first install only, and never upgrades or deletes them. Apply the new version's CRDs first, then upgrade the release:

```sh
helm pull oci://ghcr.io/mikluko/nats-operator/charts/nats-operator --version <version> --untar
kubectl apply --server-side --force-conflicts -f nats-operator/crds
helm upgrade nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
  --version <version> --namespace nats-operator --reset-then-reuse-values
```

## Uninstall

```sh
helm uninstall nats-operator --namespace nats-operator
```

This removes the controllers' Deployments, ServiceAccounts and RBAC. It leaves behind:

- the CRDs, and with them every custom resource and everything the controllers created for them: StatefulSets, Services, ConfigMaps, Secrets, PodDisruptionBudgets, NetworkPolicies and cert-manager Certificates;
- after a `helm test`, its Pods and Services `<release>-<controller>-test`;
- with leader election on, the Leases `cluster.nats.mikluko.io`, `auth.nats.mikluko.io` and `jetstream.nats.mikluko.io` in the release namespace.

The seed Secrets the auth controller generates, `<name>-<operator|systemaccount|account>-<identity|signing-1>`, outlive their objects, the CRDs' deletion included: applying the objects again takes the same keys. Deleting those Secrets discards the NATS operator and account identities for good.

Some kinds carry a finalizer that only their controller removes, so delete them while it still runs:

| Kind | Finalizer | Held by |
|---|---|---|
| `NatsCluster` with JetStream | `cluster.nats.mikluko.io/jetstream-data` | cluster controller |
| `NatsAccount` | `auth.nats.mikluko.io/delete` | auth controller |
| `NatsUser` | `auth.nats.mikluko.io/revoke` | auth controller |
| `NatsStream`, `NatsConsumer`, `NatsKeyValue`, `NatsObjectStore`, `NatsClusterEvacuation` | `jetstream.nats.mikluko.io/finalizer` | JetStream controller |

Deleting the CRDs, from the chart pulled as under [Upgrade](#upgrade), deletes every custom resource of their kinds, and hangs on the same finalizers once the controllers are gone:

```sh
kubectl delete -f nats-operator/crds
```
