---
title: Install
weight: 1
description: Install the chart, pick the controllers, and upgrade or uninstall them.
---

The Helm chart `nats-operator` installs the CRDs of all four API groups and any subset of the three controllers: cluster, auth and JetStream. Each enabled controller gets its own ServiceAccount, ClusterRole and Deployment.

## Prerequisites

- **Kubernetes 1.29 or later.** The chart declares `kubeVersion: ">=1.29.0-0"`, which the collector's native sidecar needs; Helm refuses to install it on an older cluster.
- **Helm**, to install from an OCI registry.
- **nats-server 2.15.0 or later.** The API server refuses a `NatsCluster` whose `spec.version` is below 2.15.0.
- **cert-manager, optional.** Only the cluster controller uses it, and only for a `NatsCluster` that names `certManager` under `routes.tls`, `gateway.tls` or `leafnodes.tls`. Without cert-manager, such a `NatsCluster` reports `Progressing` with the message `cert-manager Certificate is not a known kind: cert-manager is not installed`, and its servers wait for the certificate. Route TLS with no certificate named is self-signed and needs no cert-manager.

## Install

Each release publishes the chart at `oci://ghcr.io/mikluko/nats-operator/charts/nats-operator`, with the release's version as both its `version` and `appVersion`, and the three images at `ghcr.io/mikluko/nats-operator/<controller>:<version>`.

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

For each enabled controller this starts the Pod `<release>-<controller>-test`, which GETs the controller's `/healthz` on port `8081` through the Service `<release>-<controller>-test` and, with `telemetry.prometheus.enabled`, `/metrics` through `<release>-<controller>-prometheus`. Each URL gets 30 tries, two seconds apart. The test Pods and Services stay until the next `helm test` replaces them.

Then [the quickstart]({{< relref "/docs/stories/01-quickstart" >}}) deploys a NATS cluster with JetStream.

## Values

| Value | Default | What it sets |
|---|---|---|
| `imagePullSecrets` | `[]` | Pull secrets of every controller's pod. |
| `leaderElection.enabled` | `true` | `--leader-elect` on every controller, and a Role on Leases in the release namespace. Keep it on with more than one replica. |
| `cluster.enabled` | `true` | Installs the cluster controller. |
| `cluster.replicas` | `1` | Replicas of its Deployment. |
| `cluster.image.repository` | `ghcr.io/mikluko/nats-operator/cluster-controller` | Its image. |
| `cluster.image.tag` | `""` | Its image tag; empty is the chart's `appVersion`. |
| `cluster.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |
| `cluster.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Its container's resources. |
| `auth.enabled` | `true` | Installs the auth controller. |
| `auth.replicas` | `1` | Replicas of its Deployment. |
| `auth.systemConnection` | `""` | `--system-connection` of the auth controller, as `namespace/name`; empty, the flag is not passed. |
| `auth.image.repository` | `ghcr.io/mikluko/nats-operator/auth-controller` | Its image. |
| `auth.image.tag` | `""` | Its image tag; empty is the chart's `appVersion`. |
| `auth.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |
| `auth.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Its container's resources. |
| `jetstream.enabled` | `true` | Installs the JetStream controller. |
| `jetstream.replicas` | `1` | Replicas of its Deployment. |
| `jetstream.image.repository` | `ghcr.io/mikluko/nats-operator/jetstream-controller` | Its image. |
| `jetstream.image.tag` | `""` | Its image tag; empty is the chart's `appVersion`. |
| `jetstream.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |
| `jetstream.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Its container's resources. |
| `tests.image.repository` | `busybox` | Image of the `helm test` pods. |
| `tests.image.tag` | `"1.37.0"` | Its image tag. |
| `tests.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |
| `telemetry.env` | `[]` | Environment variables appended to every controller's container, such as the OpenTelemetry SDK's `OTEL_*` settings. |
| `telemetry.collector.enabled` | `true` | Runs an OpenTelemetry Collector as a native sidecar in every controller's pod, and sets `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318` on every controller; a change to `telemetry.collector.config` rolls the Deployments. |
| `telemetry.collector.image.repository` | `otel/opentelemetry-collector` | Its image. |
| `telemetry.collector.image.tag` | `"0.161.0"` | Its image tag. |
| `telemetry.collector.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |
| `telemetry.collector.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Its container's resources. |
| `telemetry.collector.config` | `{receivers: {otlp: {protocols: {grpc: {endpoint: "localhost:4317"}, http: {endpoint: "localhost:4318"}}}}, processors: {batch: {}}, exporters: {nop: {}}, service: {pipelines: {traces: {receivers: [otlp], processors: [batch], exporters: [nop]}, metrics: {receivers: [otlp], processors: [batch], exporters: [nop]}, logs: {receivers: [otlp], processors: [batch], exporters: [nop]}}}}` | The whole collector config, in the ConfigMap `<release>-otel-collector`. The default receives OTLP over gRPC on `localhost:4317` and over HTTP on `localhost:4318`, and discards it; replace its exporters to send it to a backend. |
| `telemetry.prometheus.enabled` | `false` | Sets `OTEL_METRICS_EXPORTER=prometheus`, `OTEL_EXPORTER_PROMETHEUS_HOST=0.0.0.0` and `OTEL_EXPORTER_PROMETHEUS_PORT` on every controller, which then serves its metrics for Prometheus to pull instead of exporting them over OTLP; adds the container port `prometheus` and the Service `<release>-<controller>-prometheus`. |
| `telemetry.prometheus.port` | `9464` | The listener's port, on the container and the Service. |
| `telemetry.prometheus.serviceMonitor.enabled` | `false` | With `telemetry.prometheus.enabled`, a `monitoring.coreos.com/v1` ServiceMonitor `<release>-<controller>` on that Service. |
| `telemetry.prometheus.serviceMonitor.labels` | `{}` | Labels added to each ServiceMonitor. |

## Controller flags

The chart runs every controller with `--leader-elect` set from `leaderElection.enabled`, `--leader-election-id` set to its own API group, metrics on `:8080` (container port `metrics`) and health probes on `:8081` (`/healthz`, `/readyz`). It creates no Service for the metrics port.

The flags below are the binaries' own. The chart sets only `--system-connection`, from `auth.systemConnection`:

| Flag | Controller | Default | What it does |
|---|---|---|---|
| `--system-connection` | auth | unset | `namespace/name` of a `NatsConnection` whose creds are a user of a `NatsOperator`'s system account holding the `auth-controller` preset. Through it, account JWTs are pushed to the servers' resolvers and deleted from them, and a deleted user's connections are kicked. Unset, JWTs are signed and written to status, and nothing reaches the servers. |
| `--resync-period` | JetStream | `10m` | How often a JetStream resource is compared to its server object. |
| `--zap-log-level`, `--zap-encoder`, `--zap-devel`, `--zap-stacktrace-level`, `--zap-time-encoding` | all | production logging, JSON at `info` | Logging. |

## RBAC

Each controller's ClusterRole is named `<release>-<controller>`, for example `nats-operator-cluster-controller`, and is bound to the ServiceAccount of the same name in the release namespace. No controller can write another controller's API group. While `leaderElection.enabled` is on, each also gets the Role `<release>-<controller>-leader-election` on `coordination.k8s.io` `leases` in the release namespace, with every verb.

### Every controller

| API group | Resources | Verbs |
|---|---|---|
| `""` | `events` | `create`, `patch` |
| `events.k8s.io` | `events` | `create`, `patch` |

### Cluster controller

| API group | Resources | Verbs |
|---|---|---|
| `cluster.nats.mikluko.io` | `natsclusters` | `get`, `list`, `watch`, `update`, `patch` |
| `cluster.nats.mikluko.io` | `natsclusters/status` | `get`, `update`, `patch` |
| `cluster.nats.mikluko.io` | `natsclusters/finalizers` | `update` |
| `nats.mikluko.io` | `natsoperatortrusts`, `natsaccounttrusts`, `natsconnections`, `natsreferencegrants` | `get`, `list`, `watch` |
| `apps` | `statefulsets` | `get`, `list`, `watch`, `create`, `update`, `patch`, `delete` |
| `""` | `configmaps`, `services`, `secrets` | `get`, `list`, `watch`, `create`, `update`, `patch`, `delete` |
| `policy` | `poddisruptionbudgets` | `get`, `list`, `watch`, `create`, `update`, `patch`, `delete` |
| `cert-manager.io` | `certificates` | `get`, `list`, `watch`, `create`, `update`, `patch` |
| `""` | `pods` | `get`, `list`, `watch` |
| `""` | `persistentvolumeclaims` | `get`, `list`, `watch`, `delete` |

### Auth controller

| API group | Resources | Verbs |
|---|---|---|
| `auth.nats.mikluko.io` | `natsoperators`, `natssystemaccounts`, `natsaccounts`, `natsusers` | `get`, `list`, `watch`, `update`, `patch` |
| `auth.nats.mikluko.io` | `natsoperators/status`, `natssystemaccounts/status`, `natsaccounts/status`, `natsusers/status` | `get`, `update`, `patch` |
| `auth.nats.mikluko.io` | `natsoperators/finalizers`, `natssystemaccounts/finalizers`, `natsaccounts/finalizers`, `natsusers/finalizers` | `update` |
| `nats.mikluko.io` | `natsoperatortrusts`, `natsaccounttrusts`, `natsconnections`, `natsreferencegrants` | `get`, `list`, `watch` |
| `nats.mikluko.io` | `natsoperatortrusts/status`, `natsaccounttrusts/status` | `get`, `update`, `patch` |
| `""` | `secrets` | `get`, `list`, `watch`, `create`, `update`, `patch`, `delete` |

### JetStream controller

| API group | Resources | Verbs |
|---|---|---|
| `jetstream.nats.mikluko.io` | `natsstreams`, `natsconsumers`, `natskeyvalues`, `natsobjectstores`, `natsbalancers`, `natssystembalancers`, `natsclusterevacuations` | `get`, `list`, `watch`, `update`, `patch` |
| `jetstream.nats.mikluko.io` | `natsstreams/status`, `natsconsumers/status`, `natskeyvalues/status`, `natsobjectstores/status`, `natsbalancers/status`, `natssystembalancers/status`, `natsclusterevacuations/status` | `get`, `update`, `patch` |
| `jetstream.nats.mikluko.io` | `natsstreams/finalizers`, `natsconsumers/finalizers`, `natskeyvalues/finalizers`, `natsobjectstores/finalizers`, `natsbalancers/finalizers`, `natssystembalancers/finalizers`, `natsclusterevacuations/finalizers` | `update` |
| `nats.mikluko.io` | `natsconnections`, `natsreferencegrants` | `get`, `list`, `watch` |
| `nats.mikluko.io` | `natsconnections/status` | `get`, `update`, `patch` |
| `""` | `secrets` | `get`, `list`, `watch` |

## Upgrade

Helm installs the chart's `crds/` on first install only, and never upgrades or deletes them. Apply the new version's CRDs first, then upgrade the release:

```sh
helm pull oci://ghcr.io/mikluko/nats-operator/charts/nats-operator --version <version> --untar
kubectl apply --server-side --force-conflicts -f nats-operator/crds
helm upgrade nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
  --version <version> --namespace nats-operator --reuse-values
```

## Uninstall

```sh
helm uninstall nats-operator --namespace nats-operator
```

This removes the controllers' Deployments, ServiceAccounts and RBAC. It leaves behind:

- the CRDs, and with them every custom resource and everything the controllers created for them: StatefulSets, Services, ConfigMaps, Secrets, PodDisruptionBudgets and cert-manager Certificates;
- after a `helm test`, its Pods and Services `<release>-<controller>-test`;
- with leader election on, the Leases `cluster.nats.mikluko.io`, `auth.nats.mikluko.io` and `jetstream.nats.mikluko.io` in the release namespace.

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
