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

For each enabled controller this starts the Pod `<release>-<controller>-test`, which GETs the controller's `/readyz` on port `8081` through the Service `<release>-<controller>-test`, with 30 tries, two seconds apart.

The auth controller has to stay up for the accounts it signs to keep working. Each account JWT expires its `jwtTTL` after it was signed, 48h by default, and is re-signed at half that, so an auth controller down for half a `jwtTTL` may let an account's JWT expire, and one down for a whole `jwtTTL` has let every one expire; the servers then close that account's connections. The gauge `nats_operator.account.jwt_expiry`, under [Telemetry]({{< relref "/docs/reference/telemetry#metrics" >}}), says when each current account JWT expires. An account with `jwtTTL: 0s` never expires.

Then [the quickstart]({{< relref "/docs/stories/01-quickstart" >}}) deploys a NATS cluster with JetStream.

## Values

| Value | Default | What it sets |
|---|---|---|
| `imagePullSecrets` | `[]` | Pull secrets of every controller's pod. |
| `leaderElection.enabled` | `true` | `--leader-elect` on every controller, and a Role on Leases in the release namespace. Keep it on with more than one replica. |
| `nodeSelector` | `{}` | Node selector of every controller's pod. |
| `annotations` | `{}` | Annotations of every controller's Deployment. |
| `podAnnotations` | `{}` | Annotations of every controller's pod, such as the OpenTelemetry Operator's injection annotations under [Telemetry]({{< relref "/docs/reference/telemetry#the-opentelemetry-operator" >}}). |
| `affinity` | `{}` | Affinity of every controller's pod. |
| `cluster.enabled` | `true` | Installs the cluster controller. |
| `cluster.replicas` | `1` | Replicas of its Deployment. |
| `cluster.image.repository` | `ghcr.io/mikluko/nats-operator/cluster-controller` | Its image. |
| `cluster.image.tag` | `""` | Its image tag; empty is the chart's `appVersion`. |
| `cluster.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |
| `cluster.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Its container's resources. |
| `cluster.nodeSelector` | `{}` | Its pod's node selector, each key set over `nodeSelector`. |
| `cluster.annotations` | `{}` | Its Deployment's annotations, each key set over `annotations`. |
| `cluster.podAnnotations` | `{}` | Its pod's annotations, each key set over `podAnnotations`. |
| `cluster.affinity` | `{}` | Its pod's affinity, each of `nodeAffinity`, `podAffinity` and `podAntiAffinity` replacing the one under `affinity` whole. |
| `auth.enabled` | `true` | Installs the auth controller. |
| `auth.replicas` | `1` | Replicas of its Deployment. |
| `auth.systemConnection` | `""` | `--system-connection` of the auth controller, as `namespace/name`; empty, the flag is not passed, no server receives an account JWT and a deleted `NatsUser` keeps its connections; accounts and users then read `Distributed` `False`, reason `NoSystemConnection`. |
| `auth.image.repository` | `ghcr.io/mikluko/nats-operator/auth-controller` | Its image. |
| `auth.image.tag` | `""` | Its image tag; empty is the chart's `appVersion`. |
| `auth.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |
| `auth.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Its container's resources. |
| `auth.nodeSelector` | `{}` | Its pod's node selector, each key set over `nodeSelector`. |
| `auth.annotations` | `{}` | Its Deployment's annotations, each key set over `annotations`. |
| `auth.podAnnotations` | `{}` | Its pod's annotations, each key set over `podAnnotations`. |
| `auth.affinity` | `{}` | Its pod's affinity, each of `nodeAffinity`, `podAffinity` and `podAntiAffinity` replacing the one under `affinity` whole. |
| `jetstream.enabled` | `true` | Installs the JetStream controller. |
| `jetstream.replicas` | `1` | Replicas of its Deployment. |
| `jetstream.image.repository` | `ghcr.io/mikluko/nats-operator/jetstream-controller` | Its image. |
| `jetstream.image.tag` | `""` | Its image tag; empty is the chart's `appVersion`. |
| `jetstream.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |
| `jetstream.resources` | `{requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}` | Its container's resources. |
| `jetstream.nodeSelector` | `{}` | Its pod's node selector, each key set over `nodeSelector`. |
| `jetstream.annotations` | `{}` | Its Deployment's annotations, each key set over `annotations`. |
| `jetstream.podAnnotations` | `{}` | Its pod's annotations, each key set over `podAnnotations`. |
| `jetstream.affinity` | `{}` | Its pod's affinity, each of `nodeAffinity`, `podAffinity` and `podAntiAffinity` replacing the one under `affinity` whole. |
| `metrics.scraper.serviceAccount` | `""` | A ServiceAccount, as `namespace/name`, granted the controllers' metrics under [RBAC](#rbac); empty, the chart grants them to no one. |
| `tests.image.repository` | `busybox` | Image of the `helm test` pods. |
| `tests.image.tag` | `"1.37.0"` | Its image tag. |
| `tests.image.pullPolicy` | `IfNotPresent` | Its image pull policy. |

## Controller flags

The chart runs every controller with `--leader-elect` set from `leaderElection.enabled`, `--leader-election-id` set to its own API group, metrics on `:8080` (container port `metrics`) and health probes on `:8081` (`/healthz`, `/readyz`). It creates no Service for the metrics port.

The flags below are the binaries' own. The chart sets only `--system-connection`, from `auth.systemConnection`:

| Flag | Controller | Default | What it does |
|---|---|---|---|
| `--metrics-bind-address` | all | `:8080` | Address controller-runtime's Prometheus metrics are served on over HTTPS, with a self-signed certificate, at `/metrics`. A request needs a bearer token the API server authenticates, of a user allowed `get` on the non-resource URL `/metrics`; every request costs a `TokenReview` and a `SubjectAccessReview`. `0` disables it. |
| `--health-probe-bind-address` | all | `:8081` | Address of `/healthz`, which always passes, and `/readyz`, which passes once the controller's cache is synced. |
| `--leader-elect` | all | `false` | Leader election, so that one replica reconciles. |
| `--leader-election-id` | all | the controller's API group | Name of the leader election lease. |
| `--system-connection` | auth | unset | `namespace/name` of a `NatsConnection` whose creds are a user of a `NatsOperator`'s system account holding the `auth-controller` preset. Through it, account JWTs are pushed to the servers' resolvers and deleted from them, and a deleted user's connections are kicked. Unset, JWTs are signed and written to status, nothing reaches the servers, a deleted user's connections stay open, and accounts and users read `Distributed` `False`, reason `NoSystemConnection`. |
| `--resync-period` | JetStream | `10m` | How often a JetStream resource is compared to its server object. |
| `--zap-log-level`, `--zap-encoder`, `--zap-devel`, `--zap-stacktrace-level`, `--zap-time-encoding` | all | production logging, JSON at `info` | Logging. |

## RBAC

Each controller's ClusterRole is named `<release>-<controller>`, for example `nats-operator-cluster-controller`, and is bound to the ServiceAccount of the same name in the release namespace. No controller can write another controller's API group. While `leaderElection.enabled` is on, each also gets the Role `<release>-<controller>-leader-election` in the release namespace: every verb on `coordination.k8s.io` `leases`, and `create` and `patch` on `""` `events`.

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
  --version <version> --namespace nats-operator --reuse-values
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
