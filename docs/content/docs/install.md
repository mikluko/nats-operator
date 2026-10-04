---
title: Install
weight: 1
description: Install the chart with all three controllers or a subset, check it, upgrade it and uninstall it.
---

This guide shows you how to install the Helm chart `nats-operator`, upgrade it and uninstall it.
The chart installs the CRDs of all four API groups and the controllers that you enable: cluster, auth and JetStream.

[Chart and controller flags]({{< relref "/docs/reference/chart" >}}) lists every value of the chart, the flags of the controllers and the RBAC that each controller is granted.

## Before you begin

You need:

- Kubernetes 1.33 or later.
  The chart declares `kubeVersion: ">=1.33.0-0"`, and Helm refuses to install it on an older Kubernetes cluster.
- Helm 3.14 or later.
  The upgrade uses `helm upgrade --reset-then-reuse-values`, which Helm has had since 3.14.
- nats-server 2.15.0 or later for the NATS clusters that you deploy.
  The API server refuses a `NatsCluster` whose `spec.version` is below 2.15.0.
- Optional: cert-manager.
  Only the cluster controller uses it, and only for a `NatsCluster` that has `certManager` under `tls`, `routes.tls`, `gateway.tls` or `leafnodes.tls`.
  Without cert-manager, such a `NatsCluster` has the condition `Progressing` with the message `cert-manager Certificate is not a known kind: cert-manager is not installed`, and its servers wait for the certificate.
  Route TLS with no certificate set is self-signed and needs no cert-manager.

## Install

1. Choose what to install.

   To install all three controllers, run this command with `<version>` replaced by a release version, such as `0.2.0`:

   ```sh
   helm install nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
     --version <version> \
     --namespace nats-operator --create-namespace
   ```

   To install a subset, set the `enabled` value of each controller that you leave out to `false`.
   This command installs the cluster controller alone:

   ```sh
   helm install nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
     --version <version> \
     --namespace nats-operator --create-namespace \
     --set auth.enabled=false --set jetstream.enabled=false
   ```

   The chart installs the CRDs with either command.

   If you choose another release name, see [Release name]({{< relref "/docs/reference/chart#release-name" >}}) for its longest length.
   To set other values, see [Values]({{< relref "/docs/reference/chart#values" >}}).
   Helm fails on a key that the chart does not have.

1. Check the installed controllers:

   ```sh
   helm test nats-operator --namespace nats-operator --logs
   ```

   The test tries again while a controller starts.
   The end of the output is similar to this:

   ```text
   POD LOGS: nats-operator-auth-controller-test (check)
   ok http://nats-operator-auth-controller-test.nats-operator.svc:8081/readyz

   POD LOGS: nats-operator-cluster-controller-test (check)
   ok http://nats-operator-cluster-controller-test.nats-operator.svc:8081/readyz

   POD LOGS: nats-operator-jetstream-controller-test (check)
   ok http://nats-operator-jetstream-controller-test.nats-operator.svc:8081/readyz
   ```

   A line `wget: can't connect to remote host` before an `ok` line is a try that ran before the controller was ready.
   [Helm test]({{< relref "/docs/reference/chart#helm-test" >}}) describes what the test does.

## After you install

To deploy a NATS cluster with JetStream next, follow [the quickstart]({{< relref "/docs/stories/01-quickstart" >}}).

### Keep the auth controller running

The accounts that the auth controller signs work only while it keeps signing them.
Each account JWT expires its `jwtTTL` after it was signed, which is 48h by default, and the auth controller signs it again at half that time.
If the auth controller is down for half a `jwtTTL`, the JWT of an account can expire.
If it is down for a whole `jwtTTL`, the JWT of every account has expired.
The servers close the connections of an account whose JWT has expired.
[Account JWT expiry]({{< relref "/docs/reference/telemetry#account-jwt-expiry" >}}) has an alert for it.

### Client TLS

A `NatsCluster` serves its client listener, port 4222, without TLS unless `spec.tls` sets a certificate, from a Secret or from cert-manager.
With `spec.tls` set, `status.endpoints.client` starts with `tls://`.
Every client then needs the CA that issued the certificate, and that includes each `NatsConnection` that the JetStream controller and the auth controller use, which takes it under `tls.ca`.
The cluster controller takes the CA from `ca.crt` of the certificate Secret.

## Upgrade

Helm installs the CRDs in the chart's `crds/` directory on the first install only.
It never upgrades or deletes them, so you apply the CRDs of the new version yourself, before you upgrade the release.

1. Pull the chart of the new version:

   ```sh
   helm pull oci://ghcr.io/mikluko/nats-operator/charts/nats-operator --version <version> --untar
   ```

   Helm unpacks the chart into the directory `nats-operator`.

1. Apply the CRDs of the new version:

   ```sh
   kubectl apply --server-side --force-conflicts -f nats-operator/crds
   ```

   The output has one line for each CRD, and each line ends with `serverside-applied`.

1. Upgrade the release:

   ```sh
   helm upgrade nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
     --version <version> --namespace nats-operator --reset-then-reuse-values
   ```

   The release keeps the values that you set when you installed it.

1. Check the upgraded controllers:

   ```sh
   helm test nats-operator --namespace nats-operator --logs
   ```

### Server restarts after an upgrade

An upgrade of the cluster controller can restart or reload the servers of every NATS cluster:

- If the new cluster controller renders the StatefulSet of a NATS server differently, for example with a new default exporter image, it restarts every NATS server.
  The same applies if it changes the server config under a key that nats-server does not reload.
  The servers restart one at a time, and each restart waits for the gate of the rollout.
- For every other config change, it reloads every server at once and does not wait for the gate.
- If a `NatsCluster` has no `auth.systemCredentials`, or the reload of a server fails, the server restarts instead, and the restart waits for the gate.

To hold the restarts of a NATS cluster, set `spec.rollout.paused` on its `NatsCluster`.
The restarts stop before the next server until you unset it.
[Rollout]({{< relref "/docs/design/v1#44-rollout" >}}) in the design describes the gate.

## Uninstall

`helm uninstall` removes the controllers and leaves the CRDs, every custom resource and every NATS cluster in place.
To remove those as well, do every step.
To remove the controllers alone, do the second step and stop.

1. Delete the custom resources that have a finalizer while their controller still runs.
   These are every `NatsCluster` with JetStream, `NatsAccount`, `NatsUser`, `NatsStream`, `NatsConsumer`, `NatsKeyValue`, `NatsObjectStore` and `NatsClusterEvacuation`.
   Only the controller removes the finalizer, so a delete that you start after the uninstall does not finish.
   [Finalizers]({{< relref "/docs/reference/chart#finalizers" >}}) lists the finalizer of each kind.

1. Uninstall the release:

   ```sh
   helm uninstall nats-operator --namespace nats-operator
   ```

   This removes the Deployments, ServiceAccounts and RBAC of the controllers.
   It leaves these objects:

   - The CRDs, every custom resource, and everything that the controllers created for the custom resources: StatefulSets, Services, ConfigMaps, Secrets, PodDisruptionBudgets, NetworkPolicies and cert-manager Certificates.
   - The Pods and Services `<release>-<controller>-test`, if you ran `helm test`.
   - The Leases `<release>-cluster.nats.mikluko.io`, `<release>-auth.nats.mikluko.io` and `<release>-jetstream.nats.mikluko.io` in the release namespace, if leader election is on.

1. Delete the CRDs, from the chart that you pulled as in [Upgrade](#upgrade):

   ```sh
   kubectl delete -f nats-operator/crds
   ```

   This deletes every custom resource of those kinds.
   If a custom resource still has a finalizer, the command does not finish.

1. Delete the release namespace, which removes the Pods and Services of `helm test` and the Leases:

   ```sh
   kubectl delete namespace nats-operator
   ```

The seed Secrets that the auth controller generates are named `<name>-<operator|systemaccount|account>-<identity|signing-1>`.
They remain after their objects are deleted, and after the CRDs are deleted.
If you apply the objects again, the auth controller uses the same keys.
If you delete those Secrets, the identities of the NATS operator and its accounts are lost for good.
