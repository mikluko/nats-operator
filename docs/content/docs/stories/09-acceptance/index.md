---
title: Deploy a production supercluster of three NATS clusters
weight: 9
params:
  category: Several NATS clusters
  tags: [three NATS clusters]
  e2e:
    waits:
      - {step: 1, wait: 6m, reason: "eleven servers in three Kubernetes clusters start and join by gateways before the accounts are distributed to all of them"}
      - {step: 2, wait: 3m, reason: "the balancers wait for their NATS cluster to settle before their first pass"}
    clusters:
      - name: prod-east
        files: [e2e/00-home.yaml, 01-natsoperatortrust.yaml, 01-prod-east.yaml, 01-auth.yaml, 01-users.yaml, e2e/01-runtime-streams.yaml,
          e2e/01-status-job.yaml, 01-status-natscluster-prod-east.yaml, 01-status-natsaccount-monitoring-prod.yaml, 02-jetstream.yaml,
          02-status-natsstream-requests.yaml, 02-status-natsbalancer.yaml, 02-status-natssystembalancer.yaml]
      - name: dev-east
        files: [e2e/00-dev-east.yaml, 01-natsoperatortrust.yaml, 01-dev-east.yaml, 01-status-natscluster-dev-east.yaml]
      - name: prod-west
        files: [e2e/00-prod-west.yaml, 01-natsoperatortrust.yaml, 01-prod-west.yaml, 01-status-natscluster-prod-west.yaml]
    substitutions:
      - files: [01-natsoperatortrust.yaml]
        reason: the JWTs of the NATS operator and system account whose keys e2e/00-home.yaml holds, from internal/e2e/fixtures
        patchFile: e2e/natsoperatortrust.json
      - files: [01-auth.yaml]
        kind: NatsOperator
        reason: the keys e2e/00-home.yaml holds, which sign the trust JWTs above
        patch: {spec: {keys: {identity: {secretKeyRef: {name: acme-keys, key: identity}}, signing: [{name: signing-1, secretKeyRef: {name: acme-keys, key: signing-1}}]}}}
      - files: [01-auth.yaml]
        kind: NatsSystemAccount
        reason: the keys e2e/00-home.yaml holds, which sign the trust JWTs above and the other Kubernetes clusters' controller creds
        patch: {spec: {keys: {identity: {secretKeyRef: {name: sys-keys, key: identity}}, signing: [{name: signing-1, secretKeyRef: {name: sys-keys, key: signing-1}}]}}}
      - files: [01-auth.yaml]
        kind: NatsAccount
        name: monitoring-prod
        reason: the keys e2e/00-home.yaml holds, which sign the creds of the application creating its streams at runtime
        patch: {spec: {keys: {identity: {secretKeyRef: {name: monitoring-prod-keys, key: identity}}, signing: [{name: signing-1, secretKeyRef: {name: monitoring-prod-keys, key: signing-1}}]}}}
      - files: [01-prod-east.yaml, 01-dev-east.yaml, 01-prod-west.yaml]
        reason: >-
          eleven servers share three kind nodes on one host of 6 CPUs and 12G, with one storage class; no cert-manager, so the
          gateways run without TLS
        patch:
          spec:
            resources: {requests: {cpu: 50m, memory: 192Mi}, limits: {memory: 192Mi}}
            jetstream: {volumeClaimTemplate: {spec: {storageClassName: standard, resources: {requests: {storage: 2Gi}}}}}
            gateway:
              remotes:
                - {name: prod-east, url: "nats://nats.prod-east.acme.example:7222"}
                - {name: dev-east, url: "nats://nats.dev-east.acme.example:7222"}
                - {name: prod-west, url: "nats://nats.prod-west.acme.example:7222"}
              tls: null
      - files: [01-prod-east.yaml]
        reason: a memory store of 10Gi does not fit a 192Mi server
        patch: {spec: {jetstream: {limits: {maxMemoryStore: 64Mi}}}}
      - files: [01-dev-east.yaml]
        reason: the kind node carries no zone label
        patch: {spec: {podTemplate: {spec: {nodeSelector: null}}}}
---

This guide shows you how to deploy a supercluster for production: three NATS clusters in three Kubernetes clusters, under one NATS operator, with JetStream balanced.
It combines the other guides, and each step links to the guide that has its options.

The manifests deploy these NATS clusters:

- `prod-east`, five servers, in the home cluster.
- `dev-east`, three servers, all in one zone.
- `prod-west`, three servers, in another region.

Four services use the supercluster, and each service has its own account.
The manifests have the accounts of the production environment.
The accounts of the development environment are the same under `-dev` names, and are left out.

## Before you begin

You need:

- Three Kubernetes clusters, with kubeconfig contexts named `prod-east`, `dev-east` and `prod-west`.
  Each manifest on this page is followed by the command that applies it with `--context`.
- In each, the cluster controller and the JetStream controller, cert-manager, the namespace `nats-system`, the StorageClass `gp3`, and LoadBalancer Services that the other Kubernetes clusters can reach.
  [Install]({{< relref "/docs/install" >}}) shows how to install the controllers.
- In each, the Issuer `nats-gateway-ca` in `nats-system`, from a private CA.
  See [Create the gateway CA]({{< relref "/docs/stories/06-supercluster#create-the-gateway-ca" >}}).
- In `prod-east`, the auth controller and the namespace `monitoring`.
- The DNS names `nats.prod-east.acme.example`, `nats.dev-east.acme.example` and `nats.prod-west.acme.example` pointing at the gateway Services, for example through external-dns.

## Declare the NATS operator and the accounts

Apply the NATS operator `acme`, its system account `sys` and the four accounts in the home cluster, `prod-east`:

{{< manifest "01-auth.yaml" >}}

The accounts form a chain.
`checks-prod` exports a service to `monitoring-prod`, and `monitoring-prod` exports streams and services to `core-prod` and `collector-prod`.
[Share a stream and a service between accounts]({{< relref "/docs/stories/05-account-wiring" >}}) shows how to declare exports and imports.

Declare the users of the home cluster's own controllers and the auth controller's connection as well.
[Put a NATS cluster under a NATS operator]({{< relref "/docs/stories/02-auth-plane" >}}) shows them, with the Secrets `cluster-controller-creds` and `jetstream-controller-creds` that the manifests on this page refer to.

## Declare the users

Apply the users in `prod-east`:

{{< manifest "01-users.yaml" >}}

- Every account has a `service` user and a `readonly` user.
  A `readonly` user sets the preset `readonly`, and a user with a preset cannot set `permissions`.
  [readonly]({{< relref "/docs/reference/nats-permissions#readonly" >}}) in the NATS permissions reference lists the subjects of the preset.
- The users of the monitoring team are in the namespace `monitoring`, and the `NatsReferenceGrant` lets them refer to the team's accounts in `nats-system`.
  See [Let a team declare its own users and streams]({{< relref "/docs/stories/04-team-self-service" >}}).
- Each of the other two Kubernetes clusters has two users, one for its cluster controller and one for its JetStream controller.

The auth controller writes the creds Secrets in `prod-east`.
Deliver the Secrets of the controllers' users to `nats-system` in `dev-east` and `prod-west`, as [Build a supercluster across Kubernetes clusters]({{< relref "/docs/stories/06-supercluster" >}}) shows.

## Copy the trust roots to every Kubernetes cluster

Copy the NATS operator JWT and the system account JWT from the status of the `NatsOperator` into a `NatsOperatorTrust`, and apply the same one in all three Kubernetes clusters.
Keep the copies alike with GitOps.

{{< manifest "01-natsoperatortrust.yaml" >}}

## Deploy the NATS clusters

Apply one `NatsCluster` in each Kubernetes cluster.
All three have the same `gateway.remotes`.

{{< manifest "01-prod-east.yaml" >}}

`jetstream.limits.maxMemoryStore` sets the size of the memory store.
Without it, the cluster controller derives the size from `resources.limits.memory`.

{{< manifest "01-dev-east.yaml" >}}

`podTemplate` passes scheduling fields to the pods.
Here, a `nodeSelector` puts every server of `dev-east` in one zone.

{{< manifest "01-prod-west.yaml" >}}

`serverTags` are yours to choose.
The placement tag of `prod-west` is `cluster:west`, which is not its name.

[NatsClusterSpec]({{< relref "/docs/reference/api#NatsClusterSpec" >}}) in the API reference lists every field.

## Check the gateways

Read the status of `prod-west`:

```sh
kubectl --context prod-west -n nats-system get natscluster prod-west -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natscluster-prod-west.yaml" >}}

`GatewaysConnected` is True once both of the other members are connected.
The status of every `NatsCluster` has an entry under `gateways` for each of the other members.

## Check that the accounts reached every server

Every account reaches every server of the supercluster through the resolver.
Read the status of an account:

```sh
kubectl --context prod-east -n nats-system get natsaccount monitoring-prod -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natsaccount-monitoring-prod.yaml" >}}

`distribution` counts the servers of all three NATS clusters.

## Adopt the streams and balance them

The application of the monitoring team created its streams `REQUESTS` and `RESPONSES` itself, before any `NatsStream` existed.
Apply the manifest in `prod-east`:

{{< manifest "02-jetstream.yaml" >}}

- The two `NatsStream`s set `adoptionPolicy: Adopt`, so they adopt the streams and keep their config.
  See [Adopting streams]({{< relref "/docs/stories/03-unmanaged#adopting-streams" >}}).
- The `NatsBalancer` balances the account of the monitoring team in two pools, which select the streams by label.
- The `NatsSystemBalancer` balances every account of `prod-east`.
  The platform team needs one for each NATS cluster, and the manifest has only the one for `prod-east`.

[Balance JetStream leaders and copies]({{< relref "/docs/stories/07-balancing" >}}) describes both balancers.

Read the status of an adopted stream:

```sh
kubectl --context prod-east -n monitoring get natsstream requests -o yaml
```

The `status` in the output is similar to this:

{{< manifest "02-status-natsstream-requests.yaml" >}}

Read the status of the account balancer:

```sh
kubectl --context prod-east -n monitoring get natsbalancer prod -o yaml
```

The `status` in the output is similar to this:

{{< manifest "02-status-natsbalancer.yaml" >}}

Each pool has one stream, so its `leaderSkew` is 1.
Every stream of the account has a resource in a pool, so the status has no default pool.

Read the status of the system balancer:

```sh
kubectl --context prod-east -n nats-system get natssystembalancer prod-east -o yaml
```

The `status` in the output is similar to this:

{{< manifest "02-status-natssystembalancer.yaml" >}}

`capabilities.leader` is None.
`monitoring-prod` is the one account with streams in `prod-east`, and it has no `jetstream-stepdown` export, so the system balancer moves no leader.
To let it, add the export as in [Let the system balancer move the leaders of an account]({{< relref "/docs/stories/07-balancing#let-the-system-balancer-move-the-leaders-of-an-account" >}}).
