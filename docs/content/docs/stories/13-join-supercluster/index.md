---
title: Add a NATS cluster to a supercluster that you did not deploy
weight: 13
params:
  category: Several NATS clusters
  tags: [auth.accountTrustRefs]
  e2e:
    waits:
      - {step: 0, wait: 2m, reason: "central's servers start"}
      - {step: 1, wait: 4m, reason: "west's servers start and connect central's gateway, and the meta group spanning both elects a leader"}
    substitutions:
      - files: [01-natsoperatortrust.yaml]
        reason: the JWTs of the NATS operator and system account the existing NATS cluster in e2e/00-central.yaml trusts, from internal/e2e/fixtures
        patchFile: e2e/natsoperatortrust.json
      - files: [01-natsaccounttrusts.yaml]
        kind: NatsAccountTrust
        name: orders
        reason: the key and JWT of the orders account e2e/00-central.yaml preloads, from internal/e2e/fixtures
        patchFile: e2e/natsaccounttrust-orders.json
      - files: [01-natsaccounttrusts.yaml]
        kind: NatsAccountTrust
        name: payments
        reason: the key and JWT of the payments account e2e/00-central.yaml preloads, from internal/e2e/fixtures
        patchFile: e2e/natsaccounttrust-payments.json
      - files: [01-west.yaml]
        reason: three servers share one kind node with central's, with one storage class
        patch:
          spec:
            resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 256Mi}}
            jetstream: {volumeClaimTemplate: {spec: {storageClassName: standard, resources: {requests: {storage: 5Gi}}}}}
---

This guide shows you how to add a NATS cluster to a supercluster that these controllers do not run.
The servers of that supercluster were deployed by other means, someone else has the keys of its NATS operator, and nothing pushes account JWTs to its servers.
The manifests add the NATS cluster `west` to a supercluster whose one member is `central`, and create a stream on `west` in the account `orders` of the supercluster.

To build a supercluster of NATS clusters that you deploy yourself, see [Build a supercluster across Kubernetes clusters]({{< relref "/docs/stories/06-supercluster" >}}).

## Before you begin

You need:

- A Kubernetes cluster with the cluster controller and the JetStream controller installed, and the namespaces `nats-system` and `orders`.
  [Install]({{< relref "/docs/install" >}}) shows how to install the controllers.
- From whoever has the keys of the NATS operator: the NATS operator JWT, the system account JWT, and the public key and the JWT of each account that you use on `west`.
- The creds of a user of the system account, for the cluster controller, in the Secret `west-cluster-controller-creds` in `nats-system`.
- The creds of a user of the account `orders` in the Secret `orders-creds` in `orders`.
- The DNS name `nats-west.example.net` pointing at the gateway Service of `west`, for example through external-dns.
- The StorageClass `gp3`, or your own in its place in the manifest.

## Add the new member to the existing members

A `NatsCluster` does not change a NATS cluster that it did not deploy.
Ask whoever runs `central` to add `west` to its gateways before you deploy `west`.
In this guide, `central` sets `reject_unknown`, so it refuses a gateway that its own list does not have.

This is the part of the config of `central` that matters to `west`:

```text
gateway {
  name: central
  port: 7222
  reject_unknown: true
  gateways: [
    {name: west, urls: ["nats://nats-west.example.net:7222"]}
  ]
}
operator: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...
system_account: ASYS...
resolver: MEMORY
resolver_preload: {
  ASYS...: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...
  AORDERS...: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...
  APAYMENTS...: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...
}
```

`central` has its accounts as preloads under a `MEMORY` resolver.
Its gateway has no TLS and no `authorization`.

The servers of `west` reach the gateways of `central` through the Service behind `nats-central.example.net`.
If a server of `central` is Ready only once its JetStream reaches a meta leader, that Service must also publish servers that are not Ready, with `publishNotReadyAddresses: true`.
The meta group spans both NATS clusters, so `central` alone may not hold a majority of it, and a server of `central` that `west` cannot reach may never become Ready.

## Copy the trust roots

Copy the NATS operator JWT and the system account JWT into a `NatsOperatorTrust`, and apply it:

{{< manifest "01-natsoperatortrust.yaml" >}}

## Copy the accounts

Nothing pushes account JWTs to this supercluster, so `west` must preload them, as `central` does.
For each account, copy its public key and the JWT that `central` preloads into a `NatsAccountTrust`, and apply them:

{{< manifest "01-natsaccounttrusts.yaml" >}}

A copy is good until its JWT expires.
A JWT that the auth controller signs expires after the `jwtTTL` of its `NatsAccount`, which is 48h by default, or never if `jwtTTL` is 0.
A JWT signed elsewhere expires when its signer decided.

## Allow a gateway without TLS

Do this step only if the existing members run their gateways without TLS, as `central` does.
The cluster controller refuses a `NatsCluster` whose gateway has no `tls`, unless it runs with `--allow-gateway-without-tls`.
To pass the flag, upgrade the release with this chart value:

```yaml
cluster:
  allowGatewayWithoutTLS: true
```

## Deploy the new member

Apply the `NatsCluster` `west`:

{{< manifest "01-west.yaml" >}}

- The name of the `NatsCluster` is its gateway name. It must be the name that the existing members have in their lists.
- `auth.systemCredentials` is the Secret with the creds for the cluster controller.
- `auth.accountTrustRefs` lists the accounts that every server preloads beside the system account.
  A change to the list, or to the JWT in a `NatsAccountTrust` on it, restarts the servers.
- `gateway.remotes` lists every member of the existing supercluster, and `west` itself.
- `gateway` has no `tls`, because the gateways of the existing members have none.
- `gateway.service` renders the Service `west-gateway`, which publishes the servers of `west` before they are Ready.
  A server of `west` is Ready once its JetStream reaches the meta leader, and the gateways of `central` reach it through this Service.

The servers run a `Full` resolver on the JetStream volume.
If you remove an account from `auth.accountTrustRefs`, the account stays in the directory of the resolver, and the servers still serve it after the restart.
[Trust, resolver, gateways, leaf nodes]({{< relref "/docs/design/v1#43-trust-resolver-gateways-leaf-nodes" >}}) in the design describes what the resolver stores.

## Check the new member

Read the status of `west`:

```sh
kubectl -n nats-system get natscluster west -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natscluster.yaml" >}}

`GatewaysConnected` is True, and `gateways` has an entry for `central`.
A supercluster has one JetStream meta group, so `Settled` is True once the servers of `west` have joined that group beside the servers of `central`.

## Create a stream on the new member

Apply a `NatsConnection` with the creds of the user of `orders`, and a stream on it:

{{< manifest "02-natsconnection.yaml" >}}

{{< manifest "02-natsstream.yaml" >}}

`west` authenticates the user against the JWT of `orders` that it preloads.
`placement.cluster` puts the stream on the servers of `west` and not on those of `central`, which are in the same meta group.

Read the status of the stream:

```sh
kubectl -n orders get natsstream orders -o yaml
```

The `status` in the output is similar to this:

{{< manifest "02-status-natsstream.yaml" >}}

`server.leader` and `server.replicas` are servers of `west`.
