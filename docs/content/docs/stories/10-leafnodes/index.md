---
title: Join a NATS cluster to a hub as a leaf
weight: 10
params:
  e2e:
    waits:
      - {step: 1, wait: 4m, reason: "the hub and both leaves start before the leaf remotes connect"}
    clusters:
      - name: hub
        files: [e2e/00-hub.yaml, 01-hub.yaml]
      - name: edge
        files: [e2e/00-edge.yaml, 01-edge.yaml, 01-edge-operator.yaml, 01-status-natscluster-edge-site-1.yaml]
    substitutions:
      - files: [01-edge-operator.yaml]
        kind: NatsOperatorTrust
        reason: the JWTs of the NATS operator and system account e2e/00-hub.yaml adopts, from internal/e2e/fixtures
        patchFile: e2e/natsoperatortrust.json
      - files: [01-edge-operator.yaml]
        kind: NatsAccountTrust
        reason: the key and JWT of the telemetry account e2e/00-hub.yaml adopts, from internal/e2e/fixtures
        patchFile: e2e/natsaccounttrust.json
      - files: [01-hub.yaml, 01-edge.yaml, 01-edge-operator.yaml]
        kind: NatsCluster
        reason: a Kubernetes cluster is one kind node, on a host every kind cluster of the run shares
        patch: {spec: {resources: {requests: {cpu: 100m, memory: 192Mi}, limits: {memory: 192Mi}}}}
      - files: [01-hub.yaml]
        kind: NatsCluster
        reason: >-
          one kind node, on a host every kind cluster of the run shares, holds three servers, without JetStream and without
          the rest of story 9's supercluster; no cert-manager, so the leafnode listener runs without TLS
        patch: {spec: {replicas: 3, jetstream: null, gateway: null, leafnodes: {tls: null}}}
      - files: [01-edge.yaml]
        kind: NatsConnection
        name: hub
        reason: the hub's leafnode listener runs without TLS
        patch: {spec: {servers: ["nats://leaf.prod-east.acme.example:7422"]}}
      - files: [01-edge-operator.yaml]
        kind: NatsConnection
        name: hub-system
        reason: the hub's leafnode listener runs without TLS
        patch: {spec: {servers: ["nats://leaf.prod-east.acme.example:7422"]}}
      - files: [01-edge-operator.yaml]
        kind: NatsConnection
        name: hub-telemetry
        reason: the hub's leafnode listener runs without TLS
        patch: {spec: {servers: ["nats://leaf.prod-east.acme.example:7422"]}}
---

This guide shows you how to join a NATS cluster at an edge site to a hub as a leaf.
The manifests join the NATS cluster `edge-site-1`, in a Kubernetes cluster of its own, to the hub `prod-east`.
Clients at the edge site publish telemetry that reaches the account `telemetry` of the hub, and a stream on the leaf accepts messages while the link to the hub is down.

A leaf is not a member of the supercluster of its hub.
To add a member, see [Add a NATS cluster to a supercluster that you did not deploy]({{< relref "/docs/stories/13-join-supercluster" >}}).

## Before you begin

You need:

- Two Kubernetes clusters, with kubeconfig contexts named `hub` and `edge`.
  Each manifest on this page is followed by the command that applies it with `--context`.
- In `hub`, the NATS cluster `prod-east` with its NATS operator `acme` and system account `sys`, from [Deploy a production supercluster of three NATS clusters]({{< relref "/docs/stories/09-acceptance" >}}), and a `NatsAccount` named `telemetry`.
- In `hub`, the ClusterIssuer `letsencrypt`, and the DNS name `leaf.prod-east.acme.example` pointing at the leafnode Service, for example through external-dns.
- In `edge`, the cluster controller and the JetStream controller, and the namespace `nats-system`.
  [Install]({{< relref "/docs/install" >}}) shows how to install the controllers.

## Open a leafnode listener on the hub

Apply `prod-east` with `leafnodes` added, and the users that the leaves connect as.

{{< manifest "01-hub.yaml" >}}

- `leafnodes.tls` is optional.
- `edge-site-1` is a user of the account `telemetry`.
  It sets `connectionTypes` to LEAFNODE beside its permissions, so it can open only a leafnode connection.
- `edge-site-2-system` is a user of the system account with the preset `leafnode`, which allows a leafnode connection on every subject of the account.
  Only a leaf that enforces the accounts of the hub needs it. See [Enforce the accounts of the hub on a leaf](#enforce-the-accounts-of-the-hub-on-a-leaf).

[Leafnodes]({{< relref "/docs/reference/api#Leafnodes" >}}) in the API reference lists every field of the listener.

The auth controller writes the creds Secrets in `hub`.
Deliver the Secret `edge-site-1-leaf-creds` to `nats-system` in `edge`, for example with External Secrets.

## Deploy the leaf

A leaf is a `NatsCluster` with `leafRemotes`.
Each remote refers to a `NatsConnection`, which has the address of the hub and the creds to connect with.
Apply the connection, the `NatsCluster` `edge-site-1`, and a stream on it:

{{< manifest "01-edge.yaml" >}}

- A `NatsCluster` that has `leafRemotes` and JetStream must set `jetstream.domain`.
- `edge-site-1` has no auth plane, so it has one local account, the global account, and the remote binds that account.
  Every message that a local client publishes reaches the hub in the account of the remote's creds.
- `edge-site-1` sets no `auth.systemCredentials`, so a change to its remotes restarts its servers.
- The stream `TELEMETRY_BUFFER` is on the leaf, through the connection `local`, and accepts messages while the link is down.
  To drain it, give a stream in the account `telemetry` of the hub a source. This guide does not show that stream.

If the `NatsConnection` of a remote is in another namespace, that namespace needs a `NatsReferenceGrant` that admits the `NatsCluster`.
The grant gives the creds of the connection to the namespace of the leaf, where the cluster controller copies them into the Secret `<name>-leaf-remotes`.

[LeafRemote]({{< relref "/docs/reference/api#LeafRemote" >}}) in the API reference lists every field of a remote.

## Check the leaf

Read the status of `edge-site-1`:

```sh
kubectl --context edge -n nats-system get natscluster edge-site-1 -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natscluster-edge-site-1.yaml" >}}

`LeafnodesConnected` is True once every server has a connection to every remote.
In `leafRemotes`, `connected` counts the servers that are connected, and `account` is the public key of the hub account that the creds belong to.

## Enforce the accounts of the hub on a leaf

To have the clients of a leaf authenticate against the accounts of the hub, give the leaf the trust roots of the hub.
The manifests deploy a second leaf, `edge-site-2`, in that form.

1. Deliver the Secrets `edge-site-2-system-leaf-creds` and `edge-site-2-leaf-creds` to `nats-system` in `edge`.
   The first has the creds of the user `edge-site-2-system`, and the second the creds of a user of the account `telemetry`.
1. Copy the NATS operator JWT and the system account JWT into the `NatsOperatorTrust`, as for a member of the supercluster.
1. Copy the public key and the JWT of the account `telemetry` into the `NatsAccountTrust`.

Then apply the manifest:

{{< manifest "01-edge-operator.yaml" >}}

- The remote with `localSystemAccount` connects the system account of the leaf to the hub, as a user of the system account of the hub.
  Without that remote, the leaf cannot fetch an account that it has not cached.
- The remote with `localAccountTrustRef` binds the account `telemetry`.
  The leaf preloads the JWT in the `NatsAccountTrust`, so the clients of the account can authenticate while the link is down.
- The preloaded JWT is a copy.
  It expires after the `jwtTTL` of the account, 48h by default, unless the `NatsAccount` sets `jwtTTL` to 0.
- The manifest sets no `auth.resolver`.
  A leaf that preloads an account runs the resolver `Full` on its JetStream volume, and a leaf that preloads none runs `Cache`.

In the home cluster, a `NatsAccountTrust` can set `accountRef` in place of `publicKey` and `jwt`, and the auth controller writes the JWT.
You can set only one of `accountRef` and `publicKey`.
[Trust, resolver, gateways, leaf nodes]({{< relref "/docs/design/v1#43-trust-resolver-gateways-leaf-nodes" >}}) in the design describes the resolver of a leaf.
