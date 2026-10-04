---
title: Retire a NATS cluster
weight: 11
params:
  category: JetStream
  tags: [NatsClusterEvacuation]
  e2e:
    waits:
      - {step: 1, wait: 4m, reason: "six servers start before streams move off prod-east"}
    substitutions:
      - files: [01-natscluster-prod-east.yaml, 01-evacuation.yaml]
        kind: NatsCluster
        reason: six servers share one kind node, on a host every Kubernetes cluster of the run shares
        patch:
          spec:
            replicas: 3
            resources: {requests: {cpu: 100m, memory: 192Mi}, limits: {memory: 192Mi}}
      - files: [01-natscluster-prod-east.yaml]
        kind: NatsCluster
        name: prod-east
        reason: >-
          two members of story 9's supercluster in one Kubernetes cluster, with one storage class; a memory store of
          10Gi does not fit a 192Mi server; no cert-manager, so the gateways run without TLS
        patch:
          spec:
            jetstream: {limits: null, volumeClaimTemplate: {spec: {storageClassName: standard, resources: {requests: {storage: 2Gi}}}}}
            gateway:
              remotes:
                - {name: prod-east, url: "nats://nats-prod-east.example.net:7222"}
                - {name: prod-east-2, url: "nats://nats-prod-east-2.example.net:7222"}
              tls: null
              service: {annotations: {external-dns.alpha.kubernetes.io/hostname: nats-prod-east.example.net}}
              advertise: nats-prod-east.example.net:7222
      - files: [01-evacuation.yaml]
        kind: NatsCluster
        name: prod-east-2
        reason: >-
          two members of story 9's supercluster in one Kubernetes cluster, with one storage class; a memory store of
          10Gi does not fit a 192Mi server; no cert-manager, so the gateways run without TLS
        patch:
          spec:
            jetstream: {limits: null, volumeClaimTemplate: {spec: {storageClassName: standard, resources: {requests: {storage: 2Gi}}}}}
            gateway:
              remotes:
                - {name: prod-east, url: "nats://nats-prod-east.example.net:7222"}
                - {name: prod-east-2, url: "nats://nats-prod-east-2.example.net:7222"}
              tls: null
              service: {annotations: {external-dns.alpha.kubernetes.io/hostname: nats-prod-east-2.example.net}}
              advertise: nats-prod-east-2.example.net:7222
---

This guide shows you how to retire a NATS cluster of a supercluster without losing its streams.
You deploy a replacement beside it in the same supercluster, move the streams to the replacement with a `NatsClusterEvacuation`, and then delete the old `NatsCluster`.
The manifests retire the NATS cluster `prod-east` and replace it with `prod-east-2`.

## Before you begin

You need:

- The NATS cluster `prod-east` and its supercluster from [Deploy a production supercluster of three NATS clusters]({{< relref "/docs/stories/09-acceptance" >}}), with the `NatsConnection` `prod-east-sys`, which has creds of the system account.
- The DNS name `nats.prod-east-2.acme.example` pointing at the gateway Service of the replacement, for example through external-dns.

## Add the replacement to the gateway list

Add `prod-east-2` to `gateway.remotes` of `prod-east`, and apply it.
Add the same entry to every other member of the supercluster.

{{< manifest "01-natscluster-prod-east.yaml" >}}

## Deploy the replacement and start the evacuation

Apply the `NatsCluster` `prod-east-2` and the `NatsClusterEvacuation`.
`prod-east-2` is the same as `prod-east`, except for its name, its gateway address and the server tag `cluster:prod-east-2`, which no other server has.

{{< manifest "01-evacuation.yaml" >}}

- `connectionRef` is a connection with creds of the system account.
- `from.cluster` is the NATS cluster to empty. An evacuation always empties a whole NATS cluster.
- `to.serverTags` must match only servers of the target NATS cluster.
  If a server of `from.cluster` has the same tags, the evacuation is refused before it moves anything.

The evacuation moves every stream, key-value bucket and object store of every account, with their consumers.
It does not move one whose resource sets `placement.cluster`.
Balancers skip a stream that the evacuation is moving.
[NatsClusterEvacuationSpec]({{< relref "/docs/reference/api#NatsClusterEvacuationSpec" >}}) in the API reference describes the fields.

## Check the evacuation

Read its status:

```sh
kubectl -n nats-system get natsclusterevacuation retire-prod-east -o yaml
```

When the evacuation has moved everything that it can move, the `status` in the output is similar to this:

{{< manifest "01-status-natsclusterevacuation.yaml" >}}

`moved` counts the streams that the evacuation moved, and `inFlight` the moves that are still running.

`pinned` lists the resources whose own spec sets `placement.cluster` to `prod-east`.
The evacuation is not Ready while any of them remains.
Ask the owner of each to move it, as [Move a stream to another NATS cluster]({{< relref "/docs/stories/08-stream-transfer" >}}) shows.

`stalePlacement` lists the moved streams that no resource owns and whose config still has `prod-east` as its placement.
While `prod-east` exists, an update that changes the placement of such a stream moves it back there.

[Evacuation]({{< relref "/docs/design/v1#66-evacuation" >}}) in the design describes how the streams are moved.

## Delete the retired NATS cluster

When `pinned` is empty, delete the `NatsCluster` `prod-east`:

{{< manifest "02-delete-natscluster-prod-east.yaml" >}}

The deletion waits while the NATS cluster still has JetStream data.
If the deletion does not finish, read the status of `prod-east`:

```sh
kubectl -n nats-system get natscluster prod-east -o yaml
```

The `status` in the output is similar to this:

{{< manifest "02-status-natscluster-prod-east.yaml" >}}

`Deleting` is True with the reason `JetStreamDataRemains`, and its message lists the stream groups that are still in `prod-east`.
Move them, and the deletion finishes.

To delete the `NatsCluster` together with the data that it still has, set the annotation `cluster.nats-operator.io/force-delete` on it.
[Deletion guard]({{< relref "/docs/design/v1#46-deletion-guard" >}}) in the design describes what the deletion waits for.
