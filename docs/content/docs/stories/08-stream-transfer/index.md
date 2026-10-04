---
title: Move a stream to another NATS cluster
weight: 8
params:
  e2e:
    after: 6
    waits:
      - {step: 1, wait: 3m, reason: the fixture Job loads 300MB into ORDERS before the move}
      - {step: 3, wait: 5m, reason: west copies 300MB three times over the gateways before the east copies go}
---

This guide shows you how to move a stream from one NATS cluster of a supercluster to another, together with its consumers.
The manifests move the stream `ORDERS` from the NATS cluster `east` to the NATS cluster `west`.

To move every stream off a NATS cluster that you are retiring, see [Retire a NATS cluster]({{< relref "/docs/stories/11-evacuation" >}}).
An evacuation leaves a stream whose `NatsStream` sets `placement.cluster` where it is, so move such a stream with this guide.

## Before you begin

You need:

- Both NATS clusters in one supercluster. [Build a supercluster across Kubernetes clusters]({{< relref "/docs/stories/06-supercluster" >}}) joins `east` and `west`.
- A `NatsStream` for the stream. If the stream has none, adopt it first: see [Adopting streams]({{< relref "/docs/stories/03-unmanaged#adopting-streams" >}}).

## Check where the stream is

This guide starts from a `NatsStream` whose `spec.placement.cluster` is `east`.

{{< manifest "01-natsstream-before.yaml" >}}

Read its status:

```sh
kubectl -n orders get natsstream orders -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natsstream-in-east.yaml" >}}

`server.leader` is a server of `east`.

## Change the placement

Set `spec.placement.cluster` to the name of the target NATS cluster, change nothing else, and apply the manifest.

{{< manifest "02-natsstream-after.yaml" >}}

nats-server builds new replicas in `west`, copies the stream and its consumers to them, and then removes the replicas in `east`.

During the move, clients get what nats-server provides for a placement move.
See [Guarantees]({{< relref "/docs/design/v1#8-guarantees" >}}) in the design.

## Follow the move

Read the status again:

```sh
kubectl -n orders get natsstream orders -o yaml
```

While the move runs, the `status` in the output is similar to this:

{{< manifest "02-status-natsstream-transferring.yaml" >}}

`Synced` is False with the reason `Moving`.
`transfer.replicas` has one entry for each new replica, and `transfer.consumers` counts the consumers that have moved.
[StreamTransfer]({{< relref "/docs/reference/api#StreamTransfer" >}}) in the API reference describes every field.

## Confirm that the move is done

The move is done when the replicas in `east` are gone.
The `status` is then similar to this:

{{< manifest "03-status-natsstream-in-west.yaml" >}}

`Synced` is True, `transfer` is gone, and `server.leader` is a server of `west`.
