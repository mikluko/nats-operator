---
title: A NATS cluster with JetStream
weight: 1
params:
  e2e:
    substitutions:
      - files: [01-natscluster.yaml]
        reason: three servers share one kind node, on a host every kind cluster of the run shares
        patch: {spec: {resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 256Mi}}}}
      - files: [02-natscluster-8gi.yaml]
        reason: three servers share one kind node, on a host every kind cluster of the run shares
        patch: {spec: {resources: {requests: {cpu: 100m, memory: 512Mi}, limits: {memory: 512Mi}}}}
      - files: [01-status-natscluster-at-rest.yaml]
        reason: max_memory_store derives from the substituted 256Mi limit
        patch: {status: {jetstream: {limits: {maxMemoryStore: 192Mi}}}}
---

In this story you deploy a three-server NATS cluster with JetStream in one Kubernetes cluster. You then restart it with more memory, create a stream on it and publish a message.

The NATS cluster has no auth plane, so every client lands in the global account.

## Before you begin

You need:

- A Kubernetes cluster with the cluster controller and the JetStream controller installed. See [Install]({{< relref "/docs/install" >}}).
- The namespace `nats-system`. Create it with `kubectl create namespace nats-system`.
- A StorageClass named `standard`, which the manifests ask for.
- The [NATS CLI](https://github.com/nats-io/natscli), for the last step.

[Following a story]({{< relref "/docs/stories#following-a-story" >}}) explains how to read the manifests and the status files.

## 1. Deploy the NATS cluster

Apply the `NatsCluster`.

{{< manifest "01-natscluster.yaml" >}}

The cluster controller renders the server config and creates one StatefulSet per server. The manifest sets no route certificate, so the controller self-signs one.

Check the status:

```sh
kubectl -n nats-system get natscluster demo -o yaml
```

Wait until it matches the status below. Every server reports the same `configRevision`. `Settled` is True once every Raft group has a leader and every member is current.

{{< manifest "01-status-natscluster-at-rest.yaml" >}}

You use `endpoints.client` as the server address in step 3.

## 2. Raise the memory limit

Apply the same `NatsCluster` with its memory raised from 4Gi to 8Gi.

{{< manifest "02-natscluster-8gi.yaml" >}}

The change alters each server's pod template, and only a restart applies a pod template. The cluster controller restarts one server at a time and waits for `Settled` before it restarts the next. A change to `spec.version` rolls out the same way.

While the rollout runs, the status looks like this:

{{< manifest "02-status-natscluster-mid-rollout.yaml" >}}

## 3. Create a stream

The JetStream controller reaches a NATS cluster only through a `NatsConnection`. Apply one that points at the client endpoint from step 1.

{{< manifest "03-natsconnection.yaml" >}}

Apply the `NatsStream`.

{{< manifest "03-natsstream.yaml" >}}

Check the status:

```sh
kubectl -n nats-system get natsstream orders -o yaml
```

`Ready` and `Synced` are True.

{{< manifest "03-status-natsstream.yaml" >}}

The JetStream controller reads the stream from the server again on every resync period. If the stream was changed outside Kubernetes, the controller reapplies the spec. For one resync it reports `Synced=False` with the reason `DriftCorrected`.

## 4. Publish a message

Forward the client port of the NATS cluster's client Service, then publish a message and read the stream:

```sh
kubectl -n nats-system port-forward svc/demo 4222:4222 &
nats -s nats://localhost:4222 pub orders.created '{"id": 1}'
nats -s nats://localhost:4222 stream info ORDERS
```

The last command prints the state of the stream `ORDERS`, which now has one message.

## Next

[Owning the auth plane]({{< relref "/docs/stories/02-auth-plane" >}}) puts this NATS cluster under a NATS operator, with accounts and users declared as resources.
