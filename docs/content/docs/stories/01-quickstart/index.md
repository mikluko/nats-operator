---
title: A NATS cluster with JetStream
weight: 1
params:
  e2e:
    substitutions:
      - files: [01-natscluster.yaml]
        reason: three servers share one kind node, on a host every cluster of the run shares
        patch: {spec: {resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 256Mi}}}}
      - files: [02-natscluster-8gi.yaml]
        reason: three servers share one kind node, on a host every cluster of the run shares
        patch: {spec: {resources: {requests: {cpu: 100m, memory: 512Mi}, limits: {memory: 512Mi}}}}
      - files: [01-status-natscluster-at-rest.yaml]
        reason: max_memory_store derives from the substituted 256Mi limit
        patch: {status: {jetstream: {limits: {maxMemoryStore: 192Mi}}}}
---

A platform engineer wants a three-server NATS cluster with JetStream in one Kubernetes cluster, and one stream on it. No auth plane: every client lands in the global account.

## The cluster

The cluster controller renders the server config, owns the StatefulSet, and self-signs route certificates because none is named.

{{< manifest "01-natscluster.yaml" >}}

At rest it reports every server on the same config revision, and `Settled` once every Raft group has a leader and every member is current. `endpoints` is where a connection's address comes from.

{{< manifest "01-status-natscluster-at-rest.yaml" >}}

Raising the memory changes each server's pod template, which is restart-only, so the rollout restarts one server at a time and waits for `Settled` before the next. A `spec.version` change rolls the same way.

{{< manifest "02-natscluster-8gi.yaml" >}}

{{< manifest "02-status-natscluster-mid-rollout.yaml" >}}

## The stream

The JetStream controller reaches the cluster only through a connection, the same way it reaches a NATS cluster nobody here deployed.

{{< manifest "03-natsconnection.yaml" >}}

{{< manifest "03-natsstream.yaml" >}}

The stream's status is re-read on a resync period, so drift made outside Kubernetes is reapplied from spec and reported, for one resync, as `Synced=False`, reason `DriftCorrected`.

{{< manifest "03-status-natsstream.yaml" >}}

## Connecting a client

With the [NATS CLI](https://github.com/nats-io/natscli), through a port-forward to the cluster's client Service:

```sh
kubectl -n nats-system port-forward svc/demo 4222:4222 &
nats -s nats://localhost:4222 pub orders.created '{"id": 1}'
nats -s nats://localhost:4222 stream info ORDERS
```
