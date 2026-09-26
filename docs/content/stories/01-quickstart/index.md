---
title: A NATS cluster with JetStream
weight: 1
---

A platform engineer wants a three-server NATS cluster with JetStream in one Kubernetes cluster, and one stream on it. No auth plane: every client lands in the global account.

## The cluster

The cluster controller renders the server config, owns the StatefulSet, and self-signs route certificates because none is named.

{{< manifest "natscluster.yaml" >}}

At rest it reports every server on the same config revision, and `Settled` once every Raft group has a leader and every member is current. `endpoints` is where a connection's address comes from.

{{< manifest "status-natscluster-at-rest.yaml" >}}

Changing `spec.version` is restart-only, so the rollout restarts one server at a time and waits for `Settled` before the next.

{{< manifest "status-natscluster-mid-rollout.yaml" >}}

## The stream

The JetStream controller reaches the cluster only through a connection, the same way it reaches a NATS cluster nobody here deployed.

{{< manifest "natsconnection.yaml" >}}

{{< manifest "natsstream.yaml" >}}

The stream's status is re-read on a resync period, so drift made outside Kubernetes shows up as `Synced=False`.

{{< manifest "status-natsstream.yaml" >}}
