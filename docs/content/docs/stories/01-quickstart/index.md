---
title: A NATS cluster with JetStream
weight: 1
---

A platform engineer wants a three-server NATS cluster with JetStream in one Kubernetes cluster, and one stream on it. No auth plane: every client lands in the global account.

## The cluster

The cluster controller renders the server config, owns the StatefulSet, and self-signs route certificates because none is named.

{{< manifest "01-natscluster.yaml" >}}

At rest it reports every server on the same config revision, and `Settled` once every Raft group has a leader and every member is current. `endpoints` is where a connection's address comes from.

{{< manifest "01-status-natscluster-at-rest.yaml" >}}

Changing `spec.version` is restart-only, so the rollout restarts one server at a time and waits for `Settled` before the next.

{{< manifest "02-natscluster-2.15.1.yaml" >}}

{{< manifest "02-status-natscluster-mid-rollout.yaml" >}}

## The stream

The JetStream controller reaches the cluster only through a connection, the same way it reaches a NATS cluster nobody here deployed.

{{< manifest "03-natsconnection.yaml" >}}

{{< manifest "03-natsstream.yaml" >}}

The stream's status is re-read on a resync period, so drift made outside Kubernetes shows up as `Synced=False`.

{{< manifest "03-status-natsstream.yaml" >}}
