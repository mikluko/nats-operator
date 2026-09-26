---
title: Retiring a NATS cluster
weight: 11
---

The infra team replaces `prod-east`: it rolls out `prod-east-2` beside it in the same supercluster, moves every JetStream asset across, and only then deletes the old cluster.

## The evacuation

A new cluster with a tag of its own, and one system-level evacuation of the whole old cluster.

{{< manifest "evacuation.yaml" >}}

Resources whose own spec pins the old cluster are left alone and listed; the evacuation is not Ready until their owners move them.

{{< manifest "status-natsclusterevacuation.yaml" >}}

## Deleting the old cluster

Deletion waits while the cluster still holds JetStream data.

{{< manifest "status-natscluster-old.yaml" >}}
