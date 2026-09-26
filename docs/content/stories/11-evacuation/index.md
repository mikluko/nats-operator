---
title: Retiring a NATS cluster
weight: 11
params:
  e2e:
    skip: runs in the story 9 supercluster, which spans Kubernetes clusters
---

The infra team replaces `prod-east`: it rolls out `prod-east-2` beside it in the same supercluster, moves every JetStream asset across, and only then deletes the old cluster.

## The evacuation

The old cluster:

{{< manifest "01-natscluster-prod-east.yaml" >}}

A new cluster with a tag of its own, and one system-level evacuation of the whole old cluster.

{{< manifest "01-evacuation.yaml" >}}

Resources whose own spec pins the old cluster are left alone and listed; the evacuation is not Ready until their owners move them.

{{< manifest "01-status-natsclusterevacuation.yaml" >}}

## Deleting the old cluster

Deletion waits while the cluster still holds JetStream data.

{{< manifest "02-delete-natscluster-prod-east.yaml" >}}

{{< manifest "02-status-natscluster-prod-east.yaml" >}}
