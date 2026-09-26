---
title: Moving a stream to another NATS cluster
weight: 8
params:
  e2e:
    skip: runs in the story 6 supercluster, which spans Kubernetes clusters
---

The orders team's stream lives in `east`, and its producers and consumers are moving to `west`. In the supercluster from the previous stories, moving the stream is one field.

## Before and after

{{< manifest "01-natsstream-before.yaml" >}}

{{< manifest "01-status-natsstream-in-east.yaml" >}}

{{< manifest "02-natsstream-after.yaml" >}}

## During the move

The resource reports progress per replica and per consumer, and `Synced` turns true once the east copies are gone. What clients see during the move is not yet verified.

{{< manifest "02-status-natsstream-transferring.yaml" >}}
