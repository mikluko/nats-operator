---
title: Moving a stream to another NATS cluster
weight: 8
---

The orders team's stream lives in `east`, and its producers and consumers are moving to `west`. In the supercluster from the previous stories, moving the stream is one field.

## Before and after

{{< manifest "natsstream-before.yaml" >}}

{{< manifest "natsstream-after.yaml" >}}

## During the move

The resource reports progress per replica and per consumer, and `Synced` turns true once the east copies are gone. What clients see during the move is not yet verified.

{{< manifest "status-natsstream-transferring.yaml" >}}
