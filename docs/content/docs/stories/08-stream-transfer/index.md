---
title: Moving a stream to another NATS cluster
weight: 8
params:
  e2e:
    after: 6
    waits:
      - {step: 1, wait: 3m, reason: the fixture Job loads 300MB into ORDERS before the move}
      - {step: 3, wait: 5m, reason: west copies 300MB three times over the gateways before the east copies go}
---

The orders team's stream lives in `east`, and its producers and consumers are moving to `west`. In the supercluster from the previous stories, moving the stream is one field.

## Before and after

{{< manifest "01-natsstream-before.yaml" >}}

{{< manifest "01-status-natsstream-in-east.yaml" >}}

{{< manifest "02-natsstream-after.yaml" >}}

## During the move

The resource reports progress per new replica and a count of consumers moved, with `Synced` false until the move ends. What clients see meanwhile is what nats-server provides for a placement move; the controller adds no guarantee of its own.

{{< manifest "02-status-natsstream-transferring.yaml" >}}

`Synced` turns true once the east copies are gone, and the transfer block with them.

{{< manifest "03-status-natsstream-in-west.yaml" >}}
