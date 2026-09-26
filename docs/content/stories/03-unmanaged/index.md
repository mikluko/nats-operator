---
title: JetStream on a NATS cluster you did not deploy
weight: 3
---

An application team runs on a NATS cluster someone else deployed, a Helm release in `messaging`, and wants the streams its services created at runtime declared as resources without recreating them.

## The connection

Same shape as a managed cluster's. The credentials come from whoever runs that NATS cluster's auth plane.

{{< manifest "natsconnection.yaml" >}}

## Adopting streams

`Adopt` takes over a stream that must already exist and writes the server's config into the spec. `AdoptOrCreate` takes it over or creates it, applying the spec as written. The default `Never` refuses to touch a stream the controller does not own.

{{< manifest "natsstreams.yaml" >}}

After adoption the `payments` resource carries the server's config:

{{< manifest "natsstream-payments-adopted.yaml" >}}

{{< manifest "status-natsstream-payments.yaml" >}}

A stream that exists and is not the controller's, with no adoption policy set, stops the resource and leaves the server untouched. `terminalPolicy: Retry` rechecks it every resync period; the default `Hold` waits for an edit.

{{< manifest "status-natsstream-ledger.yaml" >}}

## A key-value bucket and an object store

Declared the same way as streams, with the same connection and lifecycle policies.

{{< manifest "natskeyvalue-objectstore.yaml" >}}

## Consumers

A consumer names its stream by server-side name, for a stream with no resource, or by reference to a `NatsStream`, which it then waits for.

{{< manifest "natsconsumer.yaml" >}}
