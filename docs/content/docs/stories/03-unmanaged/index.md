---
title: Manage JetStream on a NATS cluster that you did not deploy
weight: 3
params:
  category: JetStream
  tags: [adoption]
---

This guide shows you how to declare streams, consumers, a key-value bucket and an object store as Kubernetes resources on a NATS cluster that the cluster controller did not deploy, and how to take over streams that already exist without recreating them.
The manifests reach a NATS cluster in the namespace `messaging` and declare everything in the namespace `payments`.

## Before you begin

You need:

- A Kubernetes cluster with the JetStream controller installed.
  [Install]({{< relref "/docs/install" >}}) shows how.
- The namespace `payments`.
- A NATS cluster with JetStream that pods in `payments` can reach.
  The manifests use the client address `tls://nats.messaging.svc:4222`.
- In `payments`, a Secret `payments-secrets` with the creds of a user in the account that you manage, under the key `nats.creds`, and a Secret `messaging-nats-ca` with the CA of the NATS cluster under the key `ca.crt`.
  Whoever runs the auth plane of that NATS cluster gives you the creds.
- To follow every fork of this guide, the streams `PAYMENTS` and `LEDGER` in that account, created by something other than the JetStream controller.

## Connect to the NATS cluster

Apply a `NatsConnection` with the address, the CA and the creds.
Every resource that uses it lands in the account that the creds sign in to.

{{< manifest "01-natsconnection.yaml" >}}

## Adopt the streams {#adopting-streams}

Apply three `NatsStream`s.
Each one takes a different path with a stream that it did not create:

- `payments` sets `adoptionPolicy: Adopt`.
  Use `Adopt` to take over a stream that exists and keep its config: the spec needs only the fields that identify the stream.
  If the stream does not exist, the resource waits with `Adopted` False, reason `NotFound`, and does not create it.
- `refunds` sets `adoptionPolicy: AdoptOrCreate`.
  Use `AdoptOrCreate` to take over the stream if it exists and create it if not.
  The JetStream controller applies the spec as written either way.
- `ledger` leaves `adoptionPolicy` at its default, `Never`, and sets `terminalPolicy: Retry`.

{{< manifest "01-natsstreams.yaml" >}}

[AdoptionPolicy]({{< relref "/docs/reference/api#AdoptionPolicy" >}}) and [TerminalPolicy]({{< relref "/docs/reference/api#TerminalPolicy" >}}) in the API reference describe each value.
[Lifecycle policies]({{< relref "/docs/design/v1#63-lifecycle-policies" >}}) in the design describes ownership.

### Check the adopted stream

Read `payments`:

```sh
kubectl -n payments get natsstream payments -o yaml
```

The JetStream controller has written the config of the stream into the spec.
The output is similar to this:

{{< manifest "01-live-natsstream-payments.yaml" >}}

The `status` in the output is similar to this:

{{< manifest "01-status-natsstream-payments.yaml" >}}

### Check the stream that was not adopted

Read `ledger`:

```sh
kubectl -n payments get natsstream ledger -o yaml
```

`LEDGER` exists and the JetStream controller does not own it, so the resource stops and the stream on the server stays as it is.
The `status` in the output is similar to this:

{{< manifest "01-status-natsstream-ledger.yaml" >}}

To take `LEDGER` over, set `adoptionPolicy` to `Adopt` or `AdoptOrCreate`.
With `terminalPolicy: Retry`, the resource checks again at `nextCheckTime`, and `Terminal` clears once `LEDGER` is gone.
With the default, `Hold`, `Terminal` clears only when you edit the resource.

## Declare a key-value bucket and an object store

Apply a `NatsKeyValue` and a `NatsObjectStore` on the same connection.
They take the same lifecycle policies as a stream.

{{< manifest "01-natskeyvalue-objectstore.yaml" >}}

[NatsKeyValueSpec]({{< relref "/docs/reference/api#NatsKeyValueSpec" >}}) and [NatsObjectStoreSpec]({{< relref "/docs/reference/api#NatsObjectStoreSpec" >}}) in the API reference list their fields.

## Declare consumers

A consumer refers to its stream in one of two ways:

- If the stream has no `NatsStream`, set `stream` to the name of the stream on the server, as `ledger-audit` does.
  The connection of the consumer must land in the account of the stream.
- If the stream has a `NatsStream`, set `streamRef` to it, as `payments-settlement` does.
  The consumer then uses the connection of the stream, and the JetStream controller creates the consumer once the stream is `Ready`.

{{< manifest "01-natsconsumer.yaml" >}}

Deleting a `NatsConsumer` deletes the consumer on the server unless the resource sets `deletionPolicy: Retain`.
[NatsConsumerSpec]({{< relref "/docs/reference/api#NatsConsumerSpec" >}}) in the API reference lists every field.
