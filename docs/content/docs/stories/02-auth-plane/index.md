---
title: Owning the auth plane
weight: 2
params:
  e2e:
    substitutions:
      - files: [01-natscluster.yaml]
        reason: three servers share one kind node, on a host every kind cluster of the run shares
        patch: {spec: {resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 256Mi}}}}
---

The platform engineer from the first story wants that NATS cluster to run under a NATS operator the auth controller owns, with accounts and users declared as resources instead of minted by hand.

## NATS operator and system account

The auth controller generates the NATS operator's keys and keeps each seed in a Secret. The NATS operator names exactly one system account; others may exist unreferenced, and flipping the reference is how one is rotated.

{{< manifest "01-natsoperator.yaml" >}}

{{< manifest "01-status-natsoperator.yaml" >}}

## Accounts

The system account is a kind of its own so RBAC can grant it to the platform team alone. An ordinary account's limits are signed into its JWT, and the auth controller configures nothing about JetStream beyond that.

Whoever writes a `NatsAccount` sets its limits. A limit left out or set to 0 is unlimited, and an account without `limits.jetstream` has no JetStream. A `NatsReferenceGrant` admitting `NatsAccount`s from another namespace to a `NatsOperator` therefore lets that namespace set its own accounts' limits.

{{< manifest "01-natsaccounts.yaml" >}}

An account change is pushed to the servers' resolvers without a restart; status says how many servers hold the current JWT.

{{< manifest "02-natsaccount-orders.yaml" >}}

{{< manifest "02-status-natsaccount-orders.yaml" >}}

The account JWT expires its `jwtTTL` after it was signed, 48h here, and the auth controller re-signs it at half that. The auth controller is therefore an availability requirement: down for longer than half a `jwtTTL`, it may let the orders account expire, and the servers then close its connections. The gauge `nats_operator.account.jwt_expiry` says when that happens.

## Users

A user's creds can land in a Secret shaped the way a NatsConnection reads it, so a connection needs only the Secret's name. The controllers' own system users take a permission preset instead of a permission list.

{{< manifest "01-natsusers.yaml" >}}

The auth controller signs without a connection, and pushes to the servers through the one its chart value `auth.systemConnection` names, with its own system user's creds:

{{< manifest "01-natsconnection-auth-controller.yaml" >}}

A user that brings its own key gets only a signed JWT, in status:

{{< manifest "01-status-natsuser-orders-batch.yaml" >}}

## The NATS cluster and the stream

The `NatsCluster` gains an `auth` block. Its trust roots come through a trust object that points at the NATS operator, and its controller connects with its own system user's creds.

{{< manifest "01-natsoperatortrust.yaml" >}}

{{< manifest "01-natscluster.yaml" >}}

Pointing the stream's connection at one whose creds sign into the orders account creates a new, empty `ORDERS` stream in that account. Nothing deletes the stream story 1 created in the global account, or moves its messages.

{{< manifest "01-natsconnection.yaml" >}}

{{< manifest "01-natsstream.yaml" >}}

## Connecting a client

A client of the orders account connects with the creds the auth controller wrote for `orders-service`:

```sh
kubectl -n nats-system get secret orders-service-creds -o jsonpath='{.data.user\.creds}' | base64 -d > orders.creds
kubectl -n nats-system port-forward svc/demo 4222:4222 &
nats -s nats://localhost:4222 --creds orders.creds pub orders.created '{"id": 1}'
nats -s nats://localhost:4222 --creds orders.creds stream info ORDERS
```
