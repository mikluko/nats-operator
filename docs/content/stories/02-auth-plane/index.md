---
title: Owning the auth plane
weight: 2
---

The platform engineer from the first story wants that cluster to run under a NATS operator the auth controller owns, with accounts and users declared as resources instead of minted by hand.

## Operator and system account

The auth controller generates the operator's keys and keeps each seed in a Secret. The operator names exactly one system account; others may exist unreferenced, and flipping the reference is how one is rotated.

{{< manifest "natsoperator.yaml" >}}

{{< manifest "status-natsoperator.yaml" >}}

## Accounts

The system account is a kind of its own so RBAC can grant it to the platform team alone. An ordinary account's limits are signed into its JWT, and the auth controller configures nothing about JetStream beyond that.

{{< manifest "natsaccounts.yaml" >}}

An account change is pushed to the servers' resolvers without a restart; status says how many servers hold the current JWT.

{{< manifest "status-natsaccount-orders.yaml" >}}

## Users

A user's creds can land in a Secret shaped the way a NatsConnection reads it, so a connection needs only the Secret's name. The controllers' own system users take a permission preset instead of a permission list.

{{< manifest "natsusers.yaml" >}}

A user that brings its own key gets only a signed JWT, in status:

{{< manifest "status-natsuser-orders-batch.yaml" >}}

## The cluster and the stream

The cluster gains an `auth` block. Its trust roots come through a trust object that points at the operator, and its controller connects with its own system user's creds.

{{< manifest "natsoperatortrust.yaml" >}}

{{< manifest "natscluster.yaml" >}}

The stream moves into the account by changing its connection to one whose creds sign into that account.

{{< manifest "natsconnection.yaml" >}}

{{< manifest "natsstream.yaml" >}}
