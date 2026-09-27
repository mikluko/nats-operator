---
title: Owning the auth plane
weight: 2
params:
  e2e:
    substitutions:
      - files: [01-natscluster.yaml]
        reason: three servers share one 3G minikube node
        patch: {spec: {resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 256Mi}}}}
---

The platform engineer from the first story wants that cluster to run under a NATS operator the auth controller owns, with accounts and users declared as resources instead of minted by hand.

## Operator and system account

The auth controller generates the operator's keys and keeps each seed in a Secret. The operator names exactly one system account; others may exist unreferenced, and flipping the reference is how one is rotated.

{{< manifest "01-natsoperator.yaml" >}}

{{< manifest "01-status-natsoperator.yaml" >}}

## Accounts

The system account is a kind of its own so RBAC can grant it to the platform team alone. An ordinary account's limits are signed into its JWT, and the auth controller configures nothing about JetStream beyond that.

{{< manifest "01-natsaccounts.yaml" >}}

An account change is pushed to the servers' resolvers without a restart; status says how many servers hold the current JWT.

{{< manifest "02-natsaccount-orders.yaml" >}}

{{< manifest "02-status-natsaccount-orders.yaml" >}}

## Users

A user's creds can land in a Secret shaped the way a NatsConnection reads it, so a connection needs only the Secret's name. The controllers' own system users take a permission preset instead of a permission list.

{{< manifest "01-natsusers.yaml" >}}

The auth controller signs without a connection, and pushes to the servers through the one its chart value `auth.systemConnection` names, with its own system user's creds:

{{< manifest "01-natsconnection-auth-controller.yaml" >}}

A user that brings its own key gets only a signed JWT, in status:

{{< manifest "01-status-natsuser-orders-batch.yaml" >}}

## The cluster and the stream

The cluster gains an `auth` block. Its trust roots come through a trust object that points at the operator, and its controller connects with its own system user's creds.

{{< manifest "01-natsoperatortrust.yaml" >}}

{{< manifest "01-natscluster.yaml" >}}

The stream moves into the account by changing its connection to one whose creds sign into that account.

{{< manifest "01-natsconnection.yaml" >}}

{{< manifest "01-natsstream.yaml" >}}
