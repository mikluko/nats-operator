---
title: Put a NATS cluster under a NATS operator
weight: 2
params:
  e2e:
    substitutions:
      - files: [01-natscluster.yaml]
        reason: three servers share one kind node, on a host every kind cluster of the run shares
        patch: {spec: {resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 256Mi}}}}
---

This guide shows you how to run a NATS cluster under a NATS operator that the auth controller owns, with its accounts and users declared as Kubernetes resources.
The manifests create the NATS operator `demo`, the accounts `sys` and `orders`, the users of both, the NATS cluster `demo`, and the stream `ORDERS` in the account `orders`.

## Before you begin

You need:

- A Kubernetes cluster with the cluster controller, the auth controller and the JetStream controller installed, and the chart value `auth.systemConnection` set to `nats-system/auth-controller`.
  [Install]({{< relref "/docs/install" >}}) shows how.
- The namespace `nats-system`, with no `NatsCluster` named `demo` in it.
  If you followed [the quickstart]({{< relref "/docs/stories/01-quickstart" >}}), do its clean-up first.
- Nodes with 3 CPUs and 12Gi of memory that pods can still request, and a StorageClass named `standard`.
- The [NATS CLI](https://github.com/nats-io/natscli).

## Create the NATS operator

Apply the `NatsOperator`.
Its system account is `sys`.

{{< manifest "01-natsoperator.yaml" >}}

The manifest has no `keys`, so the auth controller generates the identity key and a signing key, and keeps each seed in a Secret.
To adopt seeds that you already have, set `keys` instead.
[Keys and trust]({{< relref "/docs/design/v1#51-keys-and-trust" >}}) in the design describes both, and how to rotate a key.

Read the status:

```sh
kubectl -n nats-system get natsoperator demo -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natsoperator.yaml" >}}

`seedSecrets` lists the Secrets that contain the seeds.

## Declare the accounts

Apply the system account `sys` and the account `orders`.

{{< manifest "01-natsaccounts.yaml" >}}

The limits of `orders` are signed into its JWT.
An account without `limits.jetstream` has no JetStream.
[AccountLimits]({{< relref "/docs/reference/api#AccountLimits" >}}) in the API reference lists every limit.

The JWT of `orders` expires after its `jwtTTL` of 48h, and the auth controller signs it again before then.
If the auth controller is down for longer than half of `jwtTTL`, the JWT can expire, and the servers then close the connections of the account.
Watch the gauge `nats_operator.account.jwt_expiry`, described in [Telemetry]({{< relref "/docs/reference/telemetry" >}}).

## Declare the users

Apply the users.
`orders-service` is the client of this guide, `orders-batch` brings its own key, `orders-jetstream` is the user the JetStream controller acts as in `orders`, and the last three are the controllers' own users in the system account.

{{< manifest "01-natsusers.yaml" >}}

Each user with `credentials` gets its creds in that Secret, under the key `user.creds`.
A controller's user sets a `preset` in place of `permissions`.
[NATS permissions]({{< relref "/docs/reference/nats-permissions" >}}) lists the subjects of each preset.

A user that sets `publicKey` gets no Secret.
Read the JWT that the auth controller signed for `orders-batch`:

```sh
kubectl -n nats-system get natsuser orders-batch -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natsuser-orders-batch.yaml" >}}

Give `status.jwt` to the client, which already has the seed.

## Connect the auth controller to the servers

The auth controller pushes account JWTs to the servers through the `NatsConnection` set in `auth.systemConnection`.
Apply it, with the creds of the user `auth-controller`:

{{< manifest "01-natsconnection-auth-controller.yaml" >}}

Until the connection reaches a server, the auth controller signs JWTs and no server receives them.

## Deploy the NATS cluster under the NATS operator

Apply the `NatsOperatorTrust`.
It points at the `NatsOperator`, and the auth controller writes the trust roots into its status.

{{< manifest "01-natsoperatortrust.yaml" >}}

In a Kubernetes cluster where no auth controller runs, the `NatsOperatorTrust` contains the JWTs themselves.
[Join NATS clusters in several Kubernetes clusters into a supercluster]({{< relref "/docs/stories/06-supercluster" >}}) shows that form.

Apply the `NatsCluster`.
`auth.trustRef` points at the trust roots, and `auth.systemCredentials` at the creds that the cluster controller connects with.

{{< manifest "01-natscluster.yaml" >}}

## Create the stream in the account

A `NatsConnection` decides the account of every resource that uses it, through its credentials.
Apply a connection with the creds of `orders-jetstream`, and the stream `ORDERS` on it:

{{< manifest "01-natsconnection.yaml" >}}

{{< manifest "01-natsstream.yaml" >}}

## Publish as a user of the account

1. Write the creds of `orders-service` to a file:

   ```sh
   kubectl -n nats-system get secret orders-service-creds -o jsonpath='{.data.user\.creds}' | base64 -d > orders.creds
   ```

1. Forward the client port of the NATS cluster to your machine:

   ```sh
   kubectl -n nats-system port-forward svc/demo 4222:4222 &
   ```

1. Publish a message, and read the state of the stream:

   ```sh
   nats -s nats://localhost:4222 --creds orders.creds pub orders.created '{"id": 1}'
   nats -s nats://localhost:4222 --creds orders.creds stream info ORDERS
   ```

## Change the limits of an account

Apply the account `orders` with its connection limit raised from 500 to 1000:

{{< manifest "02-natsaccount-orders.yaml" >}}

The auth controller pushes the new JWT to the servers, and no server restarts.
Read the status of the account:

```sh
kubectl -n nats-system get natsaccount orders -o yaml
```

The `status` in the output is similar to this:

{{< manifest "02-status-natsaccount-orders.yaml" >}}

`distribution` counts the servers that hold the current JWT.
[Distribution]({{< relref "/docs/design/v1#52-distribution" >}}) in the design describes how the auth controller finds that out.
