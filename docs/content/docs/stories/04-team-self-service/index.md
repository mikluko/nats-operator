---
title: Let a team declare its own users and streams
weight: 4
params:
  e2e:
    after: 2
---

This guide shows you how to let a team declare NATS users and streams in its own namespace, in an account that the platform team owns in `nats-system`.
The team never reads `nats-system`.
The manifests give the payments team the account `payments` and the namespace `payments`.

## Before you begin

You need:

- The NATS operator `demo`, its NATS cluster `demo` and the auth controller's connection from [Put a NATS cluster under a NATS operator]({{< relref "/docs/stories/02-auth-plane" >}}).
- The namespaces `payments` and `orders`.

## Declare the account and the grant

As the platform team, apply the account `payments` and a `NatsReferenceGrant` in `nats-system`.
The grant lets a `NatsUser` in the namespace `payments` refer to the account `payments`, and to nothing else in `nats-system`.

{{< manifest "01-platform.yaml" >}}

To admit every `NatsAccount` in `nats-system`, leave out `name` under `to`.
[NatsReferenceGrantSpec]({{< relref "/docs/reference/api#NatsReferenceGrantSpec" >}}) in the API reference lists the fields.

Before you grant a namespace access, know what the grant gives it:

- A grant that admits `NatsUser`s to an account lets that namespace claim any user key of the account, including keys that the auth controller did not issue, and revoke it by deleting the `NatsUser`.
- Keep `NatsAccount`s in `nats-system`.
  A grant that admits `NatsAccount`s from another namespace to the `NatsOperator` lets that namespace set the limits of its own accounts and take any account key that no `NatsAccount` records yet.

[Tenancy]({{< relref "/docs/design/v1#7-tenancy" >}}) in the design gives the reasons.

## Declare the team's users and streams

As the payments team, apply the users, the connection and the stream in `payments`:

- `payments-api` is the client of the team.
- `payments-jetstream` is the user that the JetStream controller acts as in the account.
- The `NatsConnection` and the `NatsStream` stay in `payments`, so they need no grant.

{{< manifest "01-team.yaml" >}}

Each `accountRef` names the namespace `nats-system`, which the grant `payments-users` admits.
The creds Secrets are written beside the users, in `payments`.

## Check a user that no grant admits

A `NatsUser` in a namespace that no grant covers is refused.
Apply one in `orders`:

{{< manifest "01-natsuser-payments-reader.yaml" >}}

Read its status:

```sh
kubectl -n orders get natsuser payments-reader -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natsuser-payments-reader.yaml" >}}

If you delete a grant, the auth controller revokes the users that it admitted and deletes their creds Secrets.
