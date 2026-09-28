---
title: Team self-service
weight: 4
params:
  e2e:
    after: 2
---

The platform team owns `nats-system`: the NATS operator, the system account, every account and its limits. The payments team owns the `payments` namespace and wants to declare its own users and streams there, without reading anything in `nats-system`.

## What the platform team declares

The account, and a grant that lets users in `payments` attach to it. The grant sits in the namespace it opens up, so only someone who can write there can open it. It trusts `payments` with every user key of the account: a `NatsUser` there can claim any key the account's users hold, those issued outside the auth controller included, and deleting it revokes that key. Accounts stay in `nats-system` alone: whichever `NatsAccount` records an account key first holds it, so a namespace granted `NatsAccount`s to the `NatsOperator` could take any account key no `NatsAccount` records yet.

{{< manifest "01-platform.yaml" >}}

## What the payments team declares

Users that reference the account across namespaces, and everything JetStream in its own namespace. Creds Secrets land beside the users.

{{< manifest "01-team.yaml" >}}

## Without a grant

A user from a namespace no grant covers is refused, and because grants are checked on every reconcile, deleting one later revokes users it had admitted.

{{< manifest "01-natsuser-payments-reader.yaml" >}}

{{< manifest "01-status-natsuser-payments-reader.yaml" >}}
