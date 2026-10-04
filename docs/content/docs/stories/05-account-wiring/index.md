---
title: Share a stream and a service between accounts
weight: 5
params:
  e2e:
    after: 2
---

This guide shows you how to export a stream and a service from one account and import both into another.
The auth controller signs the exports and imports into the JWTs of both accounts.
In the manifests, the account `monitoring` publishes check results and answers execute requests, and the account `core` imports both.

## Before you begin

You need the NATS operator `demo`, its NATS cluster `demo` and the auth controller's connection from [Put a NATS cluster under a NATS operator]({{< relref "/docs/stories/02-auth-plane" >}}).

## Declare the exports

Apply the account `monitoring` with two exports:

- `check-results`, a stream export.
  It sets no `access`, so any account can import it.
- `execute`, a service export with `access: Private`.
  Only the accounts under `importers` can import it, and the auth controller signs an activation token for each of them.

{{< manifest "01-exporter.yaml" >}}

[Export]({{< relref "/docs/reference/api#Export" >}}) in the API reference lists the fields of an export and their defaults.

## Declare the imports

Apply the account `core`.
Each import refers to an account and to the name of an export.
The subject and the type come from the export.

{{< manifest "01-importer.yaml" >}}

`localSubject` sets the subject that the import has in `core`.
Without it, the import keeps the exported subject.

To import from an account in another namespace, the namespace of the exporting account needs a `NatsReferenceGrant` that admits the importing `NatsAccount`, as in [Let a team declare its own users and streams]({{< relref "/docs/stories/04-team-self-service" >}}).

## Check the imports

Read the status of `core`:

```sh
kubectl -n nats-system get natsaccount core -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natsaccount-core.yaml" >}}

`imports` lists the subject and the type of each import.
For the private export, `activation` is `Signed`.
[Exports and imports]({{< relref "/docs/design/v1#54-exports-and-imports" >}}) in the design describes the rules.
