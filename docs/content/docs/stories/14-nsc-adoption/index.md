---
title: Adopt a NATS operator and accounts made with nsc
weight: 14
params:
  category: The auth plane
  tags: [keys, adoption]
  e2e:
    waits:
      - {step: 0, wait: 3m, reason: "the existing NATS cluster's servers start, and its accounts are pushed to them"}
      - {step: 1, wait: 2m, reason: "the auth controller finds the servers before it pushes the system account"}
    substitutions:
      - files: [01-status-natsoperator.yaml]
        reason: the public keys of the NATS operator and system account e2e/00-messaging.yaml runs under, from internal/e2e/fixtures
        patchFile: e2e/natsoperator-status.json
      - files: [01-status-natssystemaccount.yaml]
        reason: the public key of the system account e2e/00-messaging.yaml runs under, and the user that account revokes, from internal/e2e/fixtures
        patchFile: e2e/natssystemaccount-status.json
      - files: [02-status-natsaccount.yaml, 03-status-natsaccount.yaml, 04-status-natsaccount.yaml]
        reason: the public key of the account the Job in e2e/00-messaging.yaml pushes, and the user that account revokes, from internal/e2e/fixtures
        patchFile: e2e/natsaccount-status.json
---

This guide shows you how to hand a NATS operator, its system account and an account that you made with `nsc` to the auth controller, on a NATS cluster that already runs under them.
The manifests adopt the NATS operator `acme`, the system account `SYS` and the account `orders`, and then raise a limit of `orders` from its `NatsAccount`.

Each account keeps its public key, so the creds of its users keep working.
The claims of each account do not carry over: the auth controller signs a new JWT from the spec of the `NatsAccount` and pushes it over the one that `nsc` signed, and refuses to sign while the spec would drop a claim that the JWT on the servers carries.

To start a NATS operator that has no accounts yet, see [Put a NATS cluster under a NATS operator]({{< relref "/docs/stories/02-auth-plane" >}}).

## Before you begin

You need:

- A NATS cluster with a `Full` resolver that pods of the auth controller can reach, running under a NATS operator that you made with `nsc`.
  The manifests use the client address `nats://nats.messaging.svc:4222`.
- A Kubernetes cluster with the auth controller installed, the chart value `auth.systemConnection` set to `nats-system/auth-controller`, and the namespace `nats-system`.
  [Install]({{< relref "/docs/install" >}}) shows how.
- The `nsc` store and keystore of that NATS operator, with the seeds of the identity key and of every signing key of the NATS operator, of the system account and of each account that you adopt.
- The creds of a user of the system account, issued by `nsc`.
- The [NATS CLI](https://github.com/nats-io/natscli).

## Compare each account with what a NatsAccount can keep

The first JWT that the auth controller signs for an account replaces the one on the servers, and holds what the spec of the `NatsAccount` says.
Before it signs, the auth controller compares the two JWTs.
If the one on the servers carries a claim that the new one would not, the auth controller signs nothing, and the `NatsAccount` has the condition `Ready` False with the reason `AdoptionDropsClaims`, naming each such claim by its field in the JWT.
A claim that the spec sets to another value passes.
[Adopt the account](#adopt-the-account) shows a refusal.

Print the claims of each account, and of the NATS operator:

```sh
nsc describe operator
nsc describe account orders
```

Write the whole spec before you apply it:

- List every signing key of the account under `keys.signing`.
  A user whose JWT was signed by a key that the list leaves out can no longer connect, and the servers close its connections.
- Give each scoped signing key its `scope`: the role, the permissions, the connection types, and the limits for subscriptions and payload.
  A scoped key that you list without its `scope` becomes a plain signing key, and its users fail with `maximum subscriptions exceeded`.
- Set `limits` for connections, subscriptions, payload and JetStream.
  A limit that you omit is unlimited, and an account without `limits.jetstream` has no JetStream.
  If the account has JetStream limits by tier, list each tier under `limits.jetstream.tiers` by its name.
  The names are `R1` to `R5`: `R` and the replica count of the streams that the tier limits.
  The auth controller refuses to adopt an account with a tier under any other name, with the reason `TierInexpressible`, whatever `adoption` says: nats-server never read such a tier, so remove it from the JWT and push the account before you adopt it.
- Declare every export under `exports`, and every import under `imports`.
  Apply an account that exports before the accounts that import from it: an import from a `NatsAccount` that does not exist is left out of the JWT.
  While the exporting `NatsAccount` exists but has no public key yet, the importing account is not signed: its `NatsAccount` has the condition `Ready` False with the reason `ExporterPending`, and its JWT on the servers stays as it is.
- Declare an import from the system account with `accountRef` of kind `NatsSystemAccount`.
  The export is `account-monitoring-services` or `account-monitoring-streams`, and the auth controller puts the public key of the account where `nsc` put it.
- Declare an import from an account that you do not adopt, or that another NATS operator signs, with `publicKey` in place of `accountRef`, and copy the `subject` and the `type` of the import from the claims.
  `nsc describe account orders --json` prints them under `nats.imports`, with the activation `token` of an import of a private export.
  Put that token in a Secret and name it under `activation.secretKeyRef`.
  An import whose token does not fit is left out of the JWT, and the condition `ReferencesResolved` of the `NatsAccount` says why.
- Set `share` on an import of a service and `allowTrace` on an import of a stream if the claims set them.
- List an importer of a private export by `publicKey` under `importers` if no `NatsAccount` describes the importing account, which may stay with `nsc`.
  The auth controller mints that account an activation token into `status.exports`, under the name of the export and the public key.
  Write the token to a file and give it to whoever holds the importing account, for `nsc add import --token`.
  The auth controller mints a new token when the signing key that signed the old one is retiring or gone, and the importing account needs that one instead.
- Set `jwtTTL`.
  A JWT from `nsc` does not expire unless you gave it an expiry.
  A JWT from the auth controller expires after `jwtTTL`, 48h by default, and the auth controller signs it again before then.
  A `jwtTTL` of 0 never expires.

No `NatsAccount` can keep these claims:

- In the scope of a scoped signing key: the response permissions, the source networks, the times, the locale, the bearer token flag, the data limit and the description.
- Subject mappings, default permissions, and auth callout.
- The latency sampling and the account token position of an export.
- The description and the tags.

To adopt an account that has one of them, set `adoption.droppedClaims` to `Accept` in the spec of the `NatsAccount`.
The auth controller then signs the account without the claims that the condition named, and pushes the JWT.
The field has no effect after the first JWT is signed.

Revocations carry over: the auth controller reads them from the JWT on the servers.
An account that has no signing key keeps its revocations too, and rotating a signing key later does not drop them.
An account that revokes every user, with the key `*`, keeps that revocation too: users whose JWTs were issued at or before its time stay refused.

The system account keeps its revocations on the same terms, and rotating its signing keys later does not drop them.
It keeps the two exports that `nsc` gives it, `account-monitoring-services` and `account-monitoring-streams`, which the auth controller signs into every system account.
No `NatsSystemAccount` can keep any other export, or an import: the auth controller refuses to sign a system account that has one, in the same way, and the `NatsOperator` and the `NatsSystemAccount` both have the condition `Ready` False with the reason `AdoptionDropsClaims`.
Set `adoption.droppedClaims` to `Accept` in the spec of the `NatsSystemAccount` to accept the loss.

[AccountLimits]({{< relref "/docs/reference/api#AccountLimits" >}}), [Export]({{< relref "/docs/reference/api#Export" >}}) and [Import]({{< relref "/docs/reference/api#Import" >}}) in the API reference list every field.

## Put the seeds in Secrets

`nsc env` prints the directory of the keystore in the row `$NKEYS_PATH`.
Under it, the seed of a key is the file `keys/<first letter of the public key>/<next two letters>/<public key>.nk`.
`nsc describe` prints the public key of each identity key and signing key.

Create one Secret for the NATS operator, one for the system account and one for each account, with the seed of the identity key and of each signing key:

```sh
kubectl -n nats-system create secret generic acme-operator-keys \
  --from-literal=identity="$(cat <operator-identity>.nk)" \
  --from-literal=signing-1="$(cat <operator-signing>.nk)"
kubectl -n nats-system create secret generic sys-keys \
  --from-literal=identity="$(cat <sys-identity>.nk)" \
  --from-literal=signing-1="$(cat <sys-signing>.nk)"
kubectl -n nats-system create secret generic orders-keys \
  --from-literal=identity="$(cat <orders-identity>.nk)" \
  --from-literal=signing-1="$(cat <orders-signing>.nk)"
```

Replace each `<...>.nk` with the path of that seed.

The servers trust the signing keys in the NATS operator JWT of their config, and nothing in this guide changes that config.
The first signing key that you list for the NATS operator signs every account, so it must be a key that the JWT in the config lists.
If it is not, the auth controller sends the servers no JWT, and the system account and each account read `Distributed` False with the reason `UntrustedSigner`.
The `NatsOperator` reads `SigningKeyUntrusted` True with the same reason, and its message names the key and how many servers do not list it.

The auth controller reads which keys a server trusts from its `VARZ`.
A server that does not report a NATS operator JWT there is not counted current in `status.distribution`, and each account reads `Distributed` False with the reason `TrustUnknown`, which names how many servers do not say; the `NatsOperator` reads `SigningKeyUntrusted` Unknown with the same reason.
A server configured with `trusted_keys` in place of `operator` reports none, and reads so until its config names the NATS operator JWT.
A server answers no `VARZ` while the creds of the auth controller come from a user of the `auth-controller` preset signed before 0.4.0; issue those creds again.

If you keep an identity seed offline, leave `keys.identity` out.
Set `jwt` of the `NatsOperator` to the NATS operator JWT, or `publicKey` of the account to its public key, and list at least one signing key.

## Connect the auth controller to the servers

The auth controller pushes account JWTs through the `NatsConnection` set in `auth.systemConnection`.
Use the creds that `nsc` issued for a user of the system account.

Under the keystore, the creds of a user are the file `creds/<operator>/<account>/<user>.creds`.
Create the Secret from the creds of the user `sys`:

```sh
kubectl -n nats-system create secret generic auth-controller-creds \
  --from-file=user.creds=<sys>.creds
```

Apply the `NatsConnection`:

{{< manifest "01-natsconnection.yaml" >}}

## Adopt the NATS operator and the system account

Apply the `NatsSystemAccount` and the `NatsOperator`.
Each one names its seeds under `keys`.

{{< manifest "01-natsoperator.yaml" >}}

`nsc` signs the creds of the user `sys` with the signing key of `SYS`, so the `NatsSystemAccount` must list that key.
Without it, the auth controller loses its own connection when the servers get the new JWT.

Read the status of the `NatsOperator`:

```sh
kubectl -n nats-system get natsoperator acme -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natsoperator.yaml" >}}

`publicKey`, `signingKeys` and `systemAccount.publicKey` are the keys that `nsc describe` printed.
`jwt` is a NATS operator JWT that the auth controller signed with the identity key.
The servers keep the one in their config, and do not restart.
`systemAccount.revocations` lists the revocations in the system account JWT that the auth controller signed.

Read the status of the `NatsSystemAccount`:

```sh
kubectl -n nats-system get natssystemaccount sys -o yaml
```

The `status` in the output is similar to this:

{{< manifest "01-status-natssystemaccount.yaml" >}}

`revocations` lists the user that the system account revoked under `nsc`.
Its `issuers` are the identity key and the signing key of the system account, since either may have signed that user's JWT.

The config of the servers still preloads the system account JWT that `nsc` signed, under `resolver_preload`.
A server that restarts with its resolver `dir` intact keeps the JWT that the auth controller pushed, because the server stores a preloaded JWT only over an older one.
A server that starts with an empty `dir`, such as an `emptyDir` volume in a pod that was recreated, serves the JWT from `nsc` until the auth controller pushes its own again, about one roster poll later.
Until then, users that the new JWT revokes can connect, and users signed by keys that only the new JWT lists cannot.
To close that gap, replace the preloaded system account JWT in the config with `status.systemAccount.jwt` of the `NatsOperator`, or keep the resolver `dir` on a persistent volume.

When `Distributed` is True, the auth controller can ask every server for the JWT that it holds, which is where the revocations of an account come from.
If you apply an account before that, the auth controller signs its JWT and does not push it.
The `NatsAccount` reads `RevocationsUnrecovered` True and `Distributed` False, with the reason `Unreachable`, until every server has answered.

## Adopt the account

To see what a refusal looks like, apply the `NatsAccount` `orders` first with a spec that leaves its export out:

{{< manifest "02-natsaccount.yaml" >}}

Read the status:

```sh
kubectl -n nats-system get natsaccount orders -o yaml
```

The `status` in the output is similar to this:

{{< manifest "02-status-natsaccount.yaml" >}}

The auth controller signed nothing, and the JWT on the servers stays as `nsc` pushed it.
`exports[0]` is the first export in that JWT: `nsc describe account orders --json` prints it under `nats.exports`.

Apply the account again, with its whole spec:

{{< manifest "03-natsaccount.yaml" >}}

Read the status again.
It is similar to this:

{{< manifest "03-status-natsaccount.yaml" >}}

- `publicKey` is the public key that the account had under `nsc`.
- `distribution` counts the servers that hold the new JWT.
- `revocations` lists the user that the account revoked under `nsc`.

From here on, change the account in its `NatsAccount`.
The auth controller creates the creds of a new user from a `NatsUser`, as [Put a NATS cluster under a NATS operator]({{< relref "/docs/stories/02-auth-plane" >}}) shows.
A `NatsUser` that sets `publicKey` to the public key of a user that `nsc` issued claims that user.
When you delete it, the auth controller revokes the key under the identity key of the account as well as its signing keys, so rotating the signing keys later does not let the user back in.

## Check the users that nsc issued

1. Forward the client port of the NATS cluster to your machine:

   ```sh
   kubectl -n messaging port-forward svc/nats 4222:4222 &
   ```

1. Read a stream with the creds of a user that the identity key of the account signed:

   ```sh
   nats -s nats://localhost:4222 --creds orders-app.creds stream info ORDERS
   ```

1. Publish with the creds of a user that the signing key of the account signed:

   ```sh
   nats -s nats://localhost:4222 --creds orders-worker.creds pub orders.created '{"id": 2}'
   ```

1. Publish with the creds of the user that the account revoked:

   ```sh
   nats -s nats://localhost:4222 --creds orders-old.creds pub orders.created '{"id": 0}'
   ```

   The server refuses the connection with `Authorization Violation`.

1. Publish with the creds of the user that the system account revoked:

   ```sh
   nats -s nats://localhost:4222 --creds sys-old.creds pub orders.created '{"id": 0}'
   ```

   The server refuses this connection in the same way.

## Change a limit of the account

Apply the account `orders` with its stream limit raised from 10 to 20:

{{< manifest "04-natsaccount.yaml" >}}

The auth controller pushes the new JWT to the servers, and no server restarts.
Read the status of the account:

```sh
kubectl -n nats-system get natsaccount orders -o yaml
```

The `status` in the output is similar to this:

{{< manifest "04-status-natsaccount.yaml" >}}
