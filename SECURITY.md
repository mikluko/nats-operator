# Security

## Reporting a vulnerability

Report a vulnerability privately through [GitHub's private vulnerability reporting](https://github.com/mikluko/nats-operator/security/advisories/new) on this repository, not in a public issue. A report is acknowledged within seven days.

## Supported versions

Only the latest release is supported; a security fix ships as a new release.

## Trust boundaries

Every kind is namespaced, and a reference crosses into another namespace only where a `NatsReferenceGrant` there admits it, as [design section 7](docs/design/v1.md#7-tenancy) states.

- A namespace granted `NatsAccount`s to a `NatsOperator` has accounts signed under it with the limits it declares, and can take any account key no `NatsAccount` records yet.
- A namespace granted `NatsUser`s to a `NatsAccount` can claim and revoke any user key of that account, keys issued outside the auth controller included.
- Whoever may write a `NatsOperator`, `NatsAccount` or `NatsSystemAccount` can sign with any seed stored in a Secret of its namespace, without permission to read Secrets: `keys.identity` and `keys.signing` may name any Secret there.
- Whoever may write a `NatsCluster` runs pods in its namespace with any privilege that namespace admits: `spec.podTemplate` is merged over the rendered pod.
- Each controller's ServiceAccount may list and watch Secrets in every namespace it watches, every namespace without `watchNamespaces`, which reads their data; the controllers' metadata-only watch limits what they cache, not what they are permitted.
- The chart's `watchNamespaces` confines every controller, and its RBAC, to the namespaces it names; it is the install for a Kubernetes cluster shared between tenants.

## Verifying a release

Each release's controller images and chart are signed keylessly with cosign by the release workflow. With `<version>` a release's version without the `v`, such as `0.1.0`:

```sh
for c in cluster-controller auth-controller jetstream-controller; do
  cosign verify "ghcr.io/mikluko/nats-operator/$c:<version>" \
    --certificate-identity https://github.com/mikluko/nats-operator/.github/workflows/release.yml@refs/heads/main \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
done

cosign verify "ghcr.io/mikluko/nats-operator/charts/nats-operator:<version>" \
  --certificate-identity https://github.com/mikluko/nats-operator/.github/workflows/release.yml@refs/heads/main \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Each also carries a GitHub build provenance attestation:

```sh
gh attestation verify "oci://ghcr.io/mikluko/nats-operator/cluster-controller:<version>" --repo mikluko/nats-operator
```
