# Security

## Reporting a vulnerability

Report a vulnerability privately through [GitHub's private vulnerability reporting](https://github.com/mikluko/nats-operator/security/advisories/new) on this repository, not in a public issue. A report is acknowledged within seven days.

## Supported versions

Only the latest release is supported; a security fix ships as a new release.

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
