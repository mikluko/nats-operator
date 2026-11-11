# nats-operator

Three Kubernetes controllers that deploy NATS clusters, own their auth plane, and manage and balance JetStream. `CONTEXT.md` is the glossary and `docs/design/v1.md` is the design; both are read before anything is written, and "operator" unqualified is not used.

Clusters: `upeks-dev` (dev), `upeks-stage`, `upeks-produs` (production), `upsidian-dev`, `upsidian-prod` (production), `upops`; contexts of the same names.

## Changelog

`CHANGELOG.md` is Keep a Changelog 2.0.0, newest version first. An entry states what is different for the reader and nothing else: no reason, no history, no account of the work. Those belong in the commit and the pull request. An entry that needs a *because* is cut back to the change, and if nothing is left it is not an entry.

    - `NatsCluster` renders one StatefulSet per server.                          entry
    - Switched to one StatefulSet per server, because a shared one could not     not an entry
      change its volume template.

## Releases

The repository lives at `sarak.sh/mikluko/nats-operator`, and its workflows under `.forgejo/workflows` run on that Forgejo's runners: `ubuntu-latest` by default, `privileged` for the jobs that run kind on rootful podman.

The changelog decides the version. Cutting one is renaming `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and opening a fresh `[Unreleased]` above it. `https://sarak.sh/mikluko/changelog` validates the file on every pull request. A push to `main` runs `release-cut.yaml`: it runs ci, then tags the pushed commit `vX.Y.Z` with `RELEASE_TOKEN` where the changelog action reports the newest entry due: it names a version, no tag names it, and it is not a prerelease. ci runs on `main` only as that job. The pushed tag starts `release-roll.yaml`, which fails unless the tag is the newest entry of `CHANGELOG.md` at that commit, then builds, signs with the key `cosign.pub` verifies, and publishes the images under `sarak.sh/mikluko/nats-operator` and the chart at `oci://sarak.sh/mikluko/nats-operator/charts`, creates the Forgejo release last, and asks the Go proxy for the tag. A tag whose `release-roll.yaml` run failed is not cut again: re-run that run. Run by hand, `release-roll.yaml` builds the changelog's newest version if it is due, a prerelease included, and publishes nothing. Nothing else sets a version.

A push to `main` also runs `docs.yaml`, which pushes the site as the image `sarak.sh/mikluko/nats-operator/docs` for Flux to deploy at `https://sarak.sh/nats-operator/`.

Before the first release from the forge is announced, a human:

- sets the repository's Actions secrets `REGISTRY_TOKEN`, a package-write token of `agent`; `RELEASE_TOKEN`, a token that may push tags; and `COSIGN_PRIVATE_KEY` and `COSIGN_PASSWORD`, of a key pair `cosign generate-key-pair` makes
- commits that pair's public key as `cosign.pub` at the repository root, which `SECURITY.md` verifies with
- runs `release-roll.yaml` by hand on a branch whose `CHANGELOG.md` has the version cut, before cutting it on `main`; the run publishes nothing and proves the plan
- makes the packages `nats-operator/cluster-controller`, `nats-operator/auth-controller`, `nats-operator/jetstream-controller`, `nats-operator/charts/nats-operator` and `nats-operator/docs` public, where the owner's packages are not already

## Fenced acts

These are performed only by an attended session, or by an unattended run the maintainer has released a ticket to; everything else is prepared and left for a human:

- merging into `main`
- cutting a version in `CHANGELOG.md`, and tagging a release
- publishing images, the Helm chart, or the documentation site
