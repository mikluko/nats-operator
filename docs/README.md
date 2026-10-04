# docs

This directory is the Hugo site published at <https://mikluko.github.io/nats-operator/>, on the [Hudocs](https://hudocs.com/) theme.
This guide shows you how to build the site and check it before you open a pull request.
[`STYLE.md`](STYLE.md) is the writing style for the site's pages.

## Before you begin

You need:

- Go on `PATH`. The theme is a Hugo module pinned in `go.mod`, and Hugo uses Go to download it.
- Hugo extended.
- `just`, to run the link check.

`hugo.toml` mounts `design/` and `adr/` into the site's content under `content/docs/`.
Edit a design document or an ADR in its own directory, not under `content/`.

## Build the site

From this directory, run one of these:

- To serve the site locally and rebuild it on every change, run `hugo server`.
- To build the site into `public/`, run `hugo`.

Every page is also rendered as Markdown, `index.md` beside its `index.html`, and the home page as `llms.txt`, which links the Markdown of every page under `docs/`.
A page's head links its renditions with `rel="alternate"`.

## Check the site

From the repository root:

1. Build the site and test what it renders:

   ```sh
   go test ./hack -run TestSite
   ```

   `TestSite` fails on:

   - a page with more than one `h1`
   - a redirect to a page the site does not have
   - a header that does not draw its logo icon
   - a page served from a story's `e2e/` directory
   - a page with no Markdown rendition, or no alternate link to it
   - a rendition that contains an unrendered shortcode
   - a page under `docs/` that `llms.txt` does not link

   `TestSiteIconSet` fails when `assets/meteor-icons/icons.json` is not the pinned meteor-icons release's.

1. Check the site's links and fragments with lychee:

   ```sh
   just site-check
   ```

## Publish the site

`.github/workflows/docs.yml` builds the site on every pull request and deploys it on every push to `main`.
Run by hand, the workflow keeps the build as the run's `github-pages` artifact and deploys nothing.

The deployment needs one repository setting, made once: **Settings > Pages > Build and deployment > Source: GitHub Actions**.
