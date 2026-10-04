# docs

The Hugo site published at <https://mikluko.github.io/nats-operator/>. Its theme is this directory's own `layouts/` and `assets/`, with the Geist Mono and Manrope fonts under `static/fonts/` beside their licenses, so a build needs Hugo and nothing else. `hugo.toml` mounts `design/` and `adr/` into the site's content under `content/docs/`, so each document has one source.

[`STYLE.md`](STYLE.md) is the writing style for the site's pages.

Build locally from this directory with `hugo server`, or `hugo` into `public/`. `go test ./hack -run TestSite` from the repository root builds the site and fails on a page with more than one `h1`, a redirect to a page the site lacks, a search index entry that addresses a page or fragment the site lacks, a page served from a story's `e2e/` directory, a page with no Markdown rendition or no alternate link to it, a rendition that holds an unrendered shortcode, or a page under `docs/` that `llms.txt` does not link; `just site-check` checks its links and fragments with lychee.

Every page is also rendered as Markdown, `index.md` beside its `index.html`, and the home page as `llms.txt`, which links the Markdown of every page under `docs/`. A page's head names its renditions in `rel="alternate"` links.

`.github/workflows/docs.yml` builds the site on every pull request and deploys it on every push to `main`. Run by hand, it keeps the build as the run's `github-pages` artifact and deploys nothing. The deployment needs one repository setting, made once: **Settings → Pages → Build and deployment → Source: GitHub Actions**.
