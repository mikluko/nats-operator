# Writing style

This sheet has the rules that are particular to this project.
For everything else, use these sources, in this order:

1. [Diátaxis](https://diataxis.fr/) for the type of a page and what belongs on it.
1. The [Kubernetes documentation style guide](https://kubernetes.io/docs/contribute/style/style-guide/) for wording and formatting.
1. The [Google developer documentation style guide](https://developers.google.com/style) and the [Microsoft Writing Style Guide](https://learn.microsoft.com/en-us/style-guide/welcome/) for anything the first two leave open.

A rule on this sheet overrides all of them.

## Terms

[`CONTEXT.md`](../CONTEXT.md) is the glossary.
Use its terms, and none of the words it lists under "Avoid".
"Operator" and "cluster" never appear unqualified: write "controller", "NATS operator", "NATS cluster" or "Kubernetes cluster".

## Page types

Each page is one Diátaxis type.
Content of another type gets one sentence and a link to the page it belongs on.

Table: The type of each page.

| Page | Its reader | Type |
|---|---|---|
| Front page, `README.md`, `charts/nats-operator/README.md` | is deciding whether to use the project | None of the four. A short explanation of what the project is, the shortest how-to guide that gets it running, and links, each under its own heading. |
| Story 1, the quickstart | is new to these controllers and learning them | Tutorial |
| Every other story | knows NATS and Kubernetes and has that goal in hand | How-to guide |
| Stories index | is choosing a story | Landing page: what the section contains, and links |
| Install | has decided and wants the chart installed, upgraded or removed | How-to guide |
| Pages under `docs/content/docs/reference` | is at work and looking up a fact | Reference |
| `SECURITY.md` | is reporting a vulnerability or judging the trust model | None of the four. How to report, the trust boundaries as reference, and how to verify a release. |
| `docs/README.md` | is contributing to the site | How-to guide |

## Stories

A story is a directory under `docs/content/docs/stories`.
The end-to-end harness applies the manifests in it and waits until the live objects match its status files, so the page and the test are the same files.
[`hack/e2e/README.md`](../hack/e2e/README.md) describes how the harness reads them.

An edit to a story's prose must leave these as they are:

- The YAML of every manifest and status file, apart from its comments.
- Every file name. The harness takes the step and the role of a file from its name.
- The front matter under `params.e2e`.
- The directory name, which is the page's address.
- The set of `manifest` shortcodes on the page. You can move one, but the page keeps showing every file it showed.

The `manifest` shortcode prints the file's name, its YAML, and the `kubectl` command that applies it or deletes what it names.
For a status file it prints the name and the YAML.
A value that the harness treats as an example carries the tag `!any` in the file, and the shortcode removes the tag.

### Comments in a manifest

A comment in a manifest is copy, and it follows the type of its page.
A comment that lists the values a field takes, gives a default or explains a behavior is reference: delete it, and link the [API reference](content/docs/reference/api.md) from the page.
A status file has no comments.

### Commands the page adds

A page can add a command that the story's files do not have, such as the `kubectl get` that shows a status.
Run it against a Kubernetes cluster before you add it.

## Departures from the Kubernetes style guide

| The Kubernetes guide says | This project does | Why |
|---|---|---|
| The command comes first, in its own block, then "The output is similar to this:", then the output. | For a manifest, the `manifest` shortcode prints the YAML and then the command. For a status file, write the `kubectl get` command in a block, then "The `status` in the output is similar to this:", then the shortcode. | The shortcode builds the command from the file's address, so the two stay together. A status file has the `status` stanza alone. |
| A procedure is a numbered list. This rule is from the Google guide. | A step that shows a manifest is a second-level heading in the imperative, with no number. Commands with no manifest between them go in a numbered list. | The Markdown rendition of a `manifest` shortcode is not indented, so it ends the list item it is in. |
| The page title is in title case. | The page title is in sentence case. | Title case would turn the glossary's "NATS cluster" into "NATS Cluster", which reads as a kind. |

The quickstart addresses its reader as "you", as every other page does.
Diátaxis allows a tutorial "we".

## Generated pages

Do not edit a page under `docs/content/docs/reference` that a tool generates.
Edit its source, then run `just verify`.

Table: The source of each generated page.

| Page | Source |
|---|---|
| `api.md` | The doc comments under `api`, and `hack/api-docs` |
| `nats-permissions.md` | `hack/permdocs` |
| `telemetry.md` | `hack/telemetrydocs` |

## Verify

Every sentence is true of the tree at the commit that ships it.
Before you open a pull request:

1. Run every command on the page, open every path, and follow every link.
1. Follow the quickstart from its first step to its last on a Kubernetes cluster that has the chart installed and nothing else, if you changed it.
1. If you renamed a heading, find the links to its anchor and update them.
1. Run the checks from the repository root:

   ```sh
   just site-check
   go test ./hack/... ./internal/e2e/...
   just verify
   ```
