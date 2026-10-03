# Writing style

Follow this sheet when you write or edit the documentation site under `docs/content`, `README.md`, `charts/nats-operator/README.md`, `docs/README.md` or `SECURITY.md`.

The rules come from the [Google developer documentation style guide](https://developers.google.com/style), the [Kubernetes documentation style guide](https://kubernetes.io/docs/contribute/style/style-guide/), the [Microsoft Writing Style Guide](https://learn.microsoft.com/en-us/style-guide/welcome/) and [Diátaxis](https://diataxis.fr/).

Use the terms in [`CONTEXT.md`](../CONTEXT.md). Write "controller", "NATS operator", "NATS cluster" and "Kubernetes cluster". Never write "operator" or "cluster" unqualified.

## Voice

- Address the reader as "you".
- Write an instruction as an imperative. "Apply the manifest."
- Use the present tense for behaviour.
- Use the active voice and name the actor. "The cluster controller creates one StatefulSet per server."
- Leave out marketing. Write no slogan, no tagline, and no adjective that a test could not check.

## Sentences

- Put one idea in a sentence. Aim for fewer than 25 words, and never exceed 35.
- Do not join two clauses with a semicolon.
- Use a colon only before a list, a block of code or an example.
- Give a reason only where the reader would otherwise do the wrong thing.
- Say what a thing is or does. Use a negative only where the reader would assume the opposite.
- Use plain verbs. A condition is True or False. An object reports a status. Avoid "reads", "carries", "holds", "names" and "admits" with an object as the subject.
- Let the material set the length of a sentence and of a paragraph. A short sentence before a dense one helps.
- Do not write "simply", "just", "easy" or "note that".
- Do not close a section with a summary.
- Do not use an em dash.

## Page types

### Story

A story is a tutorial.

1. Open with what you will have at the end, in the second person.
2. List what must exist first, under the heading "Before you begin".
3. Write each step as a numbered heading that is an imperative, such as "1. Deploy the NATS cluster".
4. Under the heading, give the instruction, then the manifest or the command, then the result to expect.
5. Link background instead of explaining it on the page.

The `manifest` shortcode prints a file's name, its YAML and the `kubectl` command that applies or deletes it. Its output cannot sit inside a Markdown list item, which is why a step is a heading.

A comment inside a manifest is copy, and these rules apply to it.

### Install page

The install page is a how-to guide with reference tables. Write each procedure as numbered steps. Keep each table cell to the fact.

### Reference page

A reference page states facts in complete, short sentences. It does not persuade.

### Front page

The front page gives four things:

- what the project is, in one sentence
- who it is for
- the fastest path, which is the quickstart, the install page and the reference
- what each area of the documentation contains

## Formatting

- Write headings in sentence case. A heading says what its section contains.
- Use H2, then H3, in order.
- Use code font for anything the reader types and for any name in the API.
- Put a placeholder in angle brackets, such as `<version>`, and explain it once.

## What a copy edit must not change

- A fact, a field, a value, a default, a command or a condition reason.
- The YAML of a story's manifest.
- A story's front matter under `params.e2e`, its `manifest` shortcodes, or the names of its files. The end-to-end harness reads them.
- A generated page under `docs/content/docs/reference`. Edit its source, then run `just verify`. The sources are `hack/telemetrydocs`, `hack/permdocs`, `hack/api-docs` and the doc comments under `api`.

Every sentence must be true of the tree at the commit that ships it. If a rewrite needs a fact that the page did not state, check the fact in the code or leave the sentence out.

If you rename a heading, find the links to its anchor and update them.

## Checks

Run these from the repository root before you open a pull request:

```sh
just site-check
go test ./hack/...
just verify
```
