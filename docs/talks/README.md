# Piddiplatsch overview slides

Edit [`overview.qmd`](overview.qmd) directly. It is the canonical slide source;
there is no Markdown conversion step. The title and ten overview slides are
followed by three operational appendix slides. Speaker notes identify the
repository documentation behind the content.
The name explanation displays Pittiplatsch's Wikipedia image from a local asset,
with links to Wikipedia and Wikimedia Commons and the photographer and license.
The photo is embedded in the generated HTML for offline viewing; its source and
license are documented in [`assets/README.md`](assets/README.md).

## Requirements

Quarto 1.6–1.x and Chrome/Chromium are required. Install Quarto into an isolated
environment using the requirements in [`environment.yml`](environment.yml):

```sh
make -C docs/talks install
```

This requires Conda and network access, and creates or updates only
`docs/talks/.conda`. It does not install Piddiplatsch or change the active
application environment. Alternatively, use an existing Quarto installation on
`PATH`. The wrapper prefers the local talks environment when present and supplies
the split tool paths required by Conda's Quarto packages.

Mermaid diagrams are authored directly in `overview.qmd` and prerendered to
embedded PNG images for offline HTML viewing. Quarto needs Chrome/Chromium for
this step. The wrapper reuses Google Chrome on macOS; on other systems set
`QUARTO_CHROMIUM` to an installed browser executable, or explicitly install one:

```sh
make -C docs/talks install-browser
```

That optional installation downloads Chromium to Quarto's tool location; it does
not change the Piddi environment. No Python packages, Node/npm installation,
DeckTape, or TeX setup is needed. Code examples are displayed only; rendering
never runs `piddi` or contacts Kafka or Handle services.

The overview emphasizes the built-in project plugins and the
`harvest → map + validation → publish` workflow with JSONL between stages.
The ESGF-NG Mermaid diagram is a simplified logical architecture, with sources
and the scope of mapping validation documented in the slide notes.
The outlook slide illustrates a proposed Rook plugin maintaining a local
PostgreSQL STAC lookup database for the Rook/WPS broker. It distinguishes this
future extension from the existing PID workflow.
A summary closes the overview before the operational appendix.

## Build and present

From the repository root:

```sh
make -C docs/talks slides-html
# Equivalent: make -C docs/talks slides
```

Open `docs/talks/_build/overview.html` in a browser. The configured
[Quarto Reveal.js format](https://quarto.org/docs/presentations/revealjs/)
embeds scripts and styles in the HTML so the deck can be shared as one file.
External documentation links require network access. Use the arrow keys to
navigate, Escape for the slide overview, and S for speaker view (browser support
and restrictions on local files may affect speaker view).

For automatic reload while editing:

```sh
make -C docs/talks preview
```

The same targets work after `cd docs/talks`. Only Reveal.js HTML is configured;
there are no PDF or PowerPoint build targets. `theme.scss` follows Woodpecker's
white background, blue headings, and Arial typography.

## Files and cleanup

- `overview.qmd`: slide content and speaker notes.
- `_quarto.yml`: HTML format, explicit render list, and output location.
- `theme.scss`: presentation styling.
- `environment.yml`, `quarto.sh`, `Makefile`: independent build tooling.
- `_build/`, `.quarto/`, `.conda/`: generated output, cache, and tools, all ignored.

```sh
make -C docs/talks slides-clean
```

Cleanup removes only this folder's `_build/`, `.quarto/`, and generated
`overview_files/`; sources and the
isolated environment remain. The root Makefile, application requirements,
root README, and deployment configuration are unchanged.

After edits, rebuild and inspect every slide for wrapping and clipping. Keep
CLI examples and project support aligned with the repository's
[architecture](../architecture.md), [configuration](../configuration.md), and
[operations](../operations.md) documentation.
