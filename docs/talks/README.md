# Piddiplatsch overview slides

Edit [`overview.qmd`](overview.qmd) directly. It is the canonical slide source;
there is no Markdown conversion step. A title slide and nine overview slides are
followed by five operational appendix slides. After the title, “What is Piddi?”
introduces the photo and name, followed by “Piddi and ESGF-NG”. Speaker notes identify the
repository documentation behind the content.
The name explanation displays Pittiplatsch's Wikipedia image from a local asset,
with links to Wikipedia and Wikimedia Commons and the photographer and license.
The photo is embedded in the generated HTML for offline viewing; its source and
license are documented in [`assets/README.md`](assets/README.md).

## Requirements

Quarto 1.6–1.x and Chrome/Chromium are required for HTML. PDF export adds
DeckTape 3.16.1 and Node.js 22. Install the base tools into an isolated
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
not change the Piddi environment. The base environment includes Node/npm for
the optional PDF exporter; HTML builds do not require DeckTape. No Python
packages or TeX setup is needed. Code examples are displayed only; rendering
never runs `piddi` or contacts Kafka or Handle services.

To install the PDF exporter and its Puppeteer browser, as in Woodpecker:

```sh
make -C docs/talks install-pdf
```

This updates only `docs/talks/.conda` and installs DeckTape there. Its browser
is downloaded into `docs/talks/.cache/puppeteer`, also ignored by Git. Quarto
can reuse that browser for Mermaid. Installation requires network access; builds
use installed tools. An existing environment with Quarto, Node, DeckTape, and
its browser can also build both formats without installing local tools.

The overview emphasizes the built-in project plugins and the
`harvest → map + validation → publish` workflow with JSONL between stages.
The ESGF-NG Mermaid diagram is a simplified logical architecture, with sources
and the scope of mapping validation documented in the slide notes.
The outlook slide illustrates a proposed Rook plugin maintaining a local
PostgreSQL STAC lookup database for the Rook/WPS broker. It distinguishes this
future extension from the existing PID workflow.
A summary closes the overview before the operational appendix, which covers
configuration, consumer groups, output, retry, and service deployment with Ansible.

## Build and present

From the repository root:

```sh
make -C docs/talks slides-html  # standalone HTML only
make -C docs/talks slides-pdf   # rebuild HTML, then export PDF
make -C docs/talks slides       # both HTML and PDF
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

The same targets work after `cd docs/talks`. PDF export uses DeckTape to capture
the Reveal.js HTML at 1600 × 900, with one page per slide, including the appendix.
Share `docs/talks/_build/overview.pdf`; the photo and Mermaid diagrams are
included. The HTML remains available for interactive presenting and speaker
notes. There is no PowerPoint target. `theme.scss` follows Woodpecker's
white background, blue headings, and Arial typography.

## Files and cleanup

- `overview.qmd`: slide content and speaker notes.
- `_quarto.yml`: HTML format, explicit render list, and output location.
- `theme.scss`: presentation styling.
- `environment.yml`, `quarto.sh`, `decktape.sh`, `Makefile`: independent build tooling.
- `_build/`: generated HTML and PDF; `.quarto/`, `.cache/`, `.conda/`: caches and
  tools. All are ignored by Git.

```sh
make -C docs/talks slides-clean
```

Cleanup removes only this folder's `_build/`, `.quarto/`, and generated
`overview_files/`; sources and the
isolated environment and downloaded browser remain. The root Makefile, application requirements,
root README, and deployment configuration are unchanged.

After edits, rebuild and inspect every HTML slide and PDF page for wrapping and
clipping. Keep
CLI examples and project support aligned with the repository's
[architecture](../architecture.md), [configuration](../configuration.md), and
[operations](../operations.md) documentation.
