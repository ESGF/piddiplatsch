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
DeckTape 3.16.1 and Node.js 22. Install all slide tools into the existing `piddi`
Conda environment:

```sh
conda activate piddi
make -C talks install
```

The installer requires the selected Conda environment to be active and network
access. Like Woodpecker, it installs directly into the active prefix. It adds Quarto and Node/npm
from [`environment.yml`](environment.yml) to `piddi` without pruning its
application dependencies. It then installs DeckTape via npm into that same
Conda prefix and downloads Puppeteer's browser into
`$CONDA_PREFIX/.cache/puppeteer`. It does not create a separate `talks/.conda`.
The `piddi` environment must already exist.

For a differently named application environment, use
activate it and use `make -C talks install CONDA_ENV=my-piddi`. Use the same `CONDA_ENV` override
when building without activation, or activate that environment first.

`install-env` adds only the Conda packages. `install-pdf` and `install-browser`
run the complete installation, including the shared PDF/Mermaid browser.
Running `conda env update` directly does not install DeckTape or its browser.
No TeX setup is needed.

The wrappers use tools on `PATH`, including an activated Conda environment or
the standalone Quarto installation in CI. If the tool is missing from `PATH`,
they run it through `conda run -n piddi`. They supply Conda's split Quarto tool
paths and the browser cache location automatically.

Mermaid diagrams are prerendered to embedded PNG images for offline viewing.
Quarto prefers the installed Puppeteer browser, with Chrome on macOS as a fallback; set
`QUARTO_CHROMIUM` to select another browser. Code examples are displayed only;
rendering never runs `piddi` or contacts Kafka or Handle services.

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

The repository's `make docs` target combines the MkDocs documentation and this
HTML deck for GitHub Pages. See [Building the documentation](../docs/building.md)
for local preview and workflow setup. The published deck lives at
<https://esgf.github.io/piddiplatsch/talks/overview.html>.

From the repository root:

```sh
make -C talks slides-html  # standalone HTML only
make -C talks slides-pdf   # rebuild HTML, then export PDF
make -C talks slides       # both HTML and PDF
```

Open `talks/_build/overview.html` in a browser. The configured
[Quarto Reveal.js format](https://quarto.org/docs/presentations/revealjs/)
embeds scripts and styles in the HTML so the deck can be shared as one file.
External documentation links require network access. Use the arrow keys to
navigate, Escape for the slide overview, and S for speaker view (browser support
and restrictions on local files may affect speaker view).

For automatic reload while editing:

```sh
make -C talks preview
```

The same targets work after `cd talks`. PDF export uses DeckTape to capture
the Reveal.js HTML at 1600 × 900, with one page per slide, including the appendix.
Share `talks/_build/overview.pdf`; the photo and Mermaid diagrams are
included. The HTML remains available for interactive presenting and speaker
notes. There is no PowerPoint target. `theme.scss` follows Woodpecker's
white background, blue headings, and Arial typography.

## Files and cleanup

- `overview.qmd`: slide content and speaker notes.
- `_quarto.yml`: HTML format, explicit render list, and output location.
- `theme.scss`: presentation styling.
- `environment.yml`, `quarto.sh`, `decktape.sh`, `Makefile`: independent build tooling.
- `_build/`: generated HTML and PDF; `.quarto/`: render cache. Both are ignored
  by Git. Installed tools and the browser live in the `piddi` Conda environment.

```sh
make -C talks slides-clean
```

Cleanup removes only this folder's `_build/`, `.quarto/`, and generated
`overview_files/`; sources and the
`piddi` environment and downloaded browser remain.

After edits, rebuild and inspect every HTML slide and PDF page for wrapping and
clipping. Keep
CLI examples and project support aligned with the repository's
[architecture](../docs/architecture.md), [configuration](../docs/configuration.md), and
[operations](../docs/operations.md) documentation.
