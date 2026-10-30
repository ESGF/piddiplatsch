# Overview slides

Edit `overview.qmd` for slide content and speaker notes. The deck introduces
Piddi and ESGF-NG, explains the PID workflow, and includes an operations appendix.

The HTML embeds images, diagrams, scripts, and styles for offline viewing.
External links still need network access. Image attribution is in
[assets/README.md](assets/README.md).

## Install tools

Use the existing `piddi` Conda environment:

```sh
conda activate piddi
make -C talks install
```

The installer adds Quarto and Node/npm from `environment.yml`, then DeckTape
and its Puppeteer browser. It needs network access. Tools are installed in the
active Conda environment; the browser cache is under
`$CONDA_PREFIX/.cache/puppeteer`.

Requirements are Quarto 1.6–1.x and Chrome/Chromium for HTML, plus Node.js 22
and DeckTape 3.16.1 for PDF. No TeX installation is needed.

For another environment name, activate it and run
`make -C talks install CONDA_ENV=my-piddi`. Use the same override when building
without activation. The `piddi` environment must already exist for the default.

`install-env` adds only Conda packages. `install-pdf` and `install-browser` run
the full installation. A direct `conda env update` does not install DeckTape
or its browser.

## Build and present

From the repository root:

```sh
make -C talks slides-html
make -C talks slides-pdf
make -C talks slides
make -C talks preview
```

`slides-html` builds `talks/_build/overview.html`. `slides-pdf` rebuilds the HTML
and exports `talks/_build/overview.pdf`. `slides` builds both. `preview` reloads
the presentation while you edit. The same targets work from the `talks/` directory.

Open the HTML in a browser. Use arrow keys to move, Escape for the overview,
and S for speaker view. Speaker view depends on browser support and local-file
restrictions. PDF export uses one 1600 × 900 page per slide, including the appendix.
There is no PowerPoint export.

After edits, inspect all affected slides for wrapping and clipping. Keep CLI
examples aligned with the [configuration](../docs/configuration.md) and
[operations](../docs/operations.md) guides.

## Build behavior

The wrappers use tools on `PATH`. If a tool is missing, they try
`conda run -n piddi`. They also set Conda's Quarto paths and browser cache location.

Quarto renders Mermaid diagrams to embedded PNGs. It prefers the installed
Puppeteer browser, with Chrome on macOS as a fallback. Set `QUARTO_CHROMIUM`
to choose another browser. Rendering does not run the CLI examples or contact
Kafka or Handle services.

## Publish with the documentation

The root `make docs` target combines MkDocs and the HTML deck. The published
[overview](https://esgf.github.io/piddiplatsch/talks/overview.html) is part of
GitHub Pages. See [Building the documentation](../docs/building.md) for preview
and publishing steps. PDF export is local only.

## Files and cleanup

| File | Purpose |
| --- | --- |
| `overview.qmd` | Content and speaker notes. |
| `_quarto.yml` | Render settings and output paths. |
| `theme.scss` | Slide styles. |
| `environment.yml` | Conda tools. |
| `quarto.sh`, `decktape.sh`, `Makefile` | Build commands. |
| `_build/` | Generated HTML and PDF. |

Run `make -C talks slides-clean` to remove generated output and render caches.
It keeps sources, the Conda environment, and the installed browser.
