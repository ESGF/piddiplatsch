# Building the documentation

MkDocs builds the documentation from `docs/`. Quarto builds the overview slides
separately from `talks/overview.qmd`. Both are published as one GitHub Pages
site:

- Documentation: <https://esgf.github.io/piddiplatsch/>
- Slides: <https://esgf.github.io/piddiplatsch/talks/overview.html>

The top-level `talks/` directory owns the slide sources, assets, Quarto settings,
and its own Makefile. Build slides independently with `make -C talks slides-html`.
The root Makefile delegates to that build and combines its output with MkDocs;
`docs/talks.md` is the normal documentation page linking to the published deck.

## Local documentation

From the repository root, create an isolated Python environment and install the
optional documentation dependencies from `pyproject.toml`, as in Woodpecker:

```sh
python3 -m venv .venv-docs
. .venv-docs/bin/activate
python -m pip install -e ".[docs]"
make docs-html
```

The `docs` extra adds MkDocs to the normal package installation. Quarto and
Chrome/Chromium are separate, non-Python tools; they are installed as described
below and explicitly by the Pages workflow. No Kafka or Handle services are
contacted during builds.

`make docs-html` runs a strict MkDocs-only build into `site/`. For live reload
while editing Markdown, run `python -m mkdocs serve` and use its printed URL.
The Talks page is included, but its HTML deck is available only in the complete
build.

## Combined site and slides

Install Quarto and Chrome/Chromium using the
[slide tooling instructions](https://github.com/ESGF/piddiplatsch/blob/main/talks/README.md).
With the documentation environment still active, run:

```sh
make docs
make docs-check
make docs-serve
```

Open <http://localhost:8000/> and select **Talks**. `make docs` builds MkDocs first, renders the
standalone Reveal.js HTML, and copies it to `site/talks/overview.html`. Quarto
embeds the images, diagrams, scripts, and styles. Source files, tool environments,
and caches are excluded from the site. `make docs-serve` rebuilds and serves the
complete site; rerun it after edits. `make pages` remains an alias for `make docs`.
`make docs-check` checks the required pages and relative links/assets in the
assembled site, including the HTML slide link. CI runs this before uploading.
Running `make docs-html` or `mkdocs serve` builds only the Markdown documentation;
use `make docs` to restore the complete output in `site/`.

After `conda activate piddi`, `make -C talks install` adds all slide tools to the existing `piddi`
Conda environment: Quarto and Node/npm, then DeckTape and its Puppeteer browser. Export locally with
`make -C talks slides-pdf`; the Pages workflow publishes the HTML deck only.
Like Woodpecker, CI installs Node 22 and DeckTape 3.16.1 and uses its
`chrome-headless-shell` for Quarto's Mermaid rendering. It checks the browser
executable before building; the build step has a five-minute timeout so a
rendering stall is reported separately from dependency installation.

## GitHub Pages

In the repository's **Settings → Pages → Build and deployment**, select
**GitHub Actions** as the source. The `Documentation and slides` workflow in
`.github/workflows/pages.yml` builds both outputs on pull requests and pushes to
`main`. A successful build on `main` uploads one Pages artifact and deploys it
using the `github-pages` environment. You can also run the workflow manually on
`main`. Pull requests and manual runs on other branches never deploy.

The workflow uses the repository's `GITHUB_TOKEN`; no personal access token or
`gh-pages` branch is needed. Any environment protection rules for `github-pages`
must allow deployments from `main`.
