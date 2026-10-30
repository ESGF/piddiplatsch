# Building the documentation

MkDocs builds the guides in `docs/`. Quarto builds the slides in
`talks/overview.qmd`. GitHub Pages publishes both:

- [Documentation](https://esgf.github.io/piddiplatsch/)
- [Overview slides](https://esgf.github.io/piddiplatsch/talks/overview.html)

The [Quick guide](quick-guide.md) uses the root README through a symlink.
Edit `README.md` to update both copies.

## Preview the guides

From the repository root, install the documentation tools:

```sh
python3 -m venv .venv-docs
. .venv-docs/bin/activate
python -m pip install -e ".[docs]"
make docs-html
```

`make develop` also installs these tools. `make docs-html` runs a strict MkDocs
build into `site/`. It builds the guides without the slide deck.

For live reload while editing:

```sh
python -m mkdocs serve
```

Open the URL printed by MkDocs. Builds do not contact Kafka or Handle services.

## Build the guides and slides

Install the slide tools in the existing `piddi` Conda environment:

```sh
conda activate piddi
make -C talks install
```

This installs Quarto, Node/npm, DeckTape, and its browser. See the
[slide guide](https://github.com/ESGF/piddiplatsch/blob/main/talks/README.md)
for requirements and options.

With MkDocs available in your active environment, run:

```sh
make docs
make docs-check
make docs-serve
```

`make docs` builds the guides, renders the HTML deck, and copies it to
`site/talks/overview.html`. `make docs-check` checks required pages and local
links. `make docs-serve` rebuilds and serves the full site at
[localhost:8000](http://localhost:8000/). Run it again after edits.
`make pages` is another name for `make docs`.

The HTML deck embeds its images, diagrams, scripts, and styles. To export a
local PDF, run `make -C talks slides-pdf`. GitHub Pages publishes the HTML only.

Running `make docs-html` or `mkdocs serve` replaces the full site with the
guides-only build. Run `make docs` again to include the deck.

## Publish with GitHub Pages

In repository **Settings → Pages → Build and deployment**, select
**GitHub Actions**.

The `Documentation and slides` workflow builds pull requests and pushes to
`main`. It checks the combined site before uploading. Successful builds on
`main` deploy through the `github-pages` environment. Pull requests and manual
runs on other branches do not deploy.

You can also run the workflow manually on `main`. It uses `GITHUB_TOKEN`;
no personal access token or `gh-pages` branch is needed. Environment protection
rules must allow deployments from `main`.

CI installs the slide tools and checks that the headless browser starts.
Slide rendering has a two-minute timeout and one retry after a timeout.
Other render errors fail immediately.
