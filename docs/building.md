# Building the documentation

MkDocs builds the guides. `docs/quick-guide.md` links to the root README; edit
`README.md` to update both.

## Preview guides

From the repository root, in your development environment:

```console
python -m pip install -e ".[docs]"
make docs-html
python -m mkdocs serve
```

Open the URL printed by MkDocs. `make docs-html` runs a strict build into `site/`.

## Include slides

Install Quarto and the slide tools using the
[slide guide](https://github.com/ESGF/piddiplatsch/blob/main/talks/README.md), then run:

```console
make docs
make docs-check
make docs-serve
```

This builds guides and HTML slides, checks local links, and serves the combined
site. `make docs-html` replaces it with guides only; run `make docs` to include
slides again.

GitHub Actions builds and checks the combined site. Successful builds on `main`
publish to GitHub Pages. In repository **Settings → Pages**, choose
**GitHub Actions** as the deployment source.
