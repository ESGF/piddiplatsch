# Building the documentation

MkDocs builds the documentation from `docs/`. Quarto builds the overview slides
separately from `docs/talks/overview.qmd`. Both are published as one GitHub Pages
site:

- Documentation: <https://esgf.github.io/piddiplatsch/>
- Slides: <https://esgf.github.io/piddiplatsch/talks/overview.html>

## Local documentation

From the repository root, create an isolated Python environment and install the
documentation requirements:

```sh
python3 -m venv .venv-docs
. .venv-docs/bin/activate
python -m pip install -r requirements-docs.txt
make docs
make docs-serve
```

`make docs` runs a strict MkDocs build into `site/`. `make docs-serve` starts
MkDocs' live preview; use the URL printed in the terminal. Neither command needs
Piddiplatsch, Kafka, or Handle services. The slides link is available in the
combined build only.

## Combined site and slides

Install Quarto and Chrome/Chromium using the
[slide tooling instructions](https://github.com/ESGF/piddiplatsch/blob/main/docs/talks/README.md).
With the documentation environment still active, run:

```sh
make pages
python -m http.server 8000 --directory site
```

Open <http://localhost:8000/>. `make pages` builds MkDocs first, renders the
standalone Reveal.js HTML, and copies it to `site/talks/overview.html`. Quarto
embeds the images, diagrams, scripts, and styles. Source files, tool environments,
and caches are excluded from the site. Running `make docs` again cleans `site/`,
so run `make pages` to restore the combined output.

PDF export remains available locally via `make -C docs/talks slides-pdf`; the
Pages workflow publishes the HTML deck only.

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
