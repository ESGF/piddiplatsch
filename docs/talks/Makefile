# Independent of the application and deployment Makefile.
# Run from this folder or use make -C docs/talks.
.DEFAULT_GOAL := help
.PHONY: help install install-browser install-pdf slides slides-html slides-pdf preview slides-clean

help:
	@echo "Piddiplatsch overview presentation targets:"
	@echo "  install       Create/update the isolated docs/talks/.conda environment"
	@echo "  install-browser Install Quarto Chromium for Mermaid if no browser exists"
	@echo "  install-pdf   Install local slide tools, DeckTape and its browser"
	@echo "  slides-html   Build standalone Reveal.js HTML in _build/overview.html"
	@echo "  slides-pdf    Build HTML, then export _build/overview.pdf"
	@echo "  slides        Build both HTML and PDF"
	@echo "  preview       Preview with automatic reload"
	@echo "  slides-clean  Remove only generated presentation files and cache"

install:
	@command -v conda >/dev/null 2>&1 || { echo "Slides: install Conda and add it to PATH first." >&2; exit 1; }
	@if test -d .conda/conda-meta; then \
		conda env update --prefix "$(CURDIR)/.conda" --file environment.yml; \
	else \
		conda env create --prefix "$(CURDIR)/.conda" --file environment.yml; \
	fi
	@conda run --prefix "$(CURDIR)/.conda" sh quarto.sh --version

install-pdf: install
	conda run --prefix "$(CURDIR)/.conda" env PUPPETEER_CACHE_DIR="$(CURDIR)/.cache/puppeteer" npm install --global --prefix "$(CURDIR)/.conda" decktape@3.16.1
	sh decktape.sh version

slides: slides-pdf

install-browser:
	sh quarto.sh install chromium

slides-html:
	sh quarto.sh render --to revealjs

slides-pdf: slides-html
	sh decktape.sh reveal --size 1600x900 --load-pause 2000 --pause 1000 _build/overview.html _build/overview.pdf

preview:
	sh quarto.sh preview --to revealjs

slides-clean:
	rm -rf -- _build .quarto overview_files
