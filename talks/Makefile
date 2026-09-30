# Independent of the application and deployment Makefile.
# Run from this folder or use make -C talks.
CONDA_ENV ?= piddi
export PIDDI_SLIDES_ENV := $(CONDA_ENV)

.DEFAULT_GOAL := help
.PHONY: help install install-env install-browser install-pdf slides slides-html slides-pdf preview slides-clean

help:
	@echo "Piddiplatsch overview presentation targets:"
	@echo "  install       Install all HTML/PDF tools in Conda environment $(CONDA_ENV)"
	@echo "  install-env   Add Quarto and Node/npm to Conda environment $(CONDA_ENV)"
	@echo "  install-browser Install the shared PDF/Mermaid browser in $(CONDA_ENV)"
	@echo "  install-pdf   Install slide tools, DeckTape and its browser in $(CONDA_ENV)"
	@echo "  slides-html   Build standalone Reveal.js HTML in _build/overview.html"
	@echo "  slides-pdf    Build HTML, then export _build/overview.pdf"
	@echo "  slides        Build both HTML and PDF"
	@echo "  preview       Preview with automatic reload"
	@echo "  slides-clean  Remove only generated presentation files and cache"

install: install-pdf

install-env:
	@command -v conda >/dev/null 2>&1 || { echo "Slides: install Conda and add it to PATH first." >&2; exit 1; }
	@test "$$CONDA_DEFAULT_ENV" = "$(CONDA_ENV)" && test -n "$$CONDA_PREFIX" || { echo "Activate the $(CONDA_ENV) Conda environment first." >&2; exit 1; }
	conda env update --name "$(CONDA_ENV)" --file environment.yml
	@PATH="$$CONDA_PREFIX/bin:$$PATH" sh quarto.sh --version

install-pdf: install-env
	PATH="$$CONDA_PREFIX/bin:$$PATH" PUPPETEER_CACHE_DIR="$$CONDA_PREFIX/.cache/puppeteer" "$$CONDA_PREFIX/bin/npm" install --global --prefix "$$CONDA_PREFIX" decktape@3.16.1
	PATH="$$CONDA_PREFIX/bin:$$PATH" sh decktape.sh version

slides: slides-pdf

install-browser: install-pdf

slides-html:
	sh quarto.sh render --to revealjs

slides-pdf: slides-html
	sh decktape.sh reveal --size 1600x900 --load-pause 2000 --pause 1000 _build/overview.html _build/overview.pdf

preview:
	sh quarto.sh preview overview.qmd --to revealjs

slides-clean:
	rm -rf -- _build .quarto overview_files
