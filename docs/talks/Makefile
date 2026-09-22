# Independent of the application and deployment Makefile.
# Run from this folder or use make -C docs/talks.
.DEFAULT_GOAL := help
.PHONY: help install slides slides-html preview slides-clean

help:
	@echo "Piddiplatsch overview presentation targets:"
	@echo "  install       Create/update the isolated docs/talks/.conda environment"
	@echo "  slides-html   Build standalone Reveal.js HTML in _build/overview.html"
	@echo "  slides        Alias for slides-html (HTML only)"
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

slides: slides-html

slides-html:
	sh quarto.sh render --to revealjs

preview:
	sh quarto.sh preview --to revealjs

slides-clean:
	rm -rf -- _build .quarto
