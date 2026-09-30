#!/bin/sh
# Use Quarto from PATH (including CI), or fall back to the piddi environment.
set -eu
talks_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$talks_dir"

if ! command -v quarto >/dev/null 2>&1; then
    if [ "${PIDDI_SLIDES_IN_CONDA:-}" != 1 ] && command -v conda >/dev/null 2>&1; then
        exec conda run --no-capture-output --name "${PIDDI_SLIDES_ENV:-piddi}" \
            env PIDDI_SLIDES_IN_CONDA=1 sh "$talks_dir/quarto.sh" "$@"
    fi
    echo "Slides: Quarto is missing. Run 'make -C talks install' or activate the piddi environment." >&2
    exit 1
fi
quarto_prefix="${CONDA_PREFIX:-}"

# Conda packages Quarto's tools separately. Set their paths even before activation.
if [ -n "$quarto_prefix" ] && [ "$(command -v quarto)" = "$quarto_prefix/bin/quarto" ]; then
    export QUARTO_DENO="$quarto_prefix/bin/deno"
    export QUARTO_PANDOC="$quarto_prefix/bin/pandoc"
    export QUARTO_ESBUILD="$quarto_prefix/bin/esbuild"
    export QUARTO_DART_SASS="$quarto_prefix/bin/sass"
    export QUARTO_SHARE_PATH="$quarto_prefix/share/quarto"
    export QUARTO_CONDA_PREFIX="$quarto_prefix"
    if [ "$(uname)" = Darwin ]; then
        export QUARTO_DENO_DOM="$quarto_prefix/lib/deno_dom.dylib"
    else
        export QUARTO_DENO_DOM="$quarto_prefix/lib/deno_dom.so"
    fi
fi

# Prefer the PDF exporter's headless browser for Mermaid, as in Woodpecker.
if [ -z "${QUARTO_CHROMIUM:-}" ] && [ -n "$quarto_prefix" ] && [ -x "$quarto_prefix/bin/decktape" ]; then
    quarto_browser=$(PUPPETEER_CACHE_DIR="${PUPPETEER_CACHE_DIR:-$quarto_prefix/.cache/puppeteer}" \
        "$quarto_prefix/bin/node" -e '
          const {createRequire} = require("node:module");
          const load = createRequire(process.argv[1]);
          console.log(load("puppeteer").executablePath({headless: "shell"}));
        ' "$quarto_prefix/lib/node_modules/decktape/package.json")
    if [ -x "$quarto_browser" ]; then
        export QUARTO_CHROMIUM="$quarto_browser"
    fi
fi

# Reuse Chrome on macOS for Mermaid prerendering; other platforms can set
# QUARTO_CHROMIUM or use Quarto's explicitly installed Chromium.
if [ -z "${QUARTO_CHROMIUM:-}" ] && [ -x "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" ]; then
    export QUARTO_CHROMIUM="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
fi

exec quarto "$@"
