#!/bin/sh
# Prefer the isolated talks environment; otherwise use Quarto from PATH.
set -eu
talks_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$talks_dir"

if [ -x "$talks_dir/.conda/bin/quarto" ]; then
    quarto_prefix="$talks_dir/.conda"
    PATH="$quarto_prefix/bin:$PATH"
    export PATH
elif ! command -v quarto >/dev/null 2>&1; then
    echo "Slides: Quarto is missing. Run 'make -C docs/talks install' or add Quarto to PATH." >&2
    exit 1
else
    quarto_prefix="${CONDA_PREFIX:-}"
fi

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

exec quarto "$@"
