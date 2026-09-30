#!/bin/sh
# Use DeckTape from PATH, or fall back to the piddi Conda environment.
set -eu
talks_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$talks_dir"

if ! command -v decktape >/dev/null 2>&1; then
    if [ "${PIDDI_SLIDES_IN_CONDA:-}" != 1 ] && command -v conda >/dev/null 2>&1; then
        exec conda run --no-capture-output --name "${PIDDI_SLIDES_ENV:-piddi}" \
            env PIDDI_SLIDES_IN_CONDA=1 sh "$talks_dir/decktape.sh" "$@"
    fi
    echo "Slides: DeckTape is missing. Run 'make -C talks install' or activate the piddi environment." >&2
    exit 1
fi
if [ -n "${CONDA_PREFIX:-}" ] && [ "$(command -v decktape)" = "$CONDA_PREFIX/bin/decktape" ]; then
    export PUPPETEER_CACHE_DIR="${PUPPETEER_CACHE_DIR:-$CONDA_PREFIX/.cache/puppeteer}"
fi

exec decktape "$@"
