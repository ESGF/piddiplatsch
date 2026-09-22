#!/bin/sh
# Prefer the isolated talks tools; an existing DeckTape on PATH also works.
set -eu
talks_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$talks_dir"

if [ -x "$talks_dir/.conda/bin/decktape" ]; then
    PATH="$talks_dir/.conda/bin:$PATH"
    export PATH
    export PUPPETEER_CACHE_DIR="${PUPPETEER_CACHE_DIR:-$talks_dir/.cache/puppeteer}"
elif ! command -v decktape >/dev/null 2>&1; then
    echo "Slides: DeckTape is missing. Run 'make -C docs/talks install-pdf' or activate an environment with DeckTape." >&2
    exit 1
fi

exec decktape "$@"
