"""Render and validate a candidate before Ansible installs it."""

import sys
from copy import deepcopy
from pathlib import Path

import toml

PRODUCTION_DEFAULTS = {
    "consumer": {"output_dir": "/var/lib/piddi"},
    "logging": {"file": "/var/log/piddi/piddi.log"},
    "stats": {"db_path": "/var/lib/piddi/piddi.db"},
}


def render_config(source: Path) -> str:
    # Opening explicitly makes a missing source an error, never a default-only
    # deployment. Use the application's merge semantics for nested settings.
    from piddiplatsch.config.config import Config

    with source.open() as stream:
        overrides = toml.load(stream)
    merged = deepcopy(PRODUCTION_DEFAULTS)
    configured = Config()
    configured._merge_dicts(merged, overrides)
    # Validate only packaged defaults plus the candidate. Loading the installed
    # site file here could conceal omissions or retain settings being removed.
    configured._merge_dicts(configured.config_data, merged)
    errors, _ = configured.validate()
    if errors:
        raise ValueError("Invalid candidate configuration:\n" + "\n".join(errors))
    return toml.dumps(merged)


if __name__ == "__main__":
    print(render_config(Path(sys.argv[1])), end="")
