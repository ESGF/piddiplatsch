# Piddiplatsch

[Documentation](https://esgf.github.io/piddiplatsch/) ·
[Overview slides](https://esgf.github.io/piddiplatsch/talks/overview.html)

[![Build Status](https://github.com/ESGF/piddiplatsch/actions/workflows/ci.yml/badge.svg)](https://github.com/ESGF/piddiplatsch/actions)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/ESGF/piddiplatsch/blob/main/LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

**Piddiplatsch** processes ESGF STAC publication records from Kafka and registers
persistent identifiers (PIDs) with the Handle System. It supports **CMIP6,
CMIP6Plus, CMIP7, and CORDEX-CMIP6**, with project-specific routing and mapping.

*Curious by nature. Persistent by design.*

## Quick start

Install with Conda and the development tools:

```bash
git clone https://github.com/ESGF/piddiplatsch.git
cd piddiplatsch
conda env create
conda activate piddi
make develop
```

Create your local configuration and replace the connection and credential
placeholders before running:

```bash
cp etc/esgf-example.toml custom.toml
# Edit custom.toml for your site; keep credentials out of Git.
piddi config validate
piddi --help
```

Start filling the local catalogs from Kafka:

```bash
piddi harvest
```

`harvest` saves raw messages and maintains project catalogs. Run `map` to prepare
Handle JSONL, or schedule `batch` for mapping and publication. Kafka access is
required; see [Configuration](https://esgf.github.io/piddiplatsch/configuration/).

## Run in stages

You can also harvest a small sample, then map it separately:

```bash
piddi harvest --limit 100
piddi map --project cmip6 --date all
# Optional predecessor links from the local catalog
piddi map --project cmip6 --version-history
```

Harvest also maintains per-project SQLite STAC catalogs under `outputs/catalog/`
using a separate Kafka group (`<group.id>.harvest`). Missing PATCH bases recover
the current item from remote STAC without reapplying the patch; recovered items
stay in remote refresh mode for subsequent events and harvest restarts. Use `--no-catalog` for the
previous raw-only behavior. Map reads complete local catalog items, including
harvested patches, without fetching STAC items or replaying raw dumps.
Use `harvest --clean-catalog` to clear and rebuild all registered project catalogs,
or repeat `--clean-project NAME` to rebuild selected projects from retained Kafka
history. Cleaning uses a fresh harvest group and compacts the cleared databases.

Map and publish share `--date` (`today-1`, `last`, `all`, or a calendar date),
inclusive `--from`/`--to` ranges, and `--offset`/`--limit`. Both default to `last`;
use `--date all` for full initial filling. Map writes daily Handle files for the
selected catalog event days.

Once your Handle service profile is configured, publish a completed daily file:

```bash
piddi publish --project cmip6 --date yesterday

# Or publish an inclusive range of available daily files
piddi publish --project cmip6 --from 2026-10-08 --to today-1
```

For scheduled production batches, `piddi batch` maps each project at 1,000 distinct
pending items or a one-hour wait, then publishes each sealed batch immediately.
Both thresholds and batch sizes are configurable. Ansible provides an optional
batch timer alongside continuous harvest; enabling it performs real publication.
See [deployment](deploy/README.md#scheduled-map-and-publish).

Use `piddi COMMAND --help` for options. The
[Operations guide](https://esgf.github.io/piddiplatsch/operations/) explains date
selection, publication, retries, logging, and monitoring.

## Learn more

- [Architecture and project plugins](https://esgf.github.io/piddiplatsch/architecture/)
- [Recovery](https://esgf.github.io/piddiplatsch/recovery/)
- [Production deployment with Ansible](https://github.com/ESGF/piddiplatsch/blob/main/deploy/README.md)
- [Contributing](https://github.com/ESGF/piddiplatsch/blob/main/CONTRIBUTING.md): development, testing, and local Docker services. Run `make test` for unit and integration tests.
