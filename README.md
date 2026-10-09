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

Start harvesting and mapping Kafka messages into prepared Handle JSONL files:

```bash
piddi consume
```

By default, `consume` saves raw messages and prepares Handles **without
publishing to a Handle service**. Kafka access is required; see
[Configuration](https://esgf.github.io/piddiplatsch/configuration/) for site settings.

## Run in stages

You can also harvest a small sample, then map it separately:

```bash
piddi harvest --limit 100
piddi map --project cmip6 --date last
```

Once your Handle service profile is configured, publish a completed daily file:

```bash
piddi publish --project cmip6 --date yesterday
```

Use `piddi COMMAND --help` for options. The
[Operations guide](https://esgf.github.io/piddiplatsch/operations/) explains date
selection, publication, retries, logging, and monitoring.

## Learn more

- [Architecture and project plugins](https://esgf.github.io/piddiplatsch/architecture/)
- [Recovery and retry](https://esgf.github.io/piddiplatsch/recovery/)
- [Production deployment with Ansible](https://github.com/ESGF/piddiplatsch/blob/main/deploy/README.md)
- [Contributing](https://github.com/ESGF/piddiplatsch/blob/main/CONTRIBUTING.md): development, testing, and local Docker services. Run `make test` for unit and integration tests.
