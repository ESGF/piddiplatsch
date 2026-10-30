# Contributing to Piddiplatsch

Use this guide to set up a development environment, run checks, and add plugins.
For CLI usage, start with the [README](README.md) and [Operations](docs/operations.md).

## Set up development

You need Git, Conda, and Python 3.11 or newer.

```bash
git clone https://github.com/ESGF/piddiplatsch.git
cd piddiplatsch
conda env create
conda activate piddi
make develop
pre-commit install
```

`make develop` installs Piddi, test and style tools, and MkDocs.
Pre-commit runs the configured checks before each commit.

Copy `etc/esgf-example.toml` to `custom.toml` for manual work. Keep real
credentials in that ignored file. Use `tests/config.toml` for the local Docker
services. See [Configuration](docs/configuration.md) for loading rules.

## Check configuration

```bash
piddi config validate
piddi --config tests/config.toml config validate
make config-validate
```

Validation runs offline. It checks structure, names, required settings, and
numeric limits. It does not test connections or verify credentials.

`make config-validate` checks both the ESGF example and test configuration over
packaged defaults. The examples provide placeholders for offline checks.
CI and smoke tests use the same validation target.

## Run tests

| Command | Coverage |
| --- | --- |
| `make test` | Unit and integration tests; no Docker. |
| `make test-unit` | Unit tests only. |
| `make test-integration` | Integration tests only. |
| `make coverage` | Non-smoke tests with a coverage report. |
| `make test-smoke` | End-to-end tests with Docker Kafka and a mock Handle service. |
| `make test-all` | Unit, integration, and smoke tests. |

To run one file, use `pytest tests/integration/test_cli.py`.

Unit tests live in `tests/`. Integration tests live in `tests/integration/` and
use the `integration` marker. Smoke tests live in `tests/smoke/` and use the
`smoke` marker. Smoke tests start Docker services and run harvest followed by sync.

Tests must use pytest's temporary directories. Do not read, truncate, or delete
the checkout's ignored `outputs/`, `pid.log`, or `piddi.db`. These may contain
local operational data.

Live Handle tests are opt-in and need a disposable prefix. See
[Real Handle service contract test](docs/operations.md#real-handle-service-contract-test).

### Docker test services

```bash
make start-docker
make stop-docker
make docker-build
```

The stack has three Kafka brokers and a mock Handle server.
`make stop-docker` removes the test volumes. `make docker-clean` also prunes
unused Docker images.

If Kafka does not become ready, check its containers and logs:

```bash
docker-compose ps
docker-compose logs kafka1 kafka2 kafka3
scripts/wait_for_kafka_health.sh 20 2
```

The Makefile also checks port `39092` on localhost if the health check times out.
See [Test scripts](scripts/README.md) for the helper scripts.

## Check code style

Ruff checks the code. Black formats it, and isort sorts imports.

```bash
make lint
make format
make check-format
make fix
pre-commit run --all-files
```

`make format` and `make fix` change files. Review their changes before committing.

Before opening a pull request, run:

```bash
make lint
make test
make coverage
make dist
python -m twine check dist/*
```

For documentation changes, follow [Building the documentation](docs/building.md).

## Add a project plugin

Plugins share mapping code and Handle models. Keep project-specific code small.

1. Add a package under `src/piddiplatsch/plugins/<python_name>/` with `model.py`,
   `record.py`, `processor.py`, and `plugin.py` as needed.
2. Reuse `DatasetHandleModel`, `FileHandleModel`, `ProjectDatasetRecord`,
   `ProjectFileRecord`, and `StacProjectProcessor`.
3. Declare a plugin specification and register it in the static registry.
4. Add `[plugins.<name>]` settings where needed.
5. Add a fixture based on a real publication, plus plugin and integration tests.

A specification looks like this:

```python
from piddiplatsch.core.plugin import PluginSpec

plugin = PluginSpec(
    name="<name>",
    project_ids=("PUBLICATION-COLLECTION-ID",),
    make_processor=YourProcessor,
)
```

Keep PID field names and real project differences in the plugin. Shared behavior
belongs in the common classes. See [Architecture](docs/architecture.md).

Harvest stores every registered project it encounters. Map and sync select
projects through `consumer.projects` and their CLI options. Plugin settings live
under `[plugins.<name>]`.

## Versions and releases

The version is stored in `pyproject.toml`. Bump-my-version updates it and creates
a Git commit and tag:

```bash
# Preview a patch release
bump-my-version bump patch --dry-run --allow-dirty

# Create the version commit and tag
make bump-patch
# Or use make bump-minor / make bump-major
```

Review the result before pushing commits and tags. `make dist` builds packages;
`make release` builds and uploads them.

## Report a problem

Include the Python version, operating system, steps to reproduce, expected result,
and actual result. Add relevant logs with credentials removed. Use GitHub issues
for bugs and discussions for questions.
