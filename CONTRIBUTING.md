# Contributing to Piddiplatsch

Thank you for your interest in contributing to Piddiplatsch!  
This guide will help you get started with development, testing, and making contributions.

---

## 🚀 Development Setup

### Prerequisites

- [Miniconda (via conda-forge)](https://conda-forge.org/download/)
- Git
- Python 3.11+

### Setting Up Your Development Environment

```bash
# Clone the repository
git clone git@github.com:ESGF/piddiplatsch.git
cd piddiplatsch

# Create and activate conda environment
conda env create
conda activate piddi

# Install in development mode with dev dependencies
pip install -e ".[dev]"
# OR
make develop
```

---

## 🔧 CLI Usage & Options

Requires Kafka and Handle services (or use the local Docker stack for smoke tests).

- Start consumer: `piddi consume`
- Common flags:
  - `--config <path>`: point to your TOML config
  - `--verbose`: more logging
  - `--debug --log my.log`: enable debug and log to file
  - `--dump`: write incoming messages to `outputs/dump/`
  - `--dry-run`: write handle records to JSONL without contacting Handle Service
  - `--force`: continue on transient external failures (e.g., STAC outages)

### Observe Mode Example

For exploratory runs without real Handle writes:

```bash
cp etc/observe.toml .
piddi --config observe.toml consume --dry-run --dump --force
```

This configuration:
- Sets `consumer.max_errors=1000` and `stop_on_skip=false` to keep processing
- Uses `handle.backend=jsonl` for local record output
- Disables strict schema checks (`schema.strict_mode=false`)

### Config Validation

Validate the loaded configuration (defaults merged with `--config file`). Structural checks only; exits non-zero on errors:

```bash
# Validate current setup
piddi config validate

# Validate a specific TOML
piddi --config tests/config.toml config validate
```

Validations include presence and format of `consumer.projects`, `consumer.topic`, `kafka.bootstrap.servers` (comma-separated `host:port`), and backend requirements for `handle` and `lookup`. The legacy `consumer.processor` setting is accepted with a deprecation warning.

### Makefile: Config Validation Target

Before smoke tests, the Makefile validates both the default config and the test config.

```bash
# Validate default + tests/config.toml
make config-validate

# Smoke tests automatically run validation first
make test-smoke
```

This catches misconfigurations early (non-zero exit on errors) before bringing up Docker.

---

## ✅ Testing

Piddiplatsch uses a three-tier testing strategy: **unit**, **integration**, and **smoke tests**.

### Test Types

**Unit Tests** (fast, no external dependencies)  
- Located in `tests/`
- Pure logic tests with mocked dependencies
- Run on every commit

**Integration Tests** (medium speed, JSONL backend)  
- Located in `tests/integration/`
- Test component interactions using JSONL backend
- Marked with `@pytest.mark.integration`
- No Docker required
- Uses test configuration from `tests/config.toml`

**Smoke Tests** (end-to-end, requires Docker)  
- Located in `tests/smoke/`
- Full workflow tests with Kafka + mock Handle server
- Marked with `@pytest.mark.smoke`
- Docker services started/stopped automatically

### Running Tests

```bash
# Run all unit + integration tests
make test

# Run tests once with a coverage report
make coverage

# Run only unit tests
make test-unit

# Run only integration tests
make test-integration

# Run smoke tests (Docker services started automatically)
make test-smoke

# Run all tests including smoke tests
make test-all

# Run a single test file (example)
pytest tests/test_my_module.py
```

### Docker Management for Testing

Docker is only used for smoke tests. The containers provide:
- Kafka cluster (3 nodes)
- Mock Handle server

Manual commands:

```bash
make start-docker    # start services
make stop-docker     # stop services
make docker-build    # rebuild images
make docker-clean    # clean images and volumes
```

See [scripts/README.md](scripts/README.md) for details on the Docker/smoke helper scripts used by these targets.

### Troubleshooting (Kafka Healthchecks)

If Docker services don’t become healthy quickly:

- Verify container status and logs:
   ```bash
   docker-compose ps
   docker-compose logs kafka1 kafka2 kafka3
   ```
- Use the helper script to wait for broker health:
   ```bash
   scripts/wait_for_kafka_health.sh 20 2
   ```
- Fallback check (port open on localhost):
   ```bash
   nc -z localhost 39092 && echo "Kafka port open"
   ```
- Adjust retries:
   - Compose healthchecks use short intervals and 12 retries.
   - Increase retries in `scripts/wait_for_kafka_health.sh` if your machine is slow.

Notes:
- Healthchecks probe internal broker ports via container hostnames (e.g., `kafka1:19092`).
- The Makefile falls back to a quick port probe on `localhost:39092` to avoid long waits.

---

## 🧼 Code Style and Linting

This project uses automated tools to maintain code quality:

- [`black`](https://black.readthedocs.io) - Code formatting
- [`isort`](https://pycqa.github.io/isort/) - Import sorting
- [`ruff`](https://docs.astral.sh/ruff/) - Linting and fast checks
- [`pre-commit`](https://pre-commit.com) - Git hooks

### Setup Pre-commit Hooks

```bash
pip install pre-commit
pre-commit install
```

Pre-commit hooks run automatically before each commit.

### Manual Checks

```bash
pre-commit run --all-files
make lint         # Run ruff, black, and isort checks
make format       # Auto-format with black and isort
make check-format # Check formatting only
make fix          # Fix linting errors automatically
```

Before opening a pull request, run the complete non-Docker quality gate:

```bash
make lint
make test
make coverage
make dist
python -m twine check dist/*
```

Tests must write only to pytest-provided temporary directories. Do not read,
truncate, or delete the repository's ignored `outputs/`, `pid.log`, or
`stats.db`; developers may retain those files for operational inspection.

---

## 📦 Versioning and Releases

This project uses [bump-my-version](https://github.com/callowayproject/bump-my-version) for version management.

### Bumping Version

```bash
# Preview version bump (dry-run)
bump-my-version bump patch --dry-run --allow-dirty

# Bump version and create git tag
make bump-patch    # 1.0.0 → 1.0.1
make bump-minor    # 1.0.0 → 1.1.0
make bump-major    # 1.0.0 → 2.0.0

# Push changes and tags
git push && git push --tags
```

Version is stored in `pyproject.toml` and automatically tagged in git.

---

## 🔄 Development Workflow

1. **Create a branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. **Make your changes**
   - Write code
   - Add/update tests
   - Update documentation if needed
3. **Run tests and linting**
   ```bash
   make lint
   make test
   ```
4. **Commit your changes**
   ```bash
   git add .
   git commit -m "Description of your changes"
   ```
   Pre-commit hooks run automatically.
5. **Push and create Pull Request**
   ```bash
   git push origin feature/your-feature-name
   ```

---

## 📝 Writing Tests

### Unit Tests

Place unit tests in `tests/` with descriptive names:

```python
# tests/test_my_module.py
def test_function_does_something():
    result = my_function(input_data)
    assert result == expected_output
```

### Integration Tests

Place integration tests in `tests/integration/` and mark them:

```python
# tests/integration/test_my_integration.py
import pytest

pytestmark = pytest.mark.integration

def test_pipeline_with_jsonl_backend():
    # Test component interactions
    pass
```

### Smoke Tests

Place smoke tests in `tests/smoke/` and mark them:

```python
# tests/smoke/test_end_to_end.py
import pytest

pytestmark = pytest.mark.smoke

def test_full_workflow(handle_client, testfile):
    # Test complete workflow
    pass
```

---

## 🔌 Project plugins

- One consumer routes to one, several, or all plugins selected via
  `consumer.projects` or the `consume --project/--all-projects` options.
- Config for each plugin lives under `[plugins.<name>]` in the TOML.
- Example (CMIP6):

```toml
[plugins.cmip6]
landing_page_url = "https://handle-esgf.dkrz.de/lp"
max_parts = -1
excluded_asset_keys = ["reference_file", "globus", "thumbnail", "quicklook"]
```

### Adding a Plugin

1. Implement a processor under:
```
src/piddiplatsch/plugins/<name>/processor.py
```
2. Add a declarative plugin specification under the plugin package:
```python
from piddiplatsch.core.plugin import PluginSpec

plugin = PluginSpec(
    name="<name>",
    project_ids=("PUBLICATION-COLLECTION-ID",),
    make_processor=YourProcessor,
)
```
3. Import and register that specification explicitly in the static registry.
4. Provide `[plugins.<name>]` config as needed.

---

## 🐛 Reporting Issues

Include:
- Python version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs or error messages

---

## 💡 Questions?

- Open an issue on GitHub
- Start a discussion
- Check existing issues and PRs

---

Thank you for contributing to Piddiplatsch! 🎉
