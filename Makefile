# Configuration
APP_ROOT := $(abspath $(lastword $(MAKEFILE_LIST))/..)
APP_NAME := piddiplatsch

# end of configuration

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
	else:
		match = re.match(r'^## (.*)$$', line)
		if match:
			help = match.groups()[0]
			print("\n%s" % (help))
endef
export PRINT_HELP_PYSCRIPT

.DEFAULT_GOAL := help

help: ## print this help message. (Default)
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

## Build targets:

install: ## install application
	@echo "Installing application ..."
	@bash -c 'pip install -e .'
	@echo "\nInspect commands with \`piddi --help\`."

develop: ## install application with development libraries
	@echo "Installing development requirements ..."
	@bash -c 'pip install -e ".[dev]"'

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	@echo "Removing build artifacts ..."
	@-rm -fr build/
	@-rm -fr dist/
	@-rm -fr .eggs/
	@-find . -name '*.egg-info' -exec rm -fr {} +
	@-find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	@echo "Removing Python file artifacts ..."
	@-find . -name '*.pyc' -exec rm -f {} +
	@-find . -name '*.pyo' -exec rm -f {} +
	@-find . -name '*~' -exec rm -f {} +
	@-find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	@echo "Removing test artifacts ..."
	@-rm -fr .tox/
	@-rm -f .coverage
	@-rm -fr .pytest_cache
	@-rm -fr .hypothesis
	@-rm -fr .ruff_cache
	@-rm -fr htmlcov

clean-dist: clean  ## remove ignored build files while preserving runtime data and local config
	@echo "Running guarded 'git clean' ..."
	@test -z "$$(git status --porcelain)" || { echo "There are uncommitted or untracked changes; aborting git clean."; exit 1; }
	@git clean -dfx -e outputs/ -e '*.log' -e '*.db' -e 'custom*.toml' -e .env -e .venv/ -e .vscode/

lint: ## check style with ruff, black, isort
	@echo "Running code style checks (ruff, black, isort) ..."
	@bash -c 'ruff check src tests'
	@bash -c 'black --check src tests'
	@bash -c 'isort --check-only src tests'

fix: ## fix linting errors automatically
	@echo "Running ruff with fix option ..."
	@bash -c 'ruff check --fix src tests'

format: ## format code using isort and black
	@echo "Formatting code with isort and black ..."
	@bash -c 'isort src tests'
	@bash -c 'black src tests'

check-format: ## check that code is correctly formatted
	@echo "Checking code formatting with isort and black ..."
	@bash -c 'isort --check-only src tests'
	@bash -c 'black --check src tests'

pre-commit: ## run all pre-commit hooks
	@pre-commit run --all-files


## Testing targets:

test: test-unit test-integration ## run all fast tests (unit + integration, no Docker required)

config-validate: ## validate default and test configuration before smoke/integration
	@echo "Validating default configuration ..."
	@bash -c 'python -m piddiplatsch.cli config validate'
	@echo "Validating tests/config.toml ..."
	@bash -c 'python -m piddiplatsch.cli --config tests/config.toml config validate'

test-unit: ## run unit tests only (fast, no external dependencies - unmarked tests)
	@echo "Running unit tests ..."
	@bash -c 'python -m pytest -v -m "not integration and not smoke" tests/'

test-integration: ## run integration tests only (JSONL backend, no Docker required)
	@echo "Running integration tests ..."
	@bash -c 'python -m pytest -v -m "integration" tests/'

test-smoke: config-validate start-docker ## run smoke tests only (requires Docker: Kafka + Handle server)
	@echo "Running smoke tests ..."
	# Ensure Kafka topic exists before starting consumer
	@echo "Ensuring Kafka topic exists (from config) ..."
	@python scripts/ensure_kafka_topic.py || true
	# Start production consumer in background
	@echo "Starting piddi consumer (background) ..."
	@bash -c 'python -m piddiplatsch.cli --config tests/config.toml consume & echo $$! > .consumer.pid && echo "Consumer PID: $$(cat .consumer.pid)"'
	# Run smoke tests; on failure, ensure consumer and docker are stopped
	@bash -c 'python -m pytest -v -s -m "smoke" tests/' || ($(MAKE) stop-consumer; $(MAKE) stop-docker; exit 1)
	# Stop consumer and docker after tests
	@$(MAKE) stop-consumer
	@$(MAKE) stop-docker

test-all: test-unit test-integration test-smoke ## run all tests including smoke tests

smoke: test-smoke

coverage: ## check code coverage quickly with the default Python
	@python -m pytest -m "not smoke" --cov=piddiplatsch --cov-report=term-missing --cov-report=html tests/

## Deployment targets:

dist: clean ## builds source and wheel package
	python -m build
	ls -l dist

release: dist ## package and upload a release
	python -m twine upload dist/*

## Versioning targets

bump-patch: ## bump patch version and create git tag
	@echo "Bumping patch version..."
	@bump-my-version bump patch

bump-minor: ## bump minor version and create git tag
	@echo "Bumping minor version..."
	@bump-my-version bump minor

bump-major: ## bump major version and create git tag
	@echo "Bumping major version..."
	@bump-my-version bump major

## Docker test services targets

start-docker: ## start Docker services (Kafka + Handle server) for testing
	@echo "======================================================================"
	@echo "🐳 Starting Docker services (Kafka + Handle server)..."
	@echo "======================================================================"
	@docker-compose up --build -d
	@bash -c 'scripts/wait_for_kafka_health.sh 20 2' || (echo "⚠️ Brokers not healthy; falling back to port check on localhost:39092" && bash -c 'retries=20; i=0; while [ $$i -lt $$retries ]; do if nc -z localhost 39092 >/dev/null 2>&1; then echo "✅ Kafka port open (fallback)!"; exit 0; fi; sleep 1; i=$$((i+1)); done; echo "❌ Kafka not ready after fallback."; exit 1')
	@echo "✅ Docker services started!"
	@echo ""

stop-docker: ## stop Docker test services
	@echo "======================================================================"
	@echo "🐳 Stopping Docker services..."
	@echo "======================================================================"
	@docker-compose down -v
	@echo "✅ Docker services stopped!"
	@echo ""

# Local consumer management
start-consumer:
	@echo "Starting piddi consumer ..."
	# Ensure Kafka topic exists before starting consumer
	@echo "Ensuring Kafka topic exists (from config) ..."
	@bash -c 'python -c "from piddiplatsch.config import config; config.load_user_config(\"tests/config.toml\"); from piddiplatsch.testing.kafka_client import ensure_topic_exists_from_config; ensure_topic_exists_from_config()"'
	@bash -c 'python -m piddiplatsch.cli --config tests/config.toml consume & echo $$! > .consumer.pid && echo "Consumer PID: $$(cat .consumer.pid)"'

stop-consumer:
	@echo "Stopping piddi consumer ..."
	@bash -c 'if [ -f .consumer.pid ]; then kill $$(cat .consumer.pid) >/dev/null 2>&1 || true; rm -f .consumer.pid; else echo "No consumer PID file"; fi'

docker-build: ## build Docker images for test services
	@echo "Building Docker images..."
	@docker-compose build

docker-clean: stop-docker ## remove all Docker images and volumes
	@echo "======================================================================"
	@echo "🧹 Cleaning Docker images..."
	@echo "======================================================================"
	@docker image prune -f
	@echo "✅ Docker cleaned!"
	@echo ""
