"""Configuration commands must work without loading Kafka or HTTP clients."""

import os
import subprocess  # noqa: S404 - fresh interpreter required for import isolation
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "command",
    [
        ["--help"],
        ["config", "explain", "--project", "cmip6"],
        ["config", "validate"],
        ["config", "show", "--section", "consumer"],
    ],
)
def test_config_commands_do_not_import_network_clients(tmp_path, command):
    source_root = Path(__file__).resolve().parents[1] / "src"
    config_path = Path(__file__).resolve().parent / "config.toml"
    # A fresh process is essential: other tests may already have imported Kafka.
    script = """
import importlib.abc
import sys

class BlockNetworkClients(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"confluent_kafka", "requests", "urllib3", "brotli", "brotlicffi", "_brotli"}:
            raise AssertionError("Configuration command attempted to import a network client: " + fullname)

sys.meta_path.insert(0, BlockNetworkClients())
from piddiplatsch.cli import cli
cli.main(args=sys.argv[1:], standalone_mode=False)
assert "confluent_kafka" not in sys.modules
"""
    result = subprocess.run(  # noqa: S603 - fixed test script and arguments, no shell
        [sys.executable, "-c", script, "--config", str(config_path), *command],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(source_root)},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "RuntimeWarning" not in result.stderr


@pytest.mark.parametrize(
    "module, blocked",
    [
        (
            "piddiplatsch.catalog",
            [
                "sqlite3",
                "duckdb",
                "confluent_kafka",
                "requests",
                "pystac_client",
                "piddiplatsch.config",
                "piddiplatsch.consumer",
            ],
        ),
        (
            "piddiplatsch.core.pipeline",
            [
                "piddiplatsch.consumer",
                "piddiplatsch.jsonl_stream",
                "piddiplatsch.runners",
            ],
        ),
        (
            "piddiplatsch.commands.map",
            [
                "piddiplatsch.consumer",
                "confluent_kafka",
                "piddiplatsch.jsonl_stream",
                "piddiplatsch.lookup",
            ],
        ),
    ],
)
def test_processing_boundaries_do_not_import_input_adapters(tmp_path, module, blocked):
    source_root = Path(__file__).resolve().parents[1] / "src"
    script = """
import importlib
import importlib.abc
import sys

blocked = sys.argv[2:]
class BlockInputAdapters(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
            raise AssertionError("Unexpected input-adapter dependency: " + fullname)

sys.meta_path.insert(0, BlockInputAdapters())
importlib.import_module(sys.argv[1])
"""
    result = subprocess.run(  # noqa: S603 - fixed import-isolation script, no shell
        [sys.executable, "-c", script, module, *blocked],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(source_root)},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_healthy_catalog_harvest_does_not_load_http_or_brotli(tmp_path):
    source_root = Path(__file__).resolve().parents[1] / "src"
    fixture = (
        Path(__file__).resolve().parent / "testdata/publication_samples/cmip6.json"
    )
    script = """
import importlib.abc
import json
import sys
from contextlib import closing
from pathlib import Path

class BlockHttp(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'requests', 'urllib3', 'brotli', 'brotlicffi', '_brotli', 'pystac_client'}:
            raise AssertionError('Healthy harvest loaded an HTTP dependency: ' + fullname)

sys.meta_path.insert(0, BlockHttp())
from piddiplatsch.commands.harvest import HarvestCommand
from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.core.messages import KafkaSource
with closing(CatalogHarvester(Path(sys.argv[1]))) as harvester:
    result = harvester.process_with_source('key', json.loads(Path(sys.argv[2]).read_text()), KafkaSource('topic', 0, 0))
    assert result.catalog_written
    assert not result.catalog_refreshed
assert 'requests' not in sys.modules
assert '_brotli' not in sys.modules
"""
    result = subprocess.run(  # noqa: S603 - fixed isolated test script
        [sys.executable, "-c", script, str(tmp_path / "catalog"), str(fixture)],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(source_root)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
