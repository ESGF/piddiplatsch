"""Configuration commands must work without loading the native Kafka client."""

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
def test_config_commands_do_not_import_kafka(tmp_path, command):
    source_root = Path(__file__).resolve().parents[1] / "src"
    config_path = Path(__file__).resolve().parent / "config.toml"
    # A fresh process is essential: other tests may already have imported Kafka.
    script = """
import importlib.abc
import sys

class BlockKafka(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "confluent_kafka" or fullname.startswith("confluent_kafka."):
            raise AssertionError("Configuration command attempted to import Kafka")

sys.meta_path.insert(0, BlockKafka())
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
