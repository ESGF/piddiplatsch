"""Failure JSONL is diagnostic; recovery repeats the stage's normal inputs."""

import pytest

from piddiplatsch.cli import cli

pytestmark = pytest.mark.integration


def test_retry_is_not_an_operational_command(runner, tmp_path):
    failure_file = tmp_path / "failed.jsonl"
    failure_file.write_text('{"diagnostic": "keep for inspection"}\n')
    original = failure_file.read_bytes()
    result = runner.invoke(cli, ["retry", str(failure_file)])
    assert result.exit_code == 2
    assert "No such command" in result.output
    assert failure_file.read_bytes() == original
