"""Failure JSONL is diagnostic; recovery repeats the stage's normal inputs."""

import pytest

from piddiplatsch.cli import cli

pytestmark = pytest.mark.integration


def test_retry_rejects_diagnostic_file_inputs(runner, tmp_path):
    failure_file = tmp_path / "failed.jsonl"
    failure_file.write_text('{"diagnostic": "keep for inspection"}\n')
    original = failure_file.read_bytes()
    result = runner.invoke(cli, ["retry", str(failure_file)])
    assert result.exit_code == 2
    assert "Invalid value" in result.output or "Missing option" in result.output
    assert failure_file.read_bytes() == original
