import json
from datetime import UTC, datetime

import pytest

from piddiplatsch.cli import cli
from piddiplatsch.config import config

pytestmark = pytest.mark.integration


def test_retry_with_dry_run(runner, tmp_path):
    """Test that retry command works with --dry-run flag."""
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)

    # Create a failure file with a valid CMIP6 ID
    failure_file = tmp_path / "test_failure.jsonl"
    item = {
        "id": "CMIP6.CMIP.MOHC.HadGEM3-GC31-LL.ssp245.r1i1p1f1.Amon.pr.gn.v20190101",
        "type": "Feature",
        "collection": "cmip6",
        "properties": {},
        "assets": {
            "data0000": {
                "alternate:name": "ceda.ac.uk",
                "published_on": "2026-01-16 00:00:00",
            }
        },
    }

    data = {
        "data": {"payload": {"item": item}},
        "metadata": {"time": datetime.now(UTC).isoformat()},
        "key": "test-key",
        "__infos__": {
            "failure_timestamp": datetime.now(UTC).isoformat(),
            "retries": 0,
            "reason": "Test failure",
        },
    }

    with failure_file.open("w") as f:
        json.dump(data, f)
        f.write("\n")

    # Run retry with --dry-run
    result = runner.invoke(cli, ["retry", str(failure_file), "--dry-run"])

    assert result.exit_code == 0
    assert "1/1 succeeded" in result.output
    assert "All items processed successfully!" in result.output

    # Verify handles were created in dry-run mode
    handles_dir = tmp_path / "handles"
    assert handles_dir.exists()
    handle_files = list(handles_dir.glob("handles_*.jsonl"))
    assert len(handle_files) > 0


def test_retry_with_directory(runner, tmp_path):
    """Test that retry command works with a directory containing multiple files."""
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)

    # Create a directory with multiple failure files
    failures_dir = tmp_path / "failures"
    failures_dir.mkdir()

    # Helper to create a valid failure item
    def create_failure_file(filename, key_suffix):
        item = {
            "id": f"CMIP6.CMIP.MOHC.HadGEM3-GC31-LL.ssp245.r1i1p1f{key_suffix}.Amon.pr.gn.v20190101",
            "type": "Feature",
            "collection": "cmip6",
            "properties": {},
            "assets": {
                "data0000": {
                    "alternate:name": "ceda.ac.uk",
                    "published_on": "2026-01-16 00:00:00",
                }
            },
        }
        data = {
            "data": {"payload": {"item": item}},
            "metadata": {"time": datetime.now(UTC).isoformat()},
            "key": f"test-key-{key_suffix}",
            "__infos__": {
                "failure_timestamp": datetime.now(UTC).isoformat(),
                "retries": 0,
                "reason": "Test failure",
            },
        }
        with (failures_dir / filename).open("w") as f:
            json.dump(data, f)
            f.write("\n")

    # Create 3 failure files
    create_failure_file("file1.jsonl", "1")
    create_failure_file("file2.jsonl", "2")
    create_failure_file("file3.jsonl", "3")

    # Run retry with directory
    result = runner.invoke(cli, ["retry", str(failures_dir), "--dry-run"])

    assert result.exit_code == 0
    assert "3/3 succeeded" in result.output


def test_retry_with_multiple_files(runner, tmp_path):
    """Test that retry command works with multiple file arguments."""
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)

    # Create two failure files
    file1 = tmp_path / "failure1.jsonl"
    file2 = tmp_path / "failure2.jsonl"

    for idx, file in enumerate([file1, file2], 1):
        item = {
            "id": f"CMIP6.CMIP.MOHC.HadGEM3-GC31-LL.ssp245.r1i1p1f{idx}.Amon.pr.gn.v20190101",
            "type": "Feature",
            "collection": "cmip6",
            "properties": {},
            "assets": {
                "data0000": {
                    "alternate:name": "ceda.ac.uk",
                    "published_on": "2026-01-16 00:00:00",
                }
            },
        }
        data = {
            "data": {"payload": {"item": item}},
            "metadata": {"time": datetime.now(UTC).isoformat()},
            "key": f"test-key-{idx}",
            "__infos__": {
                "failure_timestamp": datetime.now(UTC).isoformat(),
                "retries": 0,
                "reason": "Test failure",
            },
        }
        with file.open("w") as f:
            json.dump(data, f)
            f.write("\n")

    # Run retry with multiple files
    result = runner.invoke(cli, ["retry", str(file1), str(file2), "--dry-run"])

    assert result.exit_code == 0
    assert "2/2 succeeded" in result.output
