"""Integration tests for retry functionality."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from piddiplatsch.config import config
from piddiplatsch.persist.retry import (
    RetryRunner,
    find_retry_files,
    load_failed_messages,
)

pytestmark = pytest.mark.integration


def _make_minimal_cmip6_item() -> dict:
    """Create a minimal valid CMIP6 item for testing."""
    ds_id = "CMIP6.ActivityX.InstitutionY.SourceZ.ssp245.r1i1p1f1.Amon.pr.gn.v20190101"
    return {
        "id": ds_id,
        "type": "Feature",
        "collection": "cmip6",
        "properties": {},
        "assets": {
            "data0000": {
                "alternate:name": "ceda.ac.uk",
                "published_on": "2019-01-01 00:00:00",
            }
        },
    }


def _create_failure_jsonl(failure_file: Path, num_items: int = 3) -> None:
    """Create a failure JSONL file with test items."""
    failure_file.parent.mkdir(parents=True, exist_ok=True)

    with failure_file.open("w", encoding="utf-8") as f:
        for i in range(num_items):
            item = _make_minimal_cmip6_item()
            # Vary the id slightly
            item["id"] = item["id"].replace("pr.gn", f"pr{i}.gn")

            # Wrap in the format that would be saved by record_failed_item
            data = {
                "data": {
                    "payload": {"item": item},
                },
                "metadata": {"time": datetime.now(UTC).isoformat()},
                "key": f"test-key-{i}",
                "__infos__": {
                    "failure_timestamp": datetime.now(UTC).isoformat(),
                    "retries": 0,
                    "reason": "Test failure",
                },
            }
            json.dump(data, f)
            f.write("\n")


def test_retry_loads_and_processes_failed_messages(tmp_path: Path):
    """Test that retry() loads failed messages and processes them through the pipeline."""
    # Configure to use temp directory and dry-run mode
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)

    # Create a failure file
    failure_file = tmp_path / "failures" / "r0" / "failed_items_2026-01-16.jsonl"
    _create_failure_jsonl(failure_file, num_items=3)

    # Verify file exists and has 3 items
    assert failure_file.exists()
    with failure_file.open("r") as f:
        lines = f.readlines()
    assert len(lines) == 3

    # Retry the failed messages
    runner = RetryRunner(
        projects=["cmip6"],
        failure_dir=tmp_path / "failures",
        delete_after=False,
        dry_run=True,
    )
    result = runner.run_file(failure_file)

    # Should have retried 3 messages
    assert result.total == 3
    assert result.succeeded == 3
    assert result.failed == 0

    # File should still exist (delete_after=False)
    assert failure_file.exists()

    # Verify handles were created in dry-run mode
    handles_dir = tmp_path / "cmip6" / "handles"
    assert handles_dir.exists()

    files = list(handles_dir.glob("retry_handles_*.jsonl"))
    assert files, "No handles jsonl file created after retry"
    assert result.handle_files == {files[0]}

    # Should have at least 3 lines (one per dataset, possibly more with assets)
    with files[0].open("r", encoding="utf-8") as f:
        content_lines = [line for line in f if line.strip()]
    assert len(content_lines) >= 3


def test_retry_increments_retry_counter(tmp_path: Path):
    """Test that retry() increments the retries counter in __infos__."""
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)

    # Create a failure file with retries=0
    failure_file = tmp_path / "failures" / "r0" / "failed_items_2026-01-16.jsonl"
    _create_failure_jsonl(failure_file, num_items=1)

    # Load messages to check retry counter
    messages = load_failed_messages(failure_file)
    assert len(messages) == 1

    _key, data = messages[0]
    assert "__infos__" in data
    assert data["__infos__"]["retries"] == 1  # Should be incremented from 0 to 1


def test_retry_deletes_file_when_delete_after_true(tmp_path: Path):
    """Test that retry() deletes the file when delete_after=True."""
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("lookup", "enabled", False)

    # Create a failure file
    failure_file = tmp_path / "failures" / "r0" / "failed_items_2026-01-16.jsonl"
    _create_failure_jsonl(failure_file, num_items=2)

    assert failure_file.exists()

    # Retry with delete_after=True
    runner = RetryRunner(
        projects=["cmip6"],
        failure_dir=tmp_path / "failures",
        delete_after=True,
        dry_run=True,
    )
    result = runner.run_file(failure_file)

    assert result.total == 2
    assert result.succeeded == 2
    # File should be deleted (no failures)
    assert not failure_file.exists()


def test_retry_handles_nonexistent_file(tmp_path: Path):
    """Test that retry() handles nonexistent file gracefully."""
    config._set("consumer", "output_dir", str(tmp_path))

    nonexistent_file = tmp_path / "does_not_exist.jsonl"

    # Should return empty result and not raise an exception
    runner = RetryRunner(
        projects=["cmip6"],
        failure_dir=tmp_path / "failures",
    )
    result = runner.run_file(nonexistent_file)
    assert result.total == 0
    assert result.succeeded == 0
    assert result.failed == 0


def test_retry_handles_empty_file(tmp_path: Path):
    """Test that retry() handles empty file gracefully."""
    config._set("consumer", "output_dir", str(tmp_path))

    empty_file = tmp_path / "empty.jsonl"
    empty_file.touch()

    runner = RetryRunner(
        projects=["cmip6"],
        failure_dir=tmp_path / "failures",
    )
    result = runner.run_file(empty_file)
    assert result.total == 0
    assert result.succeeded == 0
    assert result.failed == 0


def test_find_retry_files_single_file(tmp_path: Path):
    """Test find_retry_files with a single file."""
    file1 = tmp_path / "test1.jsonl"
    file1.touch()

    files = find_retry_files((file1,))
    assert files == [file1]


def test_find_retry_files_directory(tmp_path: Path):
    """Test find_retry_files with a directory."""
    (tmp_path / "file1.jsonl").touch()
    (tmp_path / "file2.jsonl").touch()
    (tmp_path / "file3.txt").touch()  # Non-JSONL file

    files = find_retry_files((tmp_path,))
    assert len(files) == 2
    assert all(f.suffix == ".jsonl" for f in files)


def test_find_retry_files_multiple_paths(tmp_path: Path):
    """Test find_retry_files with multiple paths."""
    file1 = tmp_path / "test1.jsonl"
    file2 = tmp_path / "test2.jsonl"
    file1.touch()
    file2.touch()

    files = find_retry_files((file1, file2))
    assert len(files) == 2
    assert file1 in files
    assert file2 in files


def test_find_retry_files_removes_duplicates(tmp_path: Path):
    """Test find_retry_files removes duplicate paths."""
    file1 = tmp_path / "test1.jsonl"
    file1.touch()

    # Pass same file twice
    files = find_retry_files((file1, file1))
    assert len(files) == 1
    assert files[0] == file1


def test_find_retry_files_skips_non_jsonl(tmp_path: Path):
    """Test find_retry_files skips non-JSONL files."""
    txt_file = tmp_path / "test.txt"
    txt_file.touch()

    files = find_retry_files((txt_file,))
    assert len(files) == 0
