import fcntl
import gzip
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.maintenance import compress_closed_dumps


@pytest.fixture
def dumps(tmp_path, monkeypatch):
    config._set("consumer", "output_dir", str(tmp_path))
    monkeypatch.setattr(
        "piddiplatsch.maintenance.utc_now", lambda: datetime(2026, 10, 30, tzinfo=UTC)
    )
    directory = tmp_path / "dump"
    directory.mkdir()
    for day in ("2026-10-29", "2026-10-30", "2026-10-31", "invalid"):
        (directory / f"dump_messages_{day}.jsonl").write_bytes(
            b'{"payload": "test"}\n' * 1000
        )
    return directory


def test_maintenance_compresses_only_closed_dumps_and_is_repeatable(dumps):
    original = (dumps / "dump_messages_2026-10-29.jsonl").read_bytes()
    paths = compress_closed_dumps()
    assert len(paths) == 1
    assert gzip.decompress(paths[0].read_bytes()) == original
    assert not (dumps / "dump_messages_2026-10-29.jsonl").exists()
    assert (dumps / "dump_messages_2026-10-30.jsonl").exists()
    assert (dumps / "dump_messages_2026-10-31.jsonl").exists()
    assert (dumps / "dump_messages_invalid.jsonl").exists()
    assert compress_closed_dumps() == []


def test_failed_compression_preserves_original_and_cleans_temporary_file(
    dumps, monkeypatch
):
    monkeypatch.setattr(
        "piddiplatsch.maintenance.gzip.GzipFile.write",
        Mock(side_effect=OSError("disk full")),
    )
    with pytest.raises(OSError, match="disk full"):
        compress_closed_dumps()
    assert (dumps / "dump_messages_2026-10-29.jsonl").exists()
    assert not list(dumps.glob("*.gz"))
    assert not list(dumps.glob("*.tmp"))


def test_compression_refuses_to_remove_a_changed_dump(dumps, monkeypatch):
    source = dumps / "dump_messages_2026-10-29.jsonl"
    guard = Mock()

    def change(**_kwargs):
        if guard.check.call_count == 2:
            with source.open("ab") as stream:
                stream.write(b"{}\n")

    guard.check.side_effect = change
    monkeypatch.setattr("piddiplatsch.maintenance.get_disk_guard", lambda **_: guard)
    with pytest.raises(OSError, match="changed during compression"):
        compress_closed_dumps()
    assert source.exists()
    assert not list(dumps.glob("*.gz"))


def test_overlapping_maintenance_run_is_skipped(dumps):
    with (dumps / ".maintenance.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert compress_closed_dumps() == []
    assert not list(dumps.glob("*.gz"))


def test_maintenance_dry_run_keeps_originals(dumps):
    result = CliRunner().invoke(cli, ["maintain", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "Would compress 1 closed dump file(s)" in result.output
    assert "dump_messages_2026-10-29.jsonl" in result.output
    assert (dumps / "dump_messages_2026-10-29.jsonl").exists()
    assert not list(dumps.glob("*.gz"))


def test_existing_archive_is_not_replaced_or_used_to_delete_source(dumps):
    source = dumps / "dump_messages_2026-10-29.jsonl"
    archived = source.with_suffix(".jsonl.gz")
    archived.write_bytes(gzip.compress(b"older archive"))
    original = source.read_bytes()
    assert compress_closed_dumps() == []
    assert source.read_bytes() == original
    assert gzip.decompress(archived.read_bytes()) == b"older archive"
