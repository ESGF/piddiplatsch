import fcntl
import gzip
import json
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.dump_maintenance import deduplicate_closed_dumps


@pytest.fixture
def archives(tmp_path, monkeypatch):
    config._set("consumer", "output_dir", str(tmp_path))
    monkeypatch.setattr(
        "piddiplatsch.dump_maintenance.utc_now",
        lambda: datetime(2026, 10, 30, tzinfo=UTC),
    )
    monkeypatch.setattr(
        "piddiplatsch.maintenance.utc_now", lambda: datetime(2026, 10, 30, tzinfo=UTC)
    )
    root = tmp_path / "dump"
    root.mkdir()
    return root


def event(offset=1, *, partition=0, topic="topic", value="original"):
    return dict(
        source=dict(topic=topic, partition=partition, offset=offset),
        key="dataset",
        payload=dict(value=value),
    )


def write(root, name, records):
    path = root / name
    data = b"".join((json.dumps(record) + "\n").encode() for record in records)
    path.write_bytes(gzip.compress(data) if path.suffix == ".gz" else data)
    return path


def read(path):
    data = path.read_bytes()
    if path.suffix == ".gz":
        data = gzip.decompress(data)
    return [json.loads(line) for line in data.splitlines()]


def test_deduplication_across_plain_and_gzip_parts_preserves_order_and_conflicts(
    archives,
):
    first = write(
        archives, "dump_messages_2026-10-28_000001.jsonl.gz", [event(), event()]
    )
    conflict = event(value="different payload at same offset")
    legacy = dict(data=dict(payload="no coordinates"))
    kept = [
        event(2),
        conflict,
        event(partition=1),
        event(topic="other"),
        legacy,
        legacy,
    ]
    second = write(
        archives, "dump_messages_2026-10-29_000001.jsonl", [event(), *kept, event(2)]
    )
    before = {
        path: (path.stat().st_mode, path.stat().st_mtime_ns) for path in (first, second)
    }
    assert deduplicate_closed_dumps() == 3
    assert read(first) == [event()]
    assert read(second) == kept
    assert {
        path: (path.stat().st_mode, path.stat().st_mtime_ns) for path in (first, second)
    } == before
    assert deduplicate_closed_dumps() == 0
    assert not [
        path
        for path in archives.glob(".deduplicate-*")
        if path.name != ".deduplicate-cache"
    ]


def test_only_old_raw_dumps_are_deduplicated(archives):
    paths = [
        write(archives, name, [event(), event()])
        for name in (
            "dump_messages_2026-10-30_000001.jsonl",
            "dump_messages_2026-10-31.jsonl.gz",
            "unknown_projects_2026-10-29.jsonl",
            "dump_messages_invalid.jsonl",
            "dump_messages_2025-99-99.jsonl",
        )
    ]
    old = write(archives, "dump_messages_2026-10-29.jsonl", [event(), event()])
    assert deduplicate_closed_dumps() == 1
    assert all(read(path) == [event(), event()] for path in paths)
    assert read(old) == [event()]


def test_dry_run_does_not_rewrite_archives(archives, monkeypatch):
    guard = Mock()
    monkeypatch.setattr(
        "piddiplatsch.dump_maintenance.get_disk_guard", lambda **_: guard
    )
    paths = [
        write(
            archives,
            f"dump_messages_2026-10-29_00000{number}.jsonl.gz",
            [event(), event()],
        )
        for number in (1, 2)
    ]
    before = {path: path.read_bytes() for path in paths}
    assert deduplicate_closed_dumps(dry_run=True) == 3
    guard.check.assert_not_called()
    assert {path: path.read_bytes() for path in paths} == before
    assert deduplicate_closed_dumps() == 3
    assert read(paths[0]) == [event()]
    assert read(paths[1]) == []


def test_cli_dry_run_previews_duplicate_count_without_changing_files(archives):
    path = write(archives, "dump_messages_2026-10-29.jsonl", [event(), event()])
    original = path.read_bytes()
    result = CliRunner().invoke(cli, ["maintain", "--deduplicate-dumps", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "Would remove 1 duplicate dump event(s)" in result.output
    assert path.read_bytes() == original
    assert not list(archives.glob("*.gz"))


def test_symlinks_are_skipped_and_json_formatting_does_not_affect_identity(
    archives, tmp_path
):
    target = write(tmp_path, "outside.jsonl", [event(), event()])
    (archives / "dump_messages_2026-10-28.jsonl").symlink_to(target)
    path = write(archives, "dump_messages_2026-10-29.jsonl", [event()])
    with path.open("ab") as stream:
        stream.write(
            (json.dumps(event(), sort_keys=True, separators=(",", ":")) + "\n").encode()
        )
    assert deduplicate_closed_dumps() == 1
    assert read(path) == [event()]
    assert read(target) == [event(), event()]


@pytest.mark.parametrize("tail", [b"not JSON\n", b'{"incomplete":true}'])
def test_invalid_or_incomplete_file_is_kept_intact(archives, tail):
    path = write(archives, "dump_messages_2026-10-29.jsonl", [event(), event()])
    original = path.read_bytes() + tail
    path.write_bytes(original)
    with pytest.raises(ValueError, match="keeping original"):
        deduplicate_closed_dumps()
    assert path.read_bytes() == original
    assert not [
        path
        for path in archives.glob(".deduplicate-*")
        if path.name != ".deduplicate-cache"
    ]


def test_failed_replacement_preserves_original(archives, monkeypatch):
    path = write(archives, "dump_messages_2026-10-29.jsonl", [event(), event()])
    original = path.read_bytes()
    monkeypatch.setattr(
        "piddiplatsch.dump_maintenance.Path.replace",
        Mock(side_effect=OSError("disk full")),
    )
    with pytest.raises(OSError, match="disk full"):
        deduplicate_closed_dumps()
    assert path.read_bytes() == original
    assert not [
        path
        for path in archives.glob(".deduplicate-*")
        if path.name != ".deduplicate-cache"
    ]


def test_changed_dump_is_not_replaced(archives, monkeypatch):
    from piddiplatsch.dump_maintenance import _fingerprint

    path = write(archives, "dump_messages_2026-10-29.jsonl", [event(), event()])
    original = path.read_bytes()
    changed = False

    def fingerprint(line):
        nonlocal changed
        if not changed:
            changed = True
            with path.open("ab") as stream:
                stream.write(b"{}\n")
        return _fingerprint(line)

    monkeypatch.setattr("piddiplatsch.dump_maintenance._fingerprint", fingerprint)
    with pytest.raises(OSError, match="changed during deduplication"):
        deduplicate_closed_dumps()
    assert path.read_bytes() == original + b"{}\n"


def test_overlapping_maintenance_is_skipped(archives):
    path = write(archives, "dump_messages_2026-10-29.jsonl", [event(), event()])
    with (archives / ".maintenance.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert deduplicate_closed_dumps() == 0
    assert read(path) == [event(), event()]


def test_unchanged_archives_reuse_cache_without_parsing_or_recompression(
    archives, monkeypatch
):
    path = write(archives, "dump_messages_2026-10-29.jsonl.gz", [event(), event(2)])
    original = path.read_bytes()
    assert deduplicate_closed_dumps() == 0
    monkeypatch.setattr(
        "piddiplatsch.dump_maintenance._fingerprint",
        Mock(side_effect=AssertionError("Unchanged archive parsed again")),
    )
    monkeypatch.setattr(
        "piddiplatsch.dump_maintenance.gzip.GzipFile.write",
        Mock(side_effect=AssertionError("Unchanged archive recompressed")),
    )
    assert deduplicate_closed_dumps() == 0
    assert deduplicate_closed_dumps(dry_run=True) == 0
    assert path.read_bytes() == original


def test_new_archive_duplicates_are_detected_against_cached_older_records(
    archives, monkeypatch
):
    from piddiplatsch.dump_maintenance import _fingerprint

    first = write(archives, "dump_messages_2026-10-28.jsonl.gz", [event()])
    assert deduplicate_closed_dumps() == 0
    second = write(archives, "dump_messages_2026-10-29.jsonl", [event(), event(2)])
    fingerprint = Mock(wraps=_fingerprint)
    monkeypatch.setattr("piddiplatsch.dump_maintenance._fingerprint", fingerprint)
    assert deduplicate_closed_dumps() == 1
    assert fingerprint.call_count == 2
    assert read(first) == [event()]
    assert read(second) == [event(2)]


@pytest.mark.parametrize("corruption", ["truncated", "checksum", "header"])
def test_corrupt_cache_is_discarded_and_source_is_rescanned(archives, corruption):
    first = write(archives, "dump_messages_2026-10-28.jsonl", [event()])
    assert deduplicate_closed_dumps() == 0
    cache = next((archives / ".deduplicate-cache").glob("*.bin"))
    data = cache.read_bytes()
    if corruption == "truncated":
        data = data[:-10]
    elif corruption == "checksum":
        data = data[:-1] + bytes([data[-1] ^ 1])
    else:
        data = b"invalid header\n" + data
    cache.write_bytes(data)
    second = write(archives, "dump_messages_2026-10-29.jsonl", [event(), event(2)])
    assert deduplicate_closed_dumps() == 1
    assert read(first) == [event()]
    assert read(second) == [event(2)]


def test_changed_or_removed_sources_do_not_leave_stale_cached_events(archives):
    first = write(archives, "dump_messages_2026-10-28.jsonl", [event()])
    assert deduplicate_closed_dumps() == 0
    write(archives, first.name, [event(2)])
    second = write(archives, "dump_messages_2026-10-29.jsonl", [event(), event(2)])
    assert deduplicate_closed_dumps() == 1
    assert read(second) == [event()]
    first.unlink()
    third = write(archives, "dump_messages_2026-10-29_000001.jsonl", [event(2)])
    assert deduplicate_closed_dumps() == 0
    assert read(third) == [event(2)]


@pytest.mark.parametrize(
    "enabled,flag,removed",
    [
        (False, [], False),
        (True, [], True),
        (False, ["--deduplicate-dumps"], True),
        (True, ["--no-deduplicate-dumps"], False),
    ],
)
def test_maintenance_config_and_cli_override(archives, enabled, flag, removed):
    config._set("maintenance", "deduplicate_dumps", enabled)
    path = write(archives, "dump_messages_2026-10-29.jsonl", [event(), event()])
    result = CliRunner().invoke(cli, ["maintain", *flag])
    assert result.exit_code == 0, result.output
    assert read(path.with_suffix(".jsonl.gz")) == (
        [event()] if removed else [event(), event()]
    )
    if removed:
        assert "Removed 1 duplicate dump event(s)" in result.output
