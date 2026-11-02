"""A dated reset replays a complete suffix without disturbing older history."""

import gzip
import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.runners.reset import preview_reset, reset_processing

PROJECT = "cmip6plus"
CUTOFF = datetime(2026, 10, 27)


def snapshot(database, table):
    with closing(sqlite3.connect(database)) as connection:
        connection.row_factory = sqlite3.Row
        return [
            dict(row)
            for row in connection.execute(
                {
                    "work": "SELECT * FROM work_items ORDER BY id",
                    "stats": "SELECT * FROM stats ORDER BY project,kind,id",
                    "catalog": "SELECT * FROM catalog_items ORDER BY project,item_id",
                    "checkpoints": "SELECT * FROM kafka_checkpoints ORDER BY topic,partition",
                }[table]
            )
        ]


@pytest.fixture
def dated(tmp_path, testdata_path):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("catalog", "directory", str(tmp_path))
    with closing(CatalogHarvester(tmp_path)) as harvester:
        for offset, project in enumerate((PROJECT, "cmip7", PROJECT, PROJECT, PROJECT)):
            value = json.loads(
                (testdata_path / "publication_samples" / f"{project}.json").read_text()
            )
            value["data"]["payload"]["item"]["properties"]["revision"] = offset
            harvester.process_with_source("key", value, KafkaSource("topic", 0, offset))
    database = tmp_path / "piddi.db"
    batches = {"old": [1], "cross": [3, 4], "later": [5]}
    paths, receipts = {}, {}
    with SQLiteCatalog(PROJECT, database) as writer:
        for batch, ids in batches.items():
            handles = sum(ids)
            writer.seal_batch(
                batch, [dict(id=identity, handle_count=identity) for identity in ids]
            )
            writer.publish_batch(batch, handles)
            path = (
                tmp_path
                / "batches"
                / PROJECT
                / "published"
                / "2026"
                / "11"
                / "01"
                / batch
            )
            path.mkdir(parents=True)
            receipt = tmp_path / "published" / f"{batch}.jsonl"
            receipt.parent.mkdir(exist_ok=True)
            receipt.write_text(batch + " receipt\n")
            manifest = dict(
                schema_version=3,
                project=PROJECT,
                batch_id=batch,
                status="published",
                outcomes=[dict(id=identity) for identity in ids],
                receipt=str(receipt),
            )
            (path / "manifest.json").write_text(json.dumps(manifest))
            (path / "publication-progress.jsonl").write_text(batch + " journal\n")
            paths[batch], receipts[batch] = path, receipt
        days = {
            1: "2026-10-20",
            2: "2026-10-21",
            3: "2026-10-25",
            4: "2026-10-28",
            5: "2026-10-30",
        }
        for identity, day in days.items():
            writer._execute(
                "UPDATE work_items SET created_at=?,updated_at='2026-11-02T12:00:00+00:00' WHERE id=?",
                (day + "T00:00:00+00:00", identity),
            )
        for identity, day in (
            ("run-old", "2026-10-21"),
            ("run-overlap", "2026-10-26"),
            ("run-new", "2026-10-30"),
        ):
            writer._execute(
                "INSERT INTO stats(kind,project,id,status,created_at,updated_at) VALUES('run',?,?,'completed',?,?)",
                (
                    PROJECT,
                    identity,
                    "2026-10-20T00:00:00+00:00",
                    day + "T00:00:00+00:00",
                ),
            )
    legacy = tmp_path / PROJECT / "legacy.jsonl"
    legacy.parent.mkdir()
    legacy.write_text("unrelated output\n")
    return tmp_path, database, paths, receipts


def test_dated_reset_expands_batch_and_preserves_earlier_history(dated):
    root, database, paths, receipts = dated
    before = {
        table: snapshot(database, table)
        for table in ("work", "stats", "catalog", "checkpoints")
    }
    plan = preview_reset(config, PROJECT, from_date=CUTOFF)
    assert plan.count == 3
    assert plan.first_id == 3
    assert plan.effective_from == "2026-10-25T00:00:00+00:00"
    assert plan.batch_ids == ("cross", "later")
    assert snapshot(database, "work") == before["work"]
    count, archive = reset_processing(config, PROJECT, from_date=CUTOFF)
    assert count == 3
    work = snapshot(database, "work")
    assert work[:2] == before["work"][:2]  # Older work and the other project.
    for row, previous in zip(work[2:], before["work"][2:], strict=True):
        assert row["state"] == "pending"
        assert row["batch_id"] is None
        assert row["handle_count"] == 0
        assert (row["stac_blob"], row["created_at"], row["event_type"]) == (
            previous["stac_blob"],
            previous["created_at"],
            previous["event_type"],
        )
    assert snapshot(database, "catalog") == before["catalog"]
    assert snapshot(database, "checkpoints") == before["checkpoints"]
    retained = {row["id"]: row for row in snapshot(database, "stats")}
    assert set(retained) == {"old", "run-old"}
    assert all(
        row == next(old for old in before["stats"] if old["id"] == identity)
        for identity, row in retained.items()
    )
    assert paths["old"].exists() and receipts["old"].exists()
    assert not paths["cross"].exists() and not paths["later"].exists()
    assert not receipts["cross"].exists() and not receipts["later"].exists()
    assert (archive / "receipts/published/cross.jsonl").read_text() == "cross receipt\n"
    assert len(list(archive.rglob("publication-progress.jsonl"))) == 2
    assert (root / PROJECT / "legacy.jsonl").exists()
    saved = json.loads((archive / "reset.json").read_text())
    assert saved["requested_from"] == CUTOFF.replace(tzinfo=UTC).isoformat()
    assert saved["effective_from"] == plan.effective_from


def test_no_matching_date_is_a_noop(dated):
    root, database, paths, _ = dated
    before = {
        table: snapshot(database, table)
        for table in ("work", "stats", "catalog", "checkpoints")
    }
    assert reset_processing(config, PROJECT, from_date=datetime(2027, 1, 1)) == (
        0,
        None,
    )
    assert not (root / "reset-archives").exists()
    assert all(path.exists() for path in paths.values())
    assert {table: snapshot(database, table) for table in before} == before


def test_cli_preview_shows_expanded_boundary_and_apply_uses_it(dated):
    root, database, _, _ = dated
    before = snapshot(database, "work")
    args = ["admin", "reset", "--project", PROJECT, "--from", "2026-10-27"]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert "2026-10-25T00:00:00+00:00" in result.output
    assert "Selection expanded" in result.output
    assert "3 entries" in result.output
    assert snapshot(database, "work") == before
    assert not (root / "reset-archives").exists()
    result = CliRunner().invoke(cli, [*args, "--apply"])
    assert result.exit_code == 0, result.output
    assert "Queued 3 retained" in result.output


def test_suffix_includes_newer_ids_with_older_creation_dates(dated):
    _, database, _, _ = dated
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute(
            "UPDATE work_items SET created_at='2026-10-24T00:00:00+00:00' WHERE id=5"
        )
    plan = preview_reset(config, PROJECT, from_date=CUTOFF)
    assert plan.first_id == 3
    assert plan.count == 3
    assert plan.effective_from == "2026-10-24T00:00:00+00:00"


def test_unacknowledged_manifest_expands_boundary_even_without_batch_ids(dated):
    _, database, _, _ = dated
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute(
            "UPDATE work_items SET batch_id=NULL,state='pending' WHERE batch_id='cross'"
        )
        connection.execute("DELETE FROM stats WHERE id='cross'")
    assert preview_reset(config, PROJECT, from_date=CUTOFF).first_id == 3


def test_removed_batch_source_blocks_dated_reset_before_any_change(dated):
    root, database, paths, _ = dated
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("DELETE FROM work_items WHERE id=3")
    before = snapshot(database, "work")
    with pytest.raises(ValueError, match="source work was removed"):
        reset_processing(config, PROJECT, from_date=CUTOFF)
    assert snapshot(database, "work") == before
    assert all(path.exists() for path in paths.values())
    assert not (root / "reset-archives").exists()


def test_shared_receipts_are_copied_and_compressed_receipts_are_archived(dated):
    _, _, paths, receipts = dated
    manifest_path = paths["cross"] / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["receipt"] = str(receipts["old"])
    manifest_path.write_text(json.dumps(manifest))
    compressed = receipts["later"].with_suffix(".jsonl.gz")
    compressed.write_bytes(gzip.compress(receipts["later"].read_bytes()))
    receipts["later"].unlink()
    _, archive = reset_processing(config, PROJECT, from_date=CUTOFF)
    assert receipts["old"].exists()
    assert (archive / "receipts/published/old.jsonl").exists()
    assert not compressed.exists()
    assert (
        gzip.decompress((archive / "receipts/published/later.jsonl.gz").read_bytes())
        == b"later receipt\n"
    )


def test_dated_reset_rollback_restores_work_summaries_and_output(dated, monkeypatch):
    _, database, paths, receipts = dated
    before = snapshot(database, "work"), snapshot(database, "stats")
    original = sqlite3.connect

    def connect(*args, **kwargs):
        connection = original(*args, **kwargs)
        connection.set_authorizer(
            lambda action, table, *rest: (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_UPDATE and table == "work_items"
                else sqlite3.SQLITE_OK
            )
        )
        return connection

    with monkeypatch.context() as patch:
        patch.setattr("piddiplatsch.runners.reset.sqlite3.connect", connect)
        with pytest.raises(sqlite3.DatabaseError):
            reset_processing(config, PROJECT, from_date=CUTOFF)
    assert (snapshot(database, "work"), snapshot(database, "stats")) == before
    assert all(path.exists() for path in paths.values())
    assert all(path.exists() for path in receipts.values())


@pytest.mark.parametrize("value", ["all", "last", "2026-99-99"])
def test_reset_rejects_invalid_start_dates(value):
    result = CliRunner().invoke(
        cli, ["admin", "reset", "--project", PROJECT, "--from", value]
    )
    assert result.exit_code == 2
    assert "Invalid value" in result.output


def test_last_seven_calendar_days_uses_utc_creation_date(tmp_path, monkeypatch):
    command = MagicMock()
    monkeypatch.setattr("piddiplatsch.commands.ResetCommand", command)
    result = CliRunner().invoke(
        cli, ["admin", "reset", "--project", PROJECT, "--from", "today-6"]
    )
    assert result.exit_code == 0, result.output
    expected = datetime.combine(
        datetime.now(UTC).date() - timedelta(days=6), datetime.min.time()
    )
    assert command.call_args.kwargs["from_date"] == expected


def test_receipt_referenced_by_another_project_is_preserved(dated):
    root, _, _, receipts = dated
    other = root / "batches/cmip7/published/other"
    other.mkdir(parents=True)
    manifest = other / "manifest.json"
    manifest.write_text(json.dumps(dict(receipt=str(receipts["cross"]))))
    before = manifest.read_bytes()
    _, archive = reset_processing(config, PROJECT, from_date=CUTOFF)
    assert receipts["cross"].exists()
    assert (archive / "receipts/published/cross.jsonl").exists()
    assert manifest.read_bytes() == before


def test_missing_output_still_uses_database_batch_boundary(dated):
    _, _, paths, _ = dated
    (paths["cross"] / "manifest.json").unlink()
    (paths["later"] / "manifest.json").unlink()
    plan = preview_reset(config, PROJECT, from_date=CUTOFF)
    assert plan.first_id == 3
    assert plan.count == 3


def test_active_harvest_prevents_dated_reset(dated):
    root, database, _, _ = dated
    before = snapshot(database, "work")
    with (
        closing(CatalogHarvester(root)),
        pytest.raises(RuntimeError, match="Harvest is running"),
    ):
        reset_processing(config, PROJECT, from_date=CUTOFF)
    assert snapshot(database, "work") == before
