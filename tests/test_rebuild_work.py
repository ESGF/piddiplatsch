import base64
import gzip
import json
import sqlite3
import zlib
from contextlib import closing
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.exceptions import CatalogError
from piddiplatsch.monitoring.status import read_status
from piddiplatsch.runners import rebuild
from piddiplatsch.runners.clean import clean_outbox
from piddiplatsch.runners.sync import SyncWorker, _worker_lock


def records(path):
    with gzip.open(path, "rt") as stream:
        result = [json.loads(line) for line in stream]
    for row in result:
        for column in ("stac_blob", "manifest", "payload"):
            if column in row:
                row[column] = base64.b64decode(row[column])
    return result


def rows(database, table):
    with closing(sqlite3.connect(database)) as connection:
        connection.row_factory = sqlite3.Row
        return [
            dict(row)
            for row in connection.execute(
                {
                    "work_items": "SELECT * FROM work_items ORDER BY id",
                    "catalog_items": "SELECT * FROM catalog_items ORDER BY project",
                    "kafka_checkpoints": "SELECT * FROM kafka_checkpoints ORDER BY topic,partition",
                    "stats": "SELECT * FROM stats",
                    "outbox_batches": "SELECT * FROM outbox_batches ORDER BY project,batch_id",
                    "handle_outbox": "SELECT * FROM handle_outbox ORDER BY project,batch_id,position",
                }[table]
            )
        ]


@pytest.fixture
def seeded(tmp_path, publication_sample):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("catalog", "directory", str(tmp_path))
    with closing(CatalogHarvester(tmp_path)) as harvester:
        for offset, project in enumerate(("cmip6", "cmip7", "cmip6", "cmip7")):
            message = publication_sample(project)
            message["data"]["payload"]["item"]["properties"]["latest"] = offset
            harvester.process_with_source(
                "key", message, KafkaSource("topic", 0, offset)
            )
    database = tmp_path / "piddi.db"
    with SQLiteCatalog("cmip6", database) as catalog:
        catalog.seal_batch("old", [dict(id=1, error=None, handle_count=2)])
        catalog.publish_batch("old", 2)
        catalog.seal_batch("old-latest", [dict(id=3, error=None, handle_count=3)])
        catalog.publish_batch("old-latest", 3)
        catalog._execute(
            "UPDATE catalog_items SET last_update=CASE project WHEN 'cmip7' THEN '2026-11-01T00:00:00+00:00' ELSE '2026-11-02T00:00:00+00:00' END"
        )
    with SQLiteCatalog("cmip7", database) as catalog:
        catalog.seal_batch(
            "old-cmip7",
            [
                dict(id=2, error=None, handle_count=4),
                dict(id=4, error=None, handle_count=5),
            ],
        )
        catalog.publish_batch("old-cmip7", 9)
    for filename in (
        "batches/cmip6/published/old/manifest.json",
        "cmip7/published/receipt.jsonl",
    ):
        path = tmp_path / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("saved output\n")
    return tmp_path, database


@pytest.fixture
def finished_outbox(seeded):
    root, database = seeded
    with closing(sqlite3.connect(database)) as connection, connection:
        for project, batch_id, first_id in (
            ("cmip6", "old", 1),
            ("cmip7", "old-cmip7", 2),
        ):
            connection.execute(
                "INSERT INTO outbox_batches(project,batch_id,first_work_id,status,manifest) VALUES (?,?,?,'published',?)",
                (
                    project,
                    batch_id,
                    first_id,
                    zlib.compress(
                        json.dumps(
                            dict(project=project, batch_id=batch_id, status="published")
                        ).encode()
                    ),
                ),
            )
        connection.execute(
            "INSERT INTO handle_outbox(project,batch_id,position,handle,payload,published) VALUES ('cmip6','old',1,'21.TEST/done',?,1)",
            (zlib.compress(b'{"handle":"21.TEST/done"}'),),
        )
    return root, database


@pytest.mark.parametrize("vacuum", [False, True])
def test_clean_only_removes_completed_outbox(finished_outbox, vacuum):
    root, database = finished_outbox
    tables = ("work_items", "catalog_items", "kafka_checkpoints", "stats")
    before = {table: rows(database, table) for table in tables}
    files = {path: path.read_bytes() for path in root.rglob("*.json*")}
    result = CliRunner().invoke(
        cli, ["admin", "clean", "--apply"] + (["--vacuum"] if vacuum else [])
    )
    assert result.exit_code == 0, result.output
    assert "Cleared outbox: 2 batch records, 1 Handle rows." in result.output
    assert {table: rows(database, table) for table in tables} == before
    assert rows(database, "outbox_batches") == rows(database, "handle_outbox") == []
    assert {path: path.read_bytes() for path in files} == files
    assert not (root / "work-rebuild-archives").exists()
    assert clean_outbox(config) == dict(outbox_batches=0, handle_outbox=0)


@pytest.mark.parametrize("reason", ["ready", "unacknowledged", "export"])
def test_clean_preserves_records_needed_for_recovery(finished_outbox, reason):
    root, database = finished_outbox
    with closing(sqlite3.connect(database)) as connection, connection:
        if reason == "ready":
            connection.execute(
                "UPDATE outbox_batches SET status='ready' WHERE project='cmip6'"
            )
        elif reason == "unacknowledged":
            connection.execute("UPDATE handle_outbox SET published=0")
        else:
            (root / "batches/cmip6/ready/old").mkdir(parents=True)
        # Cleaning must work even with a pending event elsewhere.
        connection.execute("UPDATE work_items SET state='pending' WHERE id=4")
    protected = rows(database, "handle_outbox")
    assert clean_outbox(config) == dict(outbox_batches=1, handle_outbox=0)
    assert rows(database, "handle_outbox") == protected
    assert [row["project"] for row in rows(database, "outbox_batches")] == ["cmip6"]
    assert rows(database, "work_items")[-1]["state"] == "pending"


@pytest.mark.parametrize("owner", ["harvest", "worker", "maintenance", "rebuild"])
def test_clean_refuses_active_owners_and_incomplete_rebuild(finished_outbox, owner):
    from contextlib import nullcontext

    from piddiplatsch.catalog.ownership import harvest_lock
    from piddiplatsch.output_maintenance import output_maintenance_lock

    root, database = finished_outbox
    locks = {
        "harvest": harvest_lock,
        "worker": _worker_lock,
        "maintenance": output_maintenance_lock,
    }
    if owner == "rebuild":
        (root / ".work-rebuild.json").write_text("{}")
    before = rows(database, "outbox_batches")
    with locks[owner](root) if owner in locks else nullcontext():
        with pytest.raises((RuntimeError, CatalogError)):
            clean_outbox(config)
    assert rows(database, "outbox_batches") == before


def test_clean_transaction_rolls_back_all_deletes(finished_outbox, monkeypatch):
    _, database = finished_outbox
    before = {
        table: rows(database, table) for table in ("handle_outbox", "outbox_batches")
    }
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute(
            "CREATE TRIGGER block_clean BEFORE DELETE ON outbox_batches BEGIN SELECT RAISE(ABORT, 'blocked'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="blocked"):
        clean_outbox(config)
    assert {table: rows(database, table) for table in before} == before


def test_clean_accepts_schema_four_without_outbox(seeded):
    _, database = seeded
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("DROP TABLE handle_outbox")
        connection.execute("DROP TABLE outbox_batches")
        connection.execute("PRAGMA user_version=4")
    assert clean_outbox(config, vacuum=True) == dict(outbox_batches=0, handle_outbox=0)


@pytest.mark.parametrize("vacuum", [False, True])
def test_rebuild_archives_and_clears_finished_outbox_without_dropping_tables(
    finished_outbox, vacuum
):
    _, database = finished_outbox
    tables = ("outbox_batches", "handle_outbox")
    before = {table: rows(database, table) for table in tables}
    totals = read_status(database)["handle_totals"]
    result = rebuild.rebuild_work(config, vacuum=vacuum)
    archive = Path(result["archive"])
    assert result["cleared_outbox"] == {"outbox_batches": 2, "handle_outbox": 1}
    assert result["outbox_blob_encoding"] == "base64"
    for table in tables:
        assert records(archive / f"{table}.jsonl.gz") == before[table]
        assert rows(database, table) == []
    assert read_status(database)["handle_totals"] == totals
    with closing(sqlite3.connect(database)) as connection, connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 5
    # The schema remains immediately usable; no Handle work is requeued.
    publisher = MagicMock()
    worker = SyncWorker(projects=("cmip6", "cmip7"), publisher=publisher)
    worker.run(force_map=True)
    assert not worker.errors
    publisher.run.assert_not_called()


@pytest.mark.parametrize("unfinished", ["batch", "handle"])
def test_unfinished_outbox_blocks_cleanup_even_if_work_and_stats_are_published(
    finished_outbox, unfinished
):
    root, database = finished_outbox
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute(
            {
                "batch": "UPDATE outbox_batches SET status='ready' WHERE project='cmip6'",
                "handle": "UPDATE handle_outbox SET published=0",
            }[unfinished]
        )
    before = {
        table: rows(database, table)
        for table in ("work_items", "stats", "outbox_batches", "handle_outbox")
    }
    result = CliRunner().invoke(cli, ["admin", "rebuild", "--apply", "--vacuum"])
    assert result.exit_code == 1
    assert "unfinished outbox" in result.output
    assert not (root / "work-rebuild-archives").exists()
    for table, expected in before.items():
        assert rows(database, table) == expected


@pytest.mark.parametrize("table", ["outbox_batches", "handle_outbox"])
def test_outbox_export_failure_preserves_queue_and_original_work(
    finished_outbox, monkeypatch, table
):
    root, database = finished_outbox
    before = {
        name: rows(database, name)
        for name in ("work_items", "stats", "outbox_batches", "handle_outbox")
    }
    export = rebuild._export

    def fail_outbox(connection, name, destination, guard):
        if name == table:
            raise OSError("outbox archive unavailable")
        return export(connection, name, destination, guard)

    monkeypatch.setattr(rebuild, "_export", fail_outbox)
    with pytest.raises(OSError, match="outbox archive"):
        rebuild.rebuild_work(config)
    for name, expected in before.items():
        assert rows(database, name) == expected
    assert (root / "batches/cmip6/published/old/manifest.json").exists()


def test_rebuild_still_accepts_version_four_without_outbox_tables(seeded):
    _, database = seeded
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("DROP TABLE handle_outbox")
        connection.execute("DROP TABLE outbox_batches")
        connection.execute("PRAGMA user_version=4")
    result = rebuild.rebuild_work(config)
    assert result["cleared_outbox"] == {}
    assert len(rows(database, "work_items")) == 2
    with closing(sqlite3.connect(database)) as connection, connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 4


def test_rebuild_preserves_history_catalog_checkpoints_and_published_latest_in_order(
    seeded,
):
    root, database = seeded
    before = {
        table: rows(database, table)
        for table in ("work_items", "catalog_items", "kafka_checkpoints", "stats")
    }
    totals = read_status(database)["handle_totals"]
    result = rebuild.rebuild_work(config)
    archive = Path(result["archive"])
    assert result["previous_work_entries"] == 4
    assert result["catalog_entries"] == 2
    assert records(archive / "work_items.jsonl.gz") == before["work_items"]
    assert records(archive / "stats.jsonl.gz") == before["stats"]
    assert rows(database, "catalog_items") == before["catalog_items"]
    assert rows(database, "kafka_checkpoints") == before["kafka_checkpoints"]
    assert read_status(database)["handle_totals"] == totals
    work = rows(database, "work_items")
    assert [row["project"] for row in work] == ["cmip7", "cmip6"]
    assert all(
        row["id"] > 4 and row["state"] == "published" and row["event_type"] == "full"
        for row in work
    )
    assert [row["offset"] for row in work] == [0, 1]
    assert all(row["topic"] == "piddi-catalog-rebuild:" + result["id"] for row in work)
    assert all(row["batch_id"] is None and row["handle_count"] == 0 for row in work)
    expected = {row["project"]: row["stac_blob"] for row in before["catalog_items"]}
    assert all(row["stac_blob"] == expected[row["project"]] for row in work)
    assert rows(database, "stats")[:-1] == before["stats"]
    assert rows(database, "stats")[-1]["status"] == "work_rebuilt"
    assert len(list(archive.rglob("manifest.json"))) == 1
    assert len(list(archive.rglob("receipt.jsonl"))) == 1
    assert not (root / "batches").exists()
    assert not (root / ".work-rebuild.json").exists()
    assert json.loads((archive / "rebuild.json").read_text())["status"] == "completed"
    # Normal harvesting continues at the real checkpoint, not synthetic offsets.
    with closing(CatalogHarvester(root)) as harvester:
        assert harvester.kafka_resume_offset("topic", 0) == 4
    publisher = MagicMock()
    worker = SyncWorker(projects=("cmip6", "cmip7"), publisher=publisher)
    assert worker.run(force_map=True) == []
    publisher.run.assert_not_called()


@pytest.mark.parametrize("state", ["pending", "mapped", "invalid"])
def test_unfinished_events_block_rebuild_before_archiving_or_mutation(seeded, state):
    root, database = seeded
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("UPDATE work_items SET state=? WHERE id=4", (state,))
    before = {
        table: rows(database, table)
        for table in ("work_items", "catalog_items", "kafka_checkpoints", "stats")
    }
    result = CliRunner().invoke(cli, ["admin", "rebuild", "--apply", "--vacuum"])
    assert result.exit_code == 1
    assert f"{state}=1" in result.output
    assert not (root / "work-rebuild-archives").exists()
    assert not (root / ".work-rebuild.json").exists()
    assert (root / "batches/cmip6/published/old/manifest.json").exists()
    for table, expected in before.items():
        assert rows(database, table) == expected


@pytest.mark.parametrize("status", ["ready", "publication_failed"])
def test_unfinished_batch_blocks_rebuild_even_if_work_rows_say_published(
    seeded, status
):
    root, database = seeded
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("UPDATE stats SET status=? WHERE id='old'", (status,))
    with pytest.raises(ValueError, match=status):
        rebuild.rebuild_work(config)
    assert not (root / "work-rebuild-archives").exists()


def test_insert_failure_rolls_back_database_and_restores_batches(
    finished_outbox, monkeypatch
):
    root, database = finished_outbox
    before = rows(database, "work_items")
    outbox = {
        table: rows(database, table) for table in ("outbox_batches", "handle_outbox")
    }
    original = sqlite3.connect

    def connect(*args, **kwargs):
        connection = original(*args, **kwargs)
        connection.set_authorizer(
            lambda action, table, *rest: (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_INSERT and table == "work_items"
                else sqlite3.SQLITE_OK
            )
        )
        return connection

    with monkeypatch.context() as patch:
        patch.setattr(rebuild.sqlite3, "connect", connect)
        with pytest.raises(sqlite3.DatabaseError):
            rebuild.rebuild_work(config)
    assert rows(database, "work_items") == before
    for table, expected in outbox.items():
        assert rows(database, table) == expected
    assert (
        root / "batches/cmip6/published/old/manifest.json"
    ).read_text() == "saved output\n"
    assert not (root / ".work-rebuild.json").exists()


def test_export_failure_never_replaces_work(seeded, monkeypatch):
    root, database = seeded
    before = rows(database, "work_items")

    def fail(*args):
        raise OSError("archive disk full")

    monkeypatch.setattr(rebuild, "_export", fail)
    with pytest.raises(OSError, match="disk full"):
        rebuild.rebuild_work(config)
    assert rows(database, "work_items") == before
    assert (root / "batches/cmip6/published/old/manifest.json").exists()


def test_committed_rebuild_finalizes_after_interruption_without_rebuilding_again(
    finished_outbox, monkeypatch
):
    root, database = finished_outbox

    def interrupted(*args):
        raise RuntimeError("interrupted after commit")

    with monkeypatch.context() as patch:
        patch.setattr(rebuild, "_finish", interrupted)
        with pytest.raises(RuntimeError, match="after commit"):
            rebuild.rebuild_work(config)
    before = rows(database, "work_items")
    with pytest.raises(CatalogError, match="rebuild is incomplete"):
        CatalogHarvester(root)
    with pytest.raises(CatalogError, match="rebuild is incomplete"):
        SyncWorker(projects=("cmip6",)).run(force_map=True)
    result = rebuild.rebuild_work(config)
    assert rows(database, "work_items") == before
    assert len(list((root / "work-rebuild-archives").iterdir())) == 1
    assert result["catalog_entries"] == 2
    assert rows(database, "outbox_batches") == rows(database, "handle_outbox") == []
    assert not (root / ".work-rebuild.json").exists()


def test_interrupted_output_rollback_is_recovered_before_retry(seeded, monkeypatch):
    root, database = seeded
    original_move = rebuild._move
    moves = []

    def interrupted_move(source, destination):
        original_move(source, destination)
        moves.append(source)
        raise RuntimeError("interrupted while moving output")

    def interrupted_restore(plan):
        raise RuntimeError("rollback interrupted too")

    before = rows(database, "work_items")
    with monkeypatch.context() as patch:
        patch.setattr(rebuild, "_move", interrupted_move)
        patch.setattr(rebuild, "_restore", interrupted_restore)
        with pytest.raises(RuntimeError, match="rollback interrupted"):
            rebuild.rebuild_work(config)
    assert moves
    assert (root / ".work-rebuild.json").exists()
    assert rows(database, "work_items") == before
    assert not moves[0].exists()
    # Recovery first restores the old output, then starts a fresh transaction.
    result = rebuild.rebuild_work(config)
    assert len(rows(database, "work_items")) == 2
    assert len(list(Path(result["archive"]).rglob("manifest.json"))) == 1
    assert any(
        json.loads(path.read_text())["status"] == "rolled_back"
        for path in (root / "work-rebuild-archives").glob("*/rebuild.json")
    )
    assert not (root / ".work-rebuild.json").exists()


@pytest.mark.parametrize("owner", ["harvest", "worker"])
def test_rebuild_refuses_active_owners(seeded, owner):
    root, database = seeded
    before = rows(database, "work_items")
    lock = closing(CatalogHarvester(root)) if owner == "harvest" else _worker_lock(root)
    with lock, pytest.raises(RuntimeError, match="running"):
        rebuild.rebuild_work(config)
    assert rows(database, "work_items") == before


def test_vacuum_reclaims_space_after_rebuild(finished_outbox):
    _, database = finished_outbox
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.executemany(
            "INSERT INTO handle_outbox(project,batch_id,position,handle,payload,published) "
            "VALUES('cmip6','old',?,'21.TEST/done',zeroblob(16000),1)",
            [(i,) for i in range(2, 66)],
        )
    before = database.stat().st_size
    result = rebuild.rebuild_work(config, vacuum=True)
    assert result["vacuum"]
    assert database.stat().st_size < before
    with closing(sqlite3.connect(database)) as connection, connection:
        assert connection.execute("PRAGMA freelist_count").fetchone()[0] == 0


def test_cli_previews_without_creating_database(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    result = CliRunner().invoke(cli, ["admin", "rebuild", "--vacuum"])
    assert result.exit_code == 0, result.output
    assert "Preview only" in result.output
    assert "VACUUM: reclaim" in result.output
    assert not (tmp_path / "piddi.db").exists()


def test_cli_apply(finished_outbox):
    result = CliRunner().invoke(cli, ["admin", "rebuild", "--apply"])
    assert result.exit_code == 0, result.output
    assert "4 old work entries as 2 published" in result.output
    assert "Cleared outbox: 2 batch records, 1 Handle rows." in result.output
