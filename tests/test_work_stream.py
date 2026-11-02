"""Cross-project scheduling, archive replay, retention, and conservative retries."""

import json
from contextlib import closing
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from piddiplatsch.catalog import CatalogError
from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.consumer import HarvestProcessor
from piddiplatsch.core.messages import KafkaSource, SourceMessage
from piddiplatsch.core.pipeline import ProcessingPipeline
from piddiplatsch.maintenance import clean_published_work
from piddiplatsch.result import PublishResult
from piddiplatsch.runners.sync import SyncWorker


def seed(tmp_path, publication_sample, projects=("cmip6", "cmip7"), count=3):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("consumer", "projects", list(projects))
    config._set("batch", None, {"max_items": 2, "trigger_items": 1})
    offset = 0
    with closing(CatalogHarvester(tmp_path)) as harvester:
        for project in projects:
            for version in range(count):
                message = publication_sample(project, version=version + 1)
                harvester.process_with_source(
                    "seed", message, KafkaSource("topic", 0, offset)
                )
                offset += 1


class Publisher:
    def __init__(self, failed_project=None):
        self.calls = []
        self.batches = []
        self.failed_project = failed_project

    def run(self, paths, **kwargs):
        project = kwargs["project"]
        self.calls.append(project)
        manifest = json.loads((paths[0].parent / "manifest.json").read_text())
        self.batches.append((project, [entry["id"] for entry in manifest["outcomes"]]))
        count = len(paths[0].read_text().splitlines())
        failed = count if project == self.failed_project else 0
        for _, position, record in kwargs.get("record_source", ()):
            kwargs["journal"].record(
                SimpleNamespace(
                    line_number=position,
                    handle=record["handle"],
                    error=RuntimeError("test publication failed") if failed else None,
                )
            )
        return PublishResult(total=count, succeeded=count - failed, failed=failed)


def test_weighted_project_batches_preserve_each_projects_order(
    tmp_path, publication_sample
):
    seed(tmp_path, publication_sample)
    publisher = Publisher()
    worker = SyncWorker(publisher=publisher)
    assert len(worker.run(force_map=True)) == 4
    assert not worker.errors
    assert sorted(publisher.calls) == ["cmip6", "cmip6", "cmip7", "cmip7"]
    for project, expected in (("cmip6", [1, 2, 3]), ("cmip7", [4, 5, 6])):
        assert [
            identity
            for name, ids in publisher.batches
            if name == project
            for identity in ids
        ] == expected
    with SQLiteCatalog("cmip7", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader.get_checkpoint("topic", 0).next_offset == 6
        assert (
            reader._execute(
                "SELECT COUNT(*) FROM work_items WHERE state='published'"
            ).fetchone()[0]
            == 6
        )


def test_filtered_mapping_cannot_interleave_batches_and_replay_older_updates(
    tmp_path, publication_sample
):
    from piddiplatsch.runners.mapping import map_catalog

    seed(tmp_path, publication_sample, projects=("cmip7",))
    assert map_catalog(projects=["cmip7"], offset=1, limit=1).succeeded == 1
    assert map_catalog(projects=["cmip7"]).succeeded == 2
    publisher = Publisher()
    worker = SyncWorker(publisher=publisher)
    worker.run()
    assert not worker.errors
    assert [ids for _, ids in publisher.batches] == [[1], [2], [3]]


def test_failed_project_does_not_block_other_projects(tmp_path, publication_sample):
    seed(tmp_path, publication_sample)
    publisher = Publisher(failed_project="cmip6")
    worker = SyncWorker(publisher=publisher)
    worker.run(force_map=True)
    assert worker.errors
    assert sorted(publisher.calls) == ["cmip6", "cmip7", "cmip7"]
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader.pending_work().count == 1
        assert (
            reader._execute(
                "SELECT COUNT(*) FROM work_items WHERE project='cmip7' AND state='published'"
            ).fetchone()[0]
            == 3
        )
        assert reader._execute(
            "SELECT message FROM work_items WHERE project='cmip6' AND state='mapped' LIMIT 1"
        ).fetchone()[0]


def test_cleanup_uses_published_timestamp_and_preserves_summaries(
    tmp_path, publication_sample
):
    seed(tmp_path, publication_sample, projects=("cmip6",), count=4)
    path = tmp_path / "piddi.db"
    with SQLiteCatalog("cmip6", path) as writer:
        writer.seal_batch("old", [{"id": 1, "error": None, "handle_count": 2}])
        writer.publish_batch("old", 2)
        writer.seal_batch("recent", [{"id": 2, "error": None, "handle_count": 2}])
        writer.publish_batch("recent", 2)
        writer.seal_batch("invalid", [{"id": 3, "error": "bad mapping"}])
        expired = (datetime.now(UTC) - timedelta(days=31)).isoformat()
        with writer.transaction():
            writer._execute("UPDATE work_items SET created_at=?", (expired,))
            writer._execute(
                "UPDATE work_items SET updated_at=? WHERE id IN (1,3,4)", (expired,)
            )
        before = writer._execute("SELECT * FROM stats").fetchall()
    # Retain even expired published work until cleanup is explicitly enabled.
    assert clean_published_work(dry_run=True) == 0
    assert clean_published_work() == 0
    with SQLiteCatalog("cmip6", path, read_only=True) as reader:
        assert reader._execute("SELECT COUNT(*) FROM work_items").fetchone()[0] == 4
    config._set("batch", "retention_days", 30)
    assert clean_published_work(dry_run=True) == 1
    assert clean_published_work() == 1
    assert clean_published_work() == 0
    with SQLiteCatalog("cmip6", path, read_only=True) as reader:
        assert reader._execute(
            "SELECT id,state FROM work_items ORDER BY id"
        ).fetchall() == [(2, "published"), (3, "invalid"), (4, "pending")]
        assert reader._execute("SELECT * FROM stats").fetchall() == before
        assert len(list(reader.iter_items())) == 4


def test_archive_replay_preserves_coordinates_and_is_idempotent(
    tmp_path, publication_sample
):
    config._set("consumer", "output_dir", str(tmp_path))
    original = publication_sample("cmip6")
    with closing(HarvestProcessor(catalog_dir=tmp_path)) as processor:
        pipeline = ProcessingPipeline(
            [SourceMessage("key", original, KafkaSource("topic", 2, 12))],
            processor,
            dump_messages=True,
        )
        pipeline.run()
    archive = next((tmp_path / "dump").glob("*.jsonl"))
    record = json.loads(archive.read_text())
    assert record == {
        "source": {"topic": "topic", "partition": 2, "offset": 12},
        "key": "key",
        "payload": original,
    }
    replay_root = tmp_path / "replay"
    config._set("catalog", "directory", str(replay_root))
    for _ in range(2):
        result = CliRunner().invoke(
            cli, ["--silent", "harvest", "--archive", str(archive)]
        )
        assert result.exit_code == 0, result.output
    with SQLiteCatalog("cmip6", replay_root / "piddi.db", read_only=True) as reader:
        assert reader.pending_work().count == 1
        assert reader.get_checkpoint("topic", 2).next_offset == 13


def test_retry_preserves_snapshot_and_refuses_stale_publication(
    tmp_path, publication_sample
):
    seed(tmp_path, publication_sample, projects=("cmip6",), count=2)
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db") as writer:
        original = writer._execute(
            "SELECT stac_blob FROM work_items WHERE id=1"
        ).fetchone()[0]
        writer.seal_batch("invalid", [{"id": 1, "error": "software bug"}])
        writer.retry_invalid([1])
        assert writer._execute(
            "SELECT state,stac_blob,batch_id FROM work_items WHERE id=1"
        ).fetchone() == ("pending", original, None)
        writer.seal_batch("invalid-again", [{"id": 1, "error": "software bug"}])
        writer.seal_batch("newer", [{"id": 2, "error": None, "handle_count": 2}])
        with pytest.raises(CatalogError, match="newer snapshot"):
            writer.retry_invalid([1])
        writer.publish_batch("newer", 2)
        with pytest.raises(CatalogError, match="published output"):
            writer.retry_invalid([1])


def test_missing_saved_output_blocks_newer_mapping(tmp_path, publication_sample):
    seed(tmp_path, publication_sample, projects=("cmip6",), count=2)
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db") as writer:
        writer.seal_batch("missing", [{"id": 1, "error": None, "handle_count": 2}])
    publisher = Publisher()
    worker = SyncWorker(publisher=publisher)
    assert worker.run(force_map=True) == []
    assert "output is missing" in worker.errors[0]
    assert not publisher.calls


def test_map_offset_stays_skipped_across_batch_rounds(tmp_path, publication_sample):
    from piddiplatsch.runners.mapping import map_catalog

    seed(tmp_path, publication_sample, projects=("cmip6",), count=7)
    assert map_catalog(projects=["cmip6"], offset=2, limit=5).succeeded == 5
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader._execute(
            "SELECT id FROM work_items WHERE state='pending' ORDER BY id"
        ).fetchall() == [(1,), (2,)]


def test_publication_exception_keeps_saved_output_and_records_failure(
    tmp_path, publication_sample
):
    seed(tmp_path, publication_sample, projects=("cmip6",), count=2)

    class BrokenPublisher:
        def run(self, *args, **kwargs):
            raise OSError("receipt disk failed")

    worker = SyncWorker(publisher=BrokenPublisher())
    worker.run(force_map=True)
    assert worker.errors
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader._execute(
            "SELECT DISTINCT state,message FROM work_items"
        ).fetchall() == [("mapped", "receipt disk failed")]
        assert reader._execute("SELECT errors FROM stats").fetchone() == (1,)


def test_weighted_saved_publication_backlog_preserves_each_projects_order(
    tmp_path, publication_sample
):
    from piddiplatsch.runners.mapping import map_catalog

    seed(tmp_path, publication_sample)
    assert map_catalog(projects=["cmip6", "cmip7"], limit=6).succeeded == 6
    publisher = Publisher()
    worker = SyncWorker(publisher=publisher)
    assert worker.run() == []
    assert not worker.errors
    assert sorted(publisher.calls) == ["cmip6", "cmip6", "cmip7", "cmip7"]
    for project, expected in (("cmip6", [1, 2, 3]), ("cmip7", [4, 5, 6])):
        assert [
            identity
            for name, ids in publisher.batches
            if name == project
            for identity in ids
        ] == expected
