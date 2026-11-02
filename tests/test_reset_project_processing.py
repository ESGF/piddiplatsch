"""Project replay updates work in place without copying the shared database."""

import json
from contextlib import closing

import pytest
from click.testing import CliRunner

from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.runners.reset import reset_processing
from piddiplatsch.runners.sync import _worker_lock


def seed(tmp_path, testdata_path):
    config._set("consumer", "output_dir", str(tmp_path))
    with closing(CatalogHarvester(tmp_path)) as harvester:
        for offset, project in enumerate(("cmip6plus", "cmip7")):
            message = json.loads(
                (testdata_path / "publication_samples" / f"{project}.json").read_text()
            )
            harvester.process_with_source(
                "seed", message, KafkaSource("topic", 0, offset)
            )
    with SQLiteCatalog("cmip6plus", tmp_path / "piddi.db") as writer:
        writer.seal_batch("saved", [{"id": 1, "error": None, "handle_count": 2}])
        writer.publish_batch("saved", 2)
    old = tmp_path / "batches/cmip6plus/published/saved"
    old.mkdir(parents=True)
    (old / "handles.jsonl").write_text("saved output\n")


def test_reset_requeues_retained_snapshots_without_touching_shared_checkpoints(
    tmp_path, testdata_path
):
    seed(tmp_path, testdata_path)
    count, archive = reset_processing(config, "cmip6plus")
    assert count == 1
    with SQLiteCatalog("cmip6plus", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader.get_checkpoint("topic", 0).next_offset == 2
        assert len(list(reader.iter_items())) == 1
        assert reader.pending_work().count == 1
        assert reader._execute(
            "SELECT batch_id,message,handle_count FROM work_items WHERE project='cmip6plus'"
        ).fetchone() == (None, None, 0)
        assert reader._execute(
            "SELECT state FROM work_items WHERE project='cmip7'"
        ).fetchone() == ("pending",)
        assert (
            reader._execute(
                "SELECT COUNT(*) FROM stats WHERE project='cmip6plus'"
            ).fetchone()[0]
            == 0
        )
    assert archive.parent == tmp_path / "reset-archives"
    assert not list(archive.rglob("*.db"))
    assert (
        archive / "output-0/published/saved/handles.jsonl"
    ).read_text() == "saved output\n"
    assert not (tmp_path / "batches/cmip6plus").exists()


def test_reset_respects_worker_ownership(tmp_path, testdata_path):
    seed(tmp_path, testdata_path)
    with _worker_lock(tmp_path) as acquired:
        assert acquired
        with pytest.raises(RuntimeError, match="worker"):
            reset_processing(config, "cmip6plus")
    with SQLiteCatalog("cmip6plus", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader.pending_work().count == 0


def test_reset_cli_defaults_to_preview(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    result = CliRunner().invoke(
        cli, ["--silent", "admin", "reset", "--project", "cmip6plus"]
    )
    assert result.exit_code == 0, result.output
    assert "Preview only" in result.output
    assert not (tmp_path / "piddi.db").exists()
