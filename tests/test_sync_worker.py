"""Durable pending changes, sealed batches, triggers, and publication recovery."""

import json
from contextlib import closing
from copy import deepcopy
from datetime import timedelta

import pytest
from click.testing import CliRunner

from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.result import PublishResult
from piddiplatsch.runners.sync import SyncWorker, _worker_lock


class Publisher:
    def __init__(self):
        self.calls = []
        self.options = []
        self.fail = False

    def run(self, paths, **kwargs):
        path = paths[0]
        manifest = json.loads((path.parent / "manifest.json").read_text())
        assert manifest["status"] == "ready"
        assert manifest["mapping_acknowledged"] is True
        records = [json.loads(line) for line in path.read_text().splitlines()]
        assert len(records) == manifest["handle_count"]
        assert all(record["project"] == kwargs["project"] for record in records)
        self.calls.append(path)
        self.options.append(kwargs)
        return PublishResult(
            total=len(records),
            succeeded=0 if self.fail else len(records),
            failed=len(records) if self.fail else 0,
        )


@pytest.fixture
def setup(tmp_path, testdata_path):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("consumer", "projects", ["cmip7"])
    config._set(
        "batch", None, {"trigger_items": 2, "max_items": 2, "max_wait_seconds": 3600}
    )
    sample = json.loads((testdata_path / "publication_samples/cmip7.json").read_text())
    directory = tmp_path / "catalog"
    messages = []

    def put(version, *, extra=None):
        message = deepcopy(sample)
        item = message["data"]["payload"]["item"]
        item["id"] = item["id"].rsplit(".", 1)[0] + f".v{version}"
        if extra:
            item["properties"].update(extra)
        with closing(CatalogHarvester(directory)) as harvester:
            harvester.process_with_source(
                "key", message, KafkaSource("topic", 0, len(messages))
            )
        messages.append(message)
        return message

    return put, directory, tmp_path


def pending(directory):
    with SQLiteCatalog("cmip7", directory / "cmip7.sqlite", read_only=True) as reader:
        return reader.pending_changes()


def manifests(root):
    return [
        (path.parent, json.loads(path.read_text()))
        for path in sorted(root.glob("batches/cmip7/*/*/manifest.json"))
        if not path.parent.name.startswith(".")
    ]


def test_count_trigger_seals_and_publishes_then_empty_tick_does_nothing(setup):
    put, directory, root = setup
    put(1)
    publisher = Publisher()
    assert SyncWorker(publisher=publisher).run() == []
    assert not publisher.calls
    put(2)
    worker = SyncWorker(publisher=publisher)
    paths = worker.run()
    assert not worker.errors
    assert len(paths) == len(publisher.calls) == 1
    batch, manifest = manifests(root)[0]
    assert manifest["status"] == "published"
    assert manifest["item_count"] == 2
    assert manifest["sha256"]
    assert pending(directory).count == 0
    assert not list(root.glob("cmip7/handles/handles_*.jsonl"))
    original = (batch / "handles.jsonl").read_bytes()
    assert SyncWorker(publisher=publisher).run() == []
    assert len(publisher.calls) == 1
    assert (batch / "handles.jsonl").read_bytes() == original


def test_hour_trigger_uses_first_pending_change_and_coalesces_patches(setup):
    put, directory, root = setup
    put(1)
    first = pending(directory)
    put(1, extra={"custom:change": 2})
    updated = pending(directory)
    assert updated.count == 1
    assert updated.oldest_at == first.oldest_at
    assert updated.sequence > first.sequence
    publisher = Publisher()
    assert (
        SyncWorker(
            now=lambda: first.oldest_at + timedelta(seconds=3599), publisher=publisher
        ).run()
        == []
    )
    worker = SyncWorker(
        now=lambda: first.oldest_at + timedelta(hours=1), publisher=publisher
    )
    assert len(worker.run()) == 1
    assert not worker.errors
    assert manifests(root)[0][1]["item_count"] == 1


def test_batch_boundaries_keep_all_handles_for_each_item(setup):
    put, directory, root = setup
    for version in range(1, 6):
        put(version)
    publisher = Publisher()
    assert len(SyncWorker(publisher=publisher).run()) == 2
    assert pending(directory).count == 1
    assert SyncWorker(publisher=publisher).run() == []
    assert len(SyncWorker(publisher=publisher).run(force_map=True)) == 1
    assert sorted(manifest["item_count"] for _, manifest in manifests(root)) == [
        1,
        2,
        2,
    ]
    assert pending(directory).count == 0


def test_changes_during_mapping_remain_pending(setup, monkeypatch):
    put, directory, root = setup
    put(1)
    put(2)
    worker = SyncWorker(publisher=Publisher())
    original = worker._map_items

    def change_after_snapshot(*args):
        original(*args)
        put(1, extra={"custom:arrived-during-map": True})

    monkeypatch.setattr(worker, "_map_items", change_after_snapshot)
    worker.run()
    assert not worker.errors
    assert pending(directory).count == 1
    assert manifests(root)[0][1]["item_count"] == 2
    with SQLiteCatalog("cmip7", directory / "cmip7.sqlite", read_only=True) as reader:
        assert (
            next(reader.iter_pending(10)).entry.item["properties"][
                "custom:arrived-during-map"
            ]
            is True
        )


def test_failed_publication_retries_immutable_batch_before_new_mapping(setup):
    put, directory, root = setup
    put(1)
    put(2)
    publisher = Publisher()
    publisher.fail = True
    worker = SyncWorker(publisher=publisher)
    worker.run()
    assert worker.errors
    batch, manifest = manifests(root)[0]
    assert manifest["status"] == "ready"
    assert pending(directory).count == 0
    original = (batch / "handles.jsonl").read_bytes()
    put(3)
    put(4)
    failed_again = SyncWorker(publisher=publisher)
    assert failed_again.run() == []
    assert failed_again.errors
    assert len(manifests(root)) == 1
    assert pending(directory).count == 2
    assert (batch / "handles.jsonl").read_bytes() == original
    publisher.fail = False
    recovered = SyncWorker(publisher=publisher)
    recovered.run()
    assert not recovered.errors
    assert publisher.calls[:3] == [batch / "handles.jsonl"] * 3
    assert all(manifest["status"] == "published" for _, manifest in manifests(root))


def test_crash_after_sealing_recovers_acknowledgement_without_remapping(
    setup, monkeypatch
):
    put, directory, root = setup
    put(1)
    put(2)
    worker = SyncWorker(publisher=Publisher())
    monkeypatch.setattr(
        worker,
        "_acknowledge",
        lambda *args: (_ for _ in ()).throw(RuntimeError("crash before ack")),
    )
    worker.run()
    assert worker.errors
    assert pending(directory).count == 2
    batch, manifest = manifests(root)[0]
    assert manifest["status"] == "ready"
    assert not manifest["mapping_acknowledged"]
    publisher = Publisher()
    recovered = SyncWorker(publisher=publisher)
    assert recovered.run() == []
    assert not recovered.errors
    assert publisher.calls == [batch / "handles.jsonl"]
    assert pending(directory).count == 0
    assert len(manifests(root)) == 1


def test_failed_mapping_never_seals_or_advances_pending_progress(setup, monkeypatch):
    put, directory, root = setup
    put(1)
    put(2)
    worker = SyncWorker(publisher=Publisher())
    monkeypatch.setattr(
        worker,
        "_write_items",
        lambda *args: (_ for _ in ()).throw(RuntimeError("mapping failure")),
    )
    worker.run()
    assert worker.errors
    assert pending(directory).count == 2
    assert manifests(root) == []
    failed = next(root.glob("batches/cmip7/.building-*/manifest.json"))
    assert json.loads(failed.read_text())["status"] == "failed"
    recovered = SyncWorker(publisher=Publisher())
    assert len(recovered.run()) == 1
    assert not recovered.errors


def test_bad_checksum_stops_first_item_and_keeps_batch_unpublished(setup, caplog):
    put, directory, root = setup
    message = put(1)
    asset_key = next(iter(message["data"]["payload"]["item"]["assets"]))
    message["data"]["payload"]["item"]["assets"][asset_key][
        "file:checksum"
    ] = "1220122093a5616803c97b154d6eeba73fffc0b871912a2dea5d7f45c77564023970c477"
    with closing(CatalogHarvester(directory)) as harvester:
        harvester.process_with_source("bad", message, KafkaSource("corrections", 0, 0))
    put(2)
    consumer_cfg = dict(config.get("consumer", {}))
    consumer_cfg.pop("max_errors", None)
    config._set("consumer", None, consumer_cfg)
    publisher = Publisher()
    worker = SyncWorker(publisher=publisher)

    assert worker.run() == []
    assert worker.errors
    assert not publisher.calls
    assert pending(directory).count == 2
    failed = next(root.glob("batches/cmip7/.building-*/manifest.json"))
    assert json.loads(failed.read_text())["item_count"] == 1
    assert asset_key in caplog.text
    assert "32-byte digest but contains 34 bytes" in caplog.text
    assert "prefix appears to have been added twice" in caplog.text
    assert "reharvest before retrying" in caplog.text
    assert "CHECKSUM_METHOD is not allowed: unknown" not in caplog.text


def test_worker_lock_prevents_overlapping_ticks(setup):
    put, directory, _root = setup
    put(1)
    put(2)
    worker = SyncWorker(publisher=Publisher())
    with _worker_lock(worker.root) as acquired:
        assert acquired
        assert worker.run() == []
    assert pending(directory).count == 2
    assert len(worker.run()) == 1


def test_clean_generation_rejects_stale_ready_batch(setup):
    put, directory, _root = setup
    put(1)
    put(2)
    publisher = Publisher()
    publisher.fail = True
    SyncWorker(publisher=publisher).run()
    with SQLiteCatalog("cmip7", directory / "cmip7.sqlite") as writer:
        writer.reset()
    publisher.fail = False
    worker = SyncWorker(publisher=publisher)
    worker.run()
    assert worker.errors
    assert "previous catalog generation" in worker.errors[0]
    assert len(publisher.calls) == 1


def test_defaults_and_cli_worker_tick(setup, monkeypatch):
    from piddiplatsch.config.schema import BatchConfig

    assert BatchConfig().publish_workers == 4
    assert BatchConfig().trigger_items == 1000
    assert BatchConfig().max_wait_seconds == 3600
    put, _directory, _root = setup
    put(1)
    publisher = Publisher()
    monkeypatch.setattr("piddiplatsch.runners.sync.HandlePublisher", lambda: publisher)
    result = CliRunner().invoke(cli, ["--silent", "sync", "--force-map"])
    assert result.exit_code == 0, result.output
    assert len(publisher.calls) == 1
    assert publisher.options[0]["workers"] == 4
    assert publisher.options[0]["log_operation"] == "sync/publish"
    assert publisher.options[0]["retries"] == 3


def test_cli_publication_overrides_preserve_config(setup, monkeypatch):
    put, _directory, _root = setup
    put(1)
    config._set("batch", "publish_workers", 2)
    config._set("batch", "publish_retries", 5)
    publisher = Publisher()
    monkeypatch.setattr("piddiplatsch.runners.sync.HandlePublisher", lambda: publisher)
    result = CliRunner().invoke(
        cli,
        ["--silent", "sync", "--force-map", "--workers", "4", "--retries", "0"],
    )
    assert result.exit_code == 0, result.output
    assert publisher.options[0]["workers"] == 4
    assert publisher.options[0]["retries"] == 0
    assert config.get("batch", "publish_workers") == 2
    assert config.get("batch", "publish_retries") == 5
    put(2)
    worker = SyncWorker(publisher=publisher)
    worker.run(force_map=True)
    assert not worker.errors
    assert publisher.options[-1]["workers"] == 2
    assert publisher.options[-1]["retries"] == 5


def test_cli_project_selection_preserves_other_ready_and_pending_work(
    setup, monkeypatch
):
    put, directory, root = setup
    put(1)
    publisher = Publisher()
    publisher.fail = True
    SyncWorker(publisher=publisher).run(force_map=True)
    ready, _manifest = manifests(root)[0]
    assert ready.parent.name == "ready"
    put(2)
    config._set("consumer", "projects", ["cmip6", "cmip7"])
    publisher.fail = False
    publisher.calls.clear()
    monkeypatch.setattr("piddiplatsch.runners.sync.HandlePublisher", lambda: publisher)
    result = CliRunner().invoke(
        cli, ["--silent", "sync", "--project", "cmip6", "--force-map"]
    )
    assert result.exit_code == 0, result.output
    assert not publisher.calls
    assert ready.exists()
    assert pending(directory).count == 1
    result = CliRunner().invoke(
        cli,
        ["--silent", "sync", "--project", "cmip6", "--project", "cmip7", "--force-map"],
    )
    assert result.exit_code == 0, result.output
    assert len(publisher.calls) == 2
    assert not ready.exists()
    assert pending(directory).count == 0


@pytest.mark.parametrize(
    ("options", "error"),
    [
        (["--project", "unknown"], "not found"),
        (["--project", "cmip7", "--project", "unknown"], "not found"),
        (["--project", "cmip6"], "not enabled"),
        (["--workers", "0"], "Invalid value"),
        (["--retries", "-1"], "Invalid value"),
    ],
)
def test_invalid_cli_overrides_do_not_publish(setup, monkeypatch, options, error):
    put, directory, root = setup
    put(1)
    publisher = Publisher()
    monkeypatch.setattr("piddiplatsch.runners.sync.HandlePublisher", lambda: publisher)
    result = CliRunner().invoke(cli, ["--silent", "sync", "--force-map", *options])
    assert result.exit_code != 0
    assert error in result.output
    assert not publisher.calls
    assert pending(directory).count == 1
    assert not (root / "batches").exists()


def test_corrupt_ready_payload_blocks_publication_before_ack(setup, monkeypatch):
    put, directory, root = setup
    put(1)
    put(2)
    worker = SyncWorker(publisher=Publisher())
    monkeypatch.setattr(
        worker,
        "_acknowledge",
        lambda *args: (_ for _ in ()).throw(RuntimeError("crash")),
    )
    worker.run()
    batch, _ = manifests(root)[0]
    output = batch / "handles.jsonl"
    output.chmod(0o640)
    output.write_text("corrupt\n")
    publisher = Publisher()
    recovered = SyncWorker(publisher=publisher)
    recovered.run()
    assert recovered.errors
    assert "checksum/count mismatch" in recovered.errors[0]
    assert not publisher.calls
    assert pending(directory).count == 2


def test_identical_harvest_does_not_schedule_another_batch(setup):
    put, directory, root = setup
    put(1)
    first = pending(directory)
    put(1)
    assert pending(directory) == first
    SyncWorker(publisher=Publisher()).run(force_map=True)
    assert pending(directory).count == 0
    put(1)
    assert pending(directory).count == 0
    assert len(manifests(root)) == 1


def test_mapping_failure_writes_diagnostic_item_and_preserves_queue(setup):
    put, directory, root = setup
    message = put(1)
    put(2)
    item = deepcopy(message["data"]["payload"]["item"])
    item["properties"].pop("cmip7:pid", None)
    item["assets"] = {"bad-file": {}}
    from piddiplatsch.catalog import CatalogEntry
    from piddiplatsch.core.registry import get_plugin

    dataset_id, version = get_plugin("cmip7").catalog_index(item["id"])
    with SQLiteCatalog("cmip7", directory / "cmip7.sqlite") as writer:
        with writer.transaction() as transaction:
            transaction.put_item(
                CatalogEntry(item["collection"], item, dataset_id, version)
            )
    publisher = Publisher()
    worker = SyncWorker(publisher=publisher)
    worker.run()
    assert worker.errors
    assert not publisher.calls
    assert pending(directory).count == 2
    assert manifests(root) == []
    diagnostics = list(root.glob("cmip7/failures/r0/*.jsonl"))
    assert diagnostics
    assert item["id"] in diagnostics[0].read_text()


@pytest.mark.parametrize(
    "field, value",
    [
        ("trigger_items", 0),
        ("max_wait_seconds", 0),
        ("max_items", -1),
        ("publish_workers", 0),
        ("publish_retries", -1),
    ],
)
def test_invalid_batch_configuration_is_rejected(field, value):
    from piddiplatsch.config.schema import BatchConfig

    with pytest.raises(ValueError):
        BatchConfig.model_validate({field: value})


def test_real_publisher_receipts_and_archiving_without_remote_service(setup):
    from piddiplatsch.runners.publish import HandlePublisher

    put, _directory, root = setup
    put(1)
    put(2)

    class Backend:
        prefix = "21.TEST"

        def __init__(self):
            self.records = []

        def add(self, pid, record):
            self.records.append((pid, record))

    backend = Backend()
    worker = SyncWorker(publisher=HandlePublisher(backend=backend))
    paths = worker.run()
    assert not worker.errors
    assert len(paths) == 1
    batch, manifest = manifests(root)[0]
    assert batch.parent.name == "published"
    assert paths[0] == batch
    assert len(backend.records) == manifest["handle_count"]
    from pathlib import Path

    receipts = [
        json.loads(line) for line in Path(manifest["receipt"]).read_text().splitlines()
    ]
    assert len(receipts) == manifest["handle_count"]
    assert all(receipt["status"] == "succeeded" for receipt in receipts)


def test_crash_after_success_marker_archives_without_republishing(setup, monkeypatch):
    put, _directory, root = setup
    put(1)
    put(2)
    publisher = Publisher()
    worker = SyncWorker(publisher=publisher)
    monkeypatch.setattr(
        worker,
        "_archive",
        lambda *args: (_ for _ in ()).throw(RuntimeError("crash before archive")),
    )
    worker.run()
    assert worker.errors
    batch, manifest = manifests(root)[0]
    assert batch.parent.name == "ready"
    assert manifest["status"] == "published"
    recovered = SyncWorker(publisher=publisher)
    recovered.run()
    assert not recovered.errors
    assert len(publisher.calls) == 1
    assert manifests(root)[0][0].parent.name == "published"


def test_crash_after_catalog_ack_keeps_newer_changes_on_recovery(setup, monkeypatch):
    put, directory, root = setup
    put(1)
    put(2)
    worker = SyncWorker(publisher=Publisher())

    def acknowledge_then_crash(batch, manifest):
        with SQLiteCatalog("cmip7", directory / "cmip7.sqlite") as writer:
            writer.acknowledge_mapped(manifest["generation"], manifest["sequence"])
        raise RuntimeError("crash before manifest acknowledgement")

    monkeypatch.setattr(worker, "_acknowledge", acknowledge_then_crash)
    worker.run()
    assert worker.errors
    assert pending(directory).count == 0
    assert manifests(root)[0][1]["mapping_acknowledged"] is False
    put(3)
    publisher = Publisher()
    recovered = SyncWorker(publisher=publisher)
    recovered.run(force_map=True)
    assert not recovered.errors
    assert pending(directory).count == 0
    assert len(publisher.calls) == 2
    assert sorted(manifest["item_count"] for _, manifest in manifests(root)) == [1, 2]


def test_sync_progress_displays_map_and_known_publication_total(setup, monkeypatch):
    from unittest.mock import MagicMock

    from piddiplatsch.monitoring.progress import Progress
    from piddiplatsch.runners.publish import HandlePublisher

    put, _directory, root = setup
    put(1)
    put(2)
    backend = MagicMock(spec=["prefix", "add"], prefix="21.TEST")
    backend.add.return_value = None
    bars = MagicMock()
    monkeypatch.setattr("piddiplatsch.monitoring.progress.tqdm", bars)
    with Progress(title="sync") as progress:
        worker = SyncWorker(
            publisher=HandlePublisher(backend=backend), progress=progress
        )
        worker.run()
    assert not worker.errors
    _, manifest = manifests(root)[0]
    publication = [
        call.kwargs
        for call in bars.call_args_list
        if call.kwargs.get("unit") == "handle"
    ]
    assert len(publication) == 1
    assert publication[0]["total"] == manifest["handle_count"]
    assert "sync cmip7 publish" in publication[0]["desc"]
    phases = [
        str(call) for call in bars.return_value.set_description_str.call_args_list
    ]
    assert any("sync cmip7 map" in phase and "items:" in phase for phase in phases)
