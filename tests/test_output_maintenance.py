import fcntl
import gzip
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from itertools import count
from uuid import uuid4

import pytest
from click.testing import CliRunner

from piddiplatsch.catalog import CatalogEntry
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.output_maintenance import maintain_outputs, output_maintenance_lock

NOW = datetime(2026, 11, 2, 12, tzinfo=UTC)


@pytest.fixture
def outputs(tmp_path, monkeypatch):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("catalog", "directory", str(tmp_path))
    config._set(
        "maintenance", None, dict(compress_outputs=True, output_retention_days=0)
    )
    monkeypatch.setattr("piddiplatsch.output_maintenance.utc_now", lambda: NOW)
    offsets = count()

    def seed(*, days=2, state="published", acknowledge=True, created_days=None):
        finished = NOW - timedelta(days=days)
        created = NOW - timedelta(
            days=created_days if created_days is not None else days
        )
        identity = created.strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex
        root = tmp_path / "batches/cmip6"
        parent = (
            root / "published" / created.strftime("%Y/%m/%d")
            if state == "published"
            else root / "ready"
        )
        batch = parent / identity
        batch.mkdir(parents=True)
        payload = b'{"handle":"21.TEST/one","project":"cmip6"}\n'
        (batch / "handles.jsonl").write_bytes(payload)
        (batch / "failures.jsonl").write_bytes(b"")
        (batch / "publication-progress.jsonl").write_bytes(b'{"line":1}\n')
        receipt = (
            tmp_path
            / "published"
            / finished.strftime("%Y/%m/%d")
            / (
                "published_cmip6_handles_"
                + finished.strftime("%Y-%m-%d_%H-%M-%S")
                + f"_{next(offsets)}.jsonl"
            )
        )
        receipt.parent.mkdir(parents=True, exist_ok=True)
        receipt.write_bytes(b'{"success":true}\n')
        os.utime(receipt, (finished.timestamp(), finished.timestamp()))
        manifest = dict(
            batch_id=identity,
            project="cmip6",
            status=state,
            published_at=finished.isoformat(),
            receipt=str(receipt),
            sha256=hashlib.sha256(payload).hexdigest(),
        )
        (batch / "manifest.json").write_text(json.dumps(manifest))
        offset = next(offsets)
        from piddiplatsch.core.messages import KafkaSource

        with SQLiteCatalog("cmip6", tmp_path / "piddi.db") as writer:
            with writer.transaction() as transaction:
                transaction.add_work(
                    CatalogEntry("CMIP6", dict(id="one"), "dataset", 1),
                    KafkaSource("topic", 0, offset),
                )
            work_id = writer._execute(
                "SELECT id FROM work_items WHERE offset=?", (offset,)
            ).fetchone()[0]
            writer.seal_batch(identity, [dict(id=work_id, error=None, handle_count=1)])
            if state == "published" and acknowledge:
                writer.publish_batch(identity, 1)
                with writer.transaction():
                    writer._execute(
                        "UPDATE stats SET published_at=? WHERE kind='batch' AND id=?",
                        (finished.isoformat(), identity),
                    )
        return batch, receipt, payload

    return seed


def test_compress_completed_outputs_preserves_payloads_stats_and_receipt_links(
    outputs, tmp_path
):
    batch, receipt, payload = outputs(days=0)
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as reader:
        before = reader._execute("SELECT * FROM stats").fetchall()
    result = maintain_outputs()
    assert len(result.compressed) == 4
    assert not result.removed_batches and not result.removed_receipts
    assert gzip.decompress((batch / "handles.jsonl.gz").read_bytes()) == payload
    assert (
        gzip.decompress(receipt.with_suffix(".jsonl.gz").read_bytes())
        == b'{"success":true}\n'
    )
    manifest = json.loads((batch / "manifest.json").read_text())
    assert manifest["sha256"] == hashlib.sha256(payload).hexdigest()
    assert manifest["receipt"] == str(receipt.with_suffix(".jsonl.gz"))
    assert not (batch / "handles.jsonl").exists()
    assert not receipt.exists()
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader._execute("SELECT * FROM stats").fetchall() == before
    assert maintain_outputs().compressed == []


def test_default_keeps_even_old_output_and_optional_retention_deletes_it(outputs):
    batch, receipt, _ = outputs(days=31)
    maintain_outputs()
    assert batch.exists() and receipt.with_suffix(".jsonl.gz").exists()
    config._set("maintenance", "output_retention_days", 30)
    result = maintain_outputs(dry_run=True)
    assert result.removed_batches == [batch]
    assert result.removed_receipts == [receipt.with_suffix(".jsonl.gz")]
    assert batch.exists()
    result = maintain_outputs()
    assert result.removed_batches == [batch]
    assert not batch.exists() and not receipt.with_suffix(".jsonl.gz").exists()
    assert not maintain_outputs().removed_batches


def test_retention_uses_completion_time_not_old_batch_directory(outputs):
    batch, _, _ = outputs(days=1, created_days=40)
    config._set("maintenance", "output_retention_days", 30)
    assert not maintain_outputs().removed_batches
    assert batch.exists()


def test_ready_failed_and_unacknowledged_batches_are_not_compressed_or_deleted(outputs):
    ready, protected_receipt, _ = outputs(days=40, state="ready")
    unacknowledged, _, _ = outputs(days=40, acknowledge=False)
    config._set("maintenance", "output_retention_days", 30)
    result = maintain_outputs()
    assert not result.removed_batches
    assert (ready / "handles.jsonl").exists()
    assert (unacknowledged / "handles.jsonl").exists()
    assert protected_receipt.exists()


def test_standalone_receipts_wait_until_next_utc_day_and_keep_active_files(
    outputs, tmp_path
):
    # Create the DB/layout, then test unrelated, run-scoped receipt files.
    outputs()
    directory = tmp_path / "published/2026/11/01"
    directory.mkdir(parents=True, exist_ok=True)
    yesterday = directory / "published_handles_2026-11-01_01-00-00.jsonl"
    yesterday.write_bytes(b'{"success":true}\n')
    stamp = (NOW - timedelta(days=1)).timestamp()
    os.utime(yesterday, (stamp, stamp))
    today = directory / "published_handles_2026-11-02_01-00-00.jsonl"
    today.write_bytes(b'{"success":true}\n')
    os.utime(today, (NOW.timestamp(), NOW.timestamp()))
    reserved = directory / "published_handles_2026-11-01_02-00-00.jsonl"
    reserved.touch()
    os.utime(reserved, (stamp, stamp))
    from piddiplatsch.runners.publish import _PublicationResultWriter

    writer = _PublicationResultWriter(yesterday)
    try:
        maintain_outputs()
        assert yesterday.exists() and today.exists() and reserved.exists()
    finally:
        writer.close()
    maintain_outputs()
    assert yesterday.with_suffix(".jsonl.gz").exists()
    assert today.exists() and reserved.exists()
    assert yesterday.with_suffix(".jsonl.gz").stat().st_mtime == stamp


def test_output_maintenance_runs_beside_worker_but_blocks_project_reset(
    outputs, tmp_path
):
    from piddiplatsch.runners.reset import reset_processing
    from piddiplatsch.runners.sync import _worker_lock

    batch, _, _ = outputs()
    with _worker_lock(tmp_path):
        assert maintain_outputs().compressed
    with output_maintenance_lock(tmp_path):
        assert not maintain_outputs().compressed
        with pytest.raises(RuntimeError, match="maintenance is running"):
            reset_processing(config, "cmip6")
    assert batch.exists()


def test_dry_run_reports_output_actions_without_changing_files(outputs):
    batch, receipt, _ = outputs()
    result = CliRunner().invoke(cli, ["maintain", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "Would compress 4 completed output file(s)." in result.output
    assert "Output removal disabled" in result.output
    assert (batch / "handles.jsonl").exists() and receipt.exists()
    assert not list(batch.glob("*.gz"))


def test_compression_can_be_disabled_independently_of_retention(outputs):
    batch, receipt, _ = outputs(days=31)
    config._set("maintenance", "compress_outputs", False)
    assert not maintain_outputs().compressed
    assert (batch / "handles.jsonl").exists()
    config._set("maintenance", "output_retention_days", 30)
    result = maintain_outputs()
    assert result.removed_batches == [batch] and result.removed_receipts == [receipt]


def test_interrupted_batch_removal_resumes_without_its_manifest(outputs, monkeypatch):
    import shutil

    batch, _, _ = outputs(days=31)
    config._set("maintenance", "output_retention_days", 30)
    original = shutil.rmtree

    def interrupted(path):
        (path / "manifest.json").unlink()
        raise OSError("interrupted deletion")

    monkeypatch.setattr("piddiplatsch.output_maintenance.shutil.rmtree", interrupted)
    with pytest.raises(OSError, match="interrupted deletion"):
        maintain_outputs()
    deleting = batch.with_name(".deleting-" + batch.name)
    assert deleting.exists()
    monkeypatch.setattr("piddiplatsch.output_maintenance.shutil.rmtree", original)
    assert maintain_outputs().removed_batches == [deleting]
    assert not deleting.exists()


def test_active_receipt_cannot_be_removed_by_retention(outputs, tmp_path):
    _, receipt, _ = outputs(days=31, state="ready")
    # Remove its ready reference to exercise the writer's file lock independently.
    manifest = next((tmp_path / "batches").glob("*/ready/*/manifest.json"))
    manifest.unlink()
    config._set("maintenance", "output_retention_days", 30)
    with receipt.open("rb") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert receipt not in maintain_outputs().removed_receipts
    assert receipt.exists()


def test_output_compression_failure_preserves_original(outputs, monkeypatch):
    from unittest.mock import Mock

    batch, receipt, payload = outputs()
    monkeypatch.setattr(
        "piddiplatsch.maintenance.gzip.GzipFile.write",
        Mock(side_effect=OSError("disk full")),
    )
    with pytest.raises(OSError, match="disk full"):
        maintain_outputs()
    assert (batch / "handles.jsonl").read_bytes() == payload
    assert receipt.exists()
    assert not list(batch.glob("*.gz"))
    assert not list(batch.glob("*.tmp"))


def test_unreadable_ready_manifest_protects_receipts(outputs):
    ready, receipt, _ = outputs(days=40, state="ready")
    (ready / "manifest.json").write_text("broken")
    config._set("maintenance", "output_retention_days", 30)
    maintain_outputs()
    assert receipt.exists()


def test_symlinked_batch_payload_is_not_compressed(outputs, tmp_path):
    batch, _, _ = outputs()
    external = tmp_path / "unrelated.jsonl"
    external.write_bytes(b"unchanged\n")
    (batch / "handles.jsonl").unlink()
    (batch / "handles.jsonl").symlink_to(external)
    maintain_outputs()
    assert external.read_bytes() == b"unchanged\n"
    assert (batch / "handles.jsonl").is_symlink()
    assert not (batch / "handles.jsonl.gz").exists()


@pytest.mark.parametrize(
    "settings",
    [
        dict(output_retention_days=-1),
        dict(output_retention_days=True),
        dict(output_retention_days=1.5),
        dict(compress_outputs="yes"),
    ],
)
def test_output_retention_configuration_requires_explicit_valid_values(settings):
    from pydantic import ValidationError

    from piddiplatsch.config.schema import MaintenanceConfig

    with pytest.raises(ValidationError):
        MaintenanceConfig.model_validate(settings)


def test_receipt_writer_never_reopens_an_already_compressed_receipt(outputs):
    from piddiplatsch.runners.publish import _PublicationResultWriter

    _, receipt, _ = outputs()
    maintain_outputs()
    with pytest.raises(OSError, match="already archived"):
        _PublicationResultWriter(receipt)
    assert not receipt.exists()


def test_receipt_name_reservation_skips_existing_compressed_names(
    tmp_path, monkeypatch
):
    from piddiplatsch.runners.publish import HandlePublisher

    config._set("consumer", "output_dir", str(tmp_path))
    monkeypatch.setattr("piddiplatsch.runners.publish.utc_now", lambda: NOW)
    first = HandlePublisher._new_result_file("cmip6")
    first.with_suffix(".jsonl.gz").write_bytes(gzip.compress(b"{}\n"))
    first.unlink()
    second = HandlePublisher._new_result_file("cmip6")
    assert second != first
    assert second.name.endswith("_2.jsonl")


def test_receipt_writer_stops_if_maintenance_removed_file_before_lock(
    outputs, monkeypatch
):
    from piddiplatsch.runners.publish import _PublicationResultWriter

    _, receipt, _ = outputs()
    original = fcntl.flock

    def removed_before_lock(stream, operation):
        receipt.unlink()
        return original(stream, operation)

    monkeypatch.setattr("piddiplatsch.runners.publish.fcntl.flock", removed_before_lock)
    with pytest.raises(OSError):
        _PublicationResultWriter(receipt)
