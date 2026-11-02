from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from piddiplatsch.config import config
from piddiplatsch.config.schema import DiskConfig
from piddiplatsch.core.messages import KafkaSource, SourceMessage
from piddiplatsch.core.pipeline import ProcessingPipeline
from piddiplatsch.exceptions import DiskSpaceError
from piddiplatsch.monitoring.disk import GIB, DiskGuard, disk_status, get_disk_guard
from piddiplatsch.runners.publish import HandlePublisher
from piddiplatsch.runners.sync import SyncWorker


def test_guard_warns_once_recovers_and_warns_on_next_crossing(
    tmp_path, monkeypatch, caplog
):
    free = [30]
    monkeypatch.setattr(
        "piddiplatsch.monitoring.disk.shutil.disk_usage",
        lambda _: SimpleNamespace(total=200 * GIB, free=free[0] * GIB),
    )
    guard = DiskGuard((tmp_path,), DiskConfig())
    guard.check(force=True)
    guard.check(force=True)
    assert len([line for line in caplog.messages if "Disk space warning" in line]) == 1
    free[0] = 60
    guard.check(force=True)
    free[0] = 30
    guard.check(force=True)
    assert len([line for line in caplog.messages if "Disk space warning" in line]) == 2


def test_guard_throttles_checks_but_does_not_bypass_cached_stop(tmp_path, monkeypatch):
    usage = Mock(return_value=SimpleNamespace(total=200 * GIB, free=19 * GIB))
    monkeypatch.setattr("piddiplatsch.monitoring.disk.shutil.disk_usage", usage)
    guard = DiskGuard((tmp_path,), DiskConfig())
    for _ in range(2):
        with pytest.raises(DiskSpaceError, match="20 GiB stop threshold"):
            guard.check()
    assert usage.call_count == 1
    usage.return_value.free = 60 * GIB
    guard.check(force=True)


def test_disk_status_uses_existing_parent_without_creating_directories(
    tmp_path, monkeypatch
):
    missing = tmp_path / "does-not-exist" / "outputs"
    usage = Mock(return_value=SimpleNamespace(total=200 * GIB, free=50 * GIB))
    monkeypatch.setattr("piddiplatsch.monitoring.disk.shutil.disk_usage", usage)
    assert disk_status([missing])[0]["state"] == "healthy"
    usage.assert_called_once_with(tmp_path)
    assert not missing.parent.exists()


def test_guard_stops_before_processing_or_acknowledging_kafka_message(
    tmp_path, monkeypatch
):
    config._set("disk", "enabled", True)
    monkeypatch.setattr(
        "piddiplatsch.monitoring.disk.shutil.disk_usage",
        lambda _: SimpleNamespace(total=200 * GIB, free=19 * GIB),
    )
    processor = Mock()
    acknowledgement = Mock()
    message = SourceMessage("key", {}, KafkaSource("topic", 0, 0), acknowledgement)
    pipeline = ProcessingPipeline([message], processor, dump_messages=True)
    with pytest.raises(DiskSpaceError):
        pipeline.run()
    processor.process.assert_not_called()
    acknowledgement.assert_not_called()
    assert not list(tmp_path.rglob("*.jsonl"))


def test_publisher_stops_before_creating_receipts_or_writing_handles(
    tmp_path, monkeypatch
):
    config._set("disk", "enabled", True)
    monkeypatch.setattr(
        "piddiplatsch.monitoring.disk.shutil.disk_usage",
        lambda _: SimpleNamespace(total=200 * GIB, free=19 * GIB),
    )
    backend = Mock()
    with pytest.raises(DiskSpaceError):
        HandlePublisher(backend).run([])
    backend.add.assert_not_called()
    assert not list(tmp_path.rglob("published*.jsonl"))


def test_disk_threshold_configuration_rejects_inverted_thresholds():
    with pytest.raises(ValueError, match="must exceed"):
        DiskConfig(warn_free_gib=10, stop_free_gib=20)


@pytest.mark.parametrize(
    ("free", "state"), [(40, "healthy"), (39, "warning"), (20, "warning"), (19, "stop")]
)
def test_disk_threshold_boundaries(tmp_path, monkeypatch, free, state):
    monkeypatch.setattr(
        "piddiplatsch.monitoring.disk.shutil.disk_usage",
        lambda _: SimpleNamespace(total=200 * GIB, free=free * GIB),
    )
    assert disk_status([tmp_path], DiskConfig())[0]["state"] == state


def test_critical_stop_survives_new_process_and_recovery_until_manual_clear(
    tmp_path, monkeypatch
):
    usage = Mock(return_value=SimpleNamespace(total=200 * GIB, free=19 * GIB))
    monkeypatch.setattr("piddiplatsch.monitoring.disk.shutil.disk_usage", usage)
    marker = tmp_path / "outputs" / ".disk-space-stop"
    guard = DiskGuard((tmp_path,), DiskConfig(), stop_file=marker)
    with pytest.raises(DiskSpaceError, match="stop threshold"):
        guard.check()
    assert "19.0 GiB" in marker.read_text()
    usage.return_value.free = 60 * GIB
    restarted = DiskGuard((tmp_path,), DiskConfig(), stop_file=marker)
    with pytest.raises(DiskSpaceError, match="latched"):
        restarted.check()
    marker.unlink()
    restarted.check(force=True)


def test_warning_does_not_latch_stop(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "piddiplatsch.monitoring.disk.shutil.disk_usage",
        lambda _: SimpleNamespace(total=200 * GIB, free=30 * GIB),
    )
    marker = tmp_path / ".disk-space-stop"
    DiskGuard((tmp_path,), DiskConfig(), stop_file=marker).check()
    assert not marker.exists()


def test_unreadable_storage_latches_stop(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "piddiplatsch.monitoring.disk.shutil.disk_usage",
        Mock(side_effect=OSError("storage unavailable")),
    )
    marker = tmp_path / ".disk-space-stop"
    with pytest.raises(DiskSpaceError, match="Cannot check"):
        DiskGuard((tmp_path,), DiskConfig(), stop_file=marker).check()
    assert "storage unavailable" in marker.read_text()


def test_mid_message_disk_stop_is_not_recorded_as_invalid_event(tmp_path, monkeypatch):
    recorder = Mock()
    monkeypatch.setattr("piddiplatsch.core.pipeline.FailureRecorder", recorder)
    processor = Mock()
    processor.process.side_effect = DiskSpaceError("disk critical")
    pipeline = ProcessingPipeline([("key", {})], processor)
    with pytest.raises(DiskSpaceError, match="disk critical"):
        pipeline.run()
    recorder.assert_not_called()


def test_sync_stops_all_projects_on_disk_error(monkeypatch):
    config._set("consumer", "projects", ["cmip6", "cmip7"])
    worker = SyncWorker()
    publish = Mock(side_effect=DiskSpaceError("disk critical"))
    monkeypatch.setattr(worker, "_publish_ready", publish)
    with pytest.raises(DiskSpaceError, match="disk critical"):
        worker.run()
    assert publish.call_count == 1


def test_guards_with_different_storage_paths_share_stop_marker(tmp_path, monkeypatch):
    config._set("disk", "enabled", True)
    config._set("consumer", "output_dir", str(tmp_path / "output"))
    usage = Mock(return_value=SimpleNamespace(total=200 * GIB, free=19 * GIB))
    monkeypatch.setattr("piddiplatsch.monitoring.disk.shutil.disk_usage", usage)
    with pytest.raises(DiskSpaceError):
        get_disk_guard(extra_paths=(tmp_path / "catalog",)).check(force=True)
    usage.return_value.free = 60 * GIB
    with pytest.raises(DiskSpaceError, match="latched"):
        get_disk_guard(extra_paths=(tmp_path / "receipts",)).check(force=True)
