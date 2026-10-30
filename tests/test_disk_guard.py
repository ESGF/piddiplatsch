from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from piddiplatsch.config import config
from piddiplatsch.config.schema import DiskConfig
from piddiplatsch.core.messages import KafkaSource, SourceMessage
from piddiplatsch.core.pipeline import ProcessingPipeline
from piddiplatsch.exceptions import DiskSpaceError
from piddiplatsch.monitoring.disk import GIB, DiskGuard, disk_status
from piddiplatsch.runners.publish import HandlePublisher


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
