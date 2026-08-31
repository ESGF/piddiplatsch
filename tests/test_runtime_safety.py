import json

from piddiplatsch.config import config
from piddiplatsch.consumer import ConsumerPipeline, DirectConsumer, feed_messages_direct
from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.persist.dump import DumpRecorder
from piddiplatsch.persist.recovery import FailureRecorder
from piddiplatsch.persist.retry import RetryRunner
from piddiplatsch.persist.skipped import SkipRecorder
from piddiplatsch.plugins.cmip6.record import CMIP6DatasetRecord
from piddiplatsch.result import ProcessingResult


class FailingProcessor(BaseProcessor):
    def __init__(self):
        pass

    def process(self, key, value):
        raise RuntimeError("still broken")


class ProjectAwareFailingProcessor(FailingProcessor):
    def plugin_name_for(self, value):
        return "cmip6"


class SkippingProcessor(BaseProcessor):
    def __init__(self):
        pass

    def process(self, key, value):
        return ProcessingResult(
            key=key,
            skipped=True,
            skip_reason="try later",
            project="cmip6",
            plugin="cmip6",
        )


def test_recorders_resolve_output_dir_when_instantiated(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))

    assert DumpRecorder().root_dir == tmp_path / "dump"
    assert SkipRecorder().root_dir == tmp_path / "skipped"
    assert FailureRecorder().root_dir == tmp_path / "failures"


def test_recovery_recorders_store_project_and_use_project_paths(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))

    failure_path = FailureRecorder(project="cmip6").record(
        "key", {"data": {}}, reason="broken"
    )
    skipped_path = SkipRecorder(project="cmip7").record(
        "key", {"data": {}}, reason="later"
    )

    assert failure_path.parent == tmp_path / "cmip6" / "failures" / "r0"
    assert skipped_path.parent == tmp_path / "cmip7" / "skipped"
    assert json.loads(failure_path.read_text())["__infos__"]["project"] == "cmip6"
    assert json.loads(skipped_path.read_text())["__infos__"]["project"] == "cmip7"


def test_nested_retry_count_selects_failure_subdirectory(tmp_path):
    value = {"payload": {}, "__infos__": {"retries": 2}}
    pipeline = ConsumerPipeline(
        DirectConsumer([("key", value)]),
        FailingProcessor(),
        failure_dir=tmp_path / "failures",
    )

    pipeline.run()

    failure_files = list((tmp_path / "failures" / "r2").glob("*.jsonl"))
    assert len(failure_files) == 1


def test_pipeline_persists_resolved_project_on_failure(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    pipeline = ConsumerPipeline(
        DirectConsumer([("key", {"data": {}})]),
        ProjectAwareFailingProcessor(),
    )

    pipeline.run()

    failure_file = next((tmp_path / "cmip6" / "failures" / "r0").glob("*.jsonl"))
    assert json.loads(failure_file.read_text())["__infos__"]["project"] == "cmip6"


def test_skipped_message_is_not_reported_as_success(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    result = feed_messages_direct(
        [("key", {})],
        processor=SkippingProcessor(),
        failure_dir=tmp_path / "failures",
        force=True,
    )

    assert result.total == 1
    assert result.succeeded == 0
    assert result.failed == 0
    assert result.skipped == 1
    skipped_file = next((tmp_path / "cmip6" / "skipped").glob("*.jsonl"))
    assert json.loads(skipped_file.read_text())["__infos__"]["project"] == "cmip6"


def test_malformed_retry_input_is_retained(tmp_path):
    source = tmp_path / "retry.jsonl"
    source.write_text(json.dumps({"key": "valid"}) + "\n{broken\n", encoding="utf-8")
    runner = RetryRunner(
        projects=["cmip6"],
        failure_dir=tmp_path / "failures",
        delete_after=True,
        publish=False,
    )

    result = runner.run_file(source)

    assert result.succeeded == 0
    assert result.failed == 1
    assert result.errors
    assert "line 2" in result.errors[0]
    assert source.exists()


def test_record_repr_handles_cached_pid(tmp_path):
    config._set("lookup", "enabled", False)
    record = CMIP6DatasetRecord(
        {"id": "CMIP6.Activity.Institute.Source.Experiment.r1i1p1f1.Amon.pr.gn.v1"}
    )

    assert "pid=" in repr(record)
