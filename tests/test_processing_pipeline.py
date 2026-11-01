from unittest.mock import Mock

import pytest

from piddiplatsch.core.pipeline import ProcessingPipeline, process_messages
from piddiplatsch.exceptions import MaxErrorsExceededError
from piddiplatsch.result import ProcessingResult


class SuccessfulProcessor:
    def process(self, key, value):
        return ProcessingResult(key=key, success=True)


@pytest.mark.parametrize("limit", [0, 1])
def test_zero_tolerance_processes_successes_and_stops_at_first_error(limit):
    visited = []

    class Processor:
        def process(self, key, value):
            visited.append(key)
            return ProcessingResult(
                key=key, success=key != "bad", error="bad" if key == "bad" else None
            )

    with pytest.raises(MaxErrorsExceededError):
        process_messages(
            [("good", {}), ("bad", {}), ("later", {})],
            processor=Processor(),
            max_errors=limit,
        )
    assert visited == ["good", "bad"]


def test_mapping_project_error_budgets_are_independent():
    visited = []

    class Processor:
        def process(self, key, value):
            visited.append(key)
            return ProcessingResult(key=key, plugin=value["project"], error="invalid")

    with pytest.raises(MaxErrorsExceededError, match=r"2/2.*cmip7"):
        process_messages(
            [
                ("a", {"project": "cmip6plus"}),
                ("b", {"project": "cmip7"}),
                ("c", {"project": "cmip7"}),
                ("later", {"project": "cmip6plus"}),
            ],
            processor=Processor(),
            max_errors=1,
            project_max_errors={"cmip6plus": 10000, "cmip7": 2},
        )
    assert visited == ["a", "b", "c"]


def test_pipeline_is_lazy_and_limit_does_not_read_ahead():
    seen = []

    def messages():
        for i in range(4):
            seen.append(i)
            yield str(i), {}

    pipeline = ProcessingPipeline(messages(), SuccessfulProcessor(), limit=2)
    assert seen == []
    try:
        result = pipeline.run()
    finally:
        pipeline.close_progress()
    assert seen == [0, 1]
    assert result.total == result.succeeded == 2


def test_process_messages_returns_per_run_counts():
    first = process_messages(
        ((str(i), {}) for i in range(3)), processor=SuccessfulProcessor()
    )
    second = process_messages(iter([("next", {})]), processor=SuccessfulProcessor())
    assert first.total == first.succeeded == 3
    assert second.total == second.succeeded == 1
    assert first.failed == second.failed == 0


@pytest.mark.parametrize("injected", [False, True])
@pytest.mark.parametrize("fails", [False, True])
def test_progress_ownership_on_completion_and_input_failure(
    monkeypatch, injected, fails
):
    progress = Mock()
    monkeypatch.setattr(
        "piddiplatsch.core.pipeline.get_progress", lambda *args, **kwargs: progress
    )

    def messages():
        yield "one", {}
        if fails:
            raise RuntimeError("input failure")

    kwargs = {
        "processor": SuccessfulProcessor(),
        "progress": progress if injected else None,
    }
    if fails:
        with pytest.raises(RuntimeError, match="input failure"):
            process_messages(messages(), **kwargs)
    else:
        assert process_messages(messages(), **kwargs).total == 1
    progress.refresh.assert_called_once_with()
    if injected:
        progress.close.assert_not_called()
    else:
        progress.close.assert_called_once_with()
