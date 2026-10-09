from unittest.mock import Mock

import pytest

from piddiplatsch.core.pipeline import ProcessingPipeline, process_messages
from piddiplatsch.result import ProcessingResult


class SuccessfulProcessor:
    def process(self, key, value):
        return ProcessingResult(key=key, success=True)


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
