import pytest

from piddiplatsch.config import config
from piddiplatsch.consumer import start_consumer
from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.result import ProcessingResult

pytestmark = pytest.mark.integration


class BoomProcessor(BaseProcessor):
    def preflight_check(self, **kwargs):
        return None

    def process(self, key, value):
        if key == "bad1":
            raise RuntimeError("boom: simulated processing error")
        return ProcessingResult(key=key, success=True, num_handles=0)


class OKProcessor(BaseProcessor):
    def preflight_check(self, **kwargs):
        return None

    def process(self, key, value):
        return ProcessingResult(key=key, success=True, num_handles=0)


def test_stop_on_first_error(tmp_path):
    # Ensure conservative setting for this test
    consumer_cfg = dict(config.get("consumer", {}))
    consumer_cfg["max_errors"] = 1
    config._set("consumer", None, consumer_cfg)
    with pytest.raises(SystemExit) as exc:
        start_consumer(
            processor=BoomProcessor(publish=False),
            direct_messages=[("bad1", {"retries": 0}), ("good1", {"ok": True})],
            publish=False,
            verbose=False,
        )
    assert exc.value.code == 1

    # Failure JSONL should be created
    failure_files = sorted((tmp_path / "outputs" / "failures").glob("**/*.jsonl"))
    assert len(failure_files) >= 1


def test_proceed_after_fix(tmp_path):
    # After fixing, consumer should process both messages successfully
    start_consumer(
        processor=OKProcessor(publish=False),
        direct_messages=[("bad1", {"fixed": True}), ("good2", {"ok": True})],
        publish=False,
        verbose=False,
    )
