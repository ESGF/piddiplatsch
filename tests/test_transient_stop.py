import pytest

from piddiplatsch.core.pipeline import ProcessingPipeline
from piddiplatsch.exceptions import StopOnTransientSkipError, TransientExternalError
from piddiplatsch.plugins.cmip6.processor import CMIP6Processor


class AlwaysTransientProcessor(CMIP6Processor):
    def _apply_patch_to_stac_item(self, payload):
        # Simulate external failure after retries
        raise TransientExternalError("simulated STAC outage")


def test_stop_on_transient_skip(monkeypatch):
    key = "k1"
    value = {
        "data": {
            "payload": {
                "method": "PATCH",
                "collection_id": "c1",
                "item_id": "i1",
                "patch": {"operations": []},
            }
        },
        "metadata": {"time": "2024-01-01T00:00:00Z"},
    }

    consumer = [(key, value)]
    pipeline = ProcessingPipeline(
        consumer,
        processor=AlwaysTransientProcessor(publish=False),
        force=False,
    )

    with pytest.raises(StopOnTransientSkipError):
        pipeline.run()
