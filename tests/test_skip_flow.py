import json

from piddiplatsch.consumer import ConsumerPipeline, DirectConsumer
from piddiplatsch.exceptions import TransientExternalError
from piddiplatsch.persist.skipped import SkipRecorder
from piddiplatsch.plugins.cmip6.processor import CMIP6Processor


class FailingPatchProcessor(CMIP6Processor):
    def _apply_patch_to_stac_item(self, payload):
        raise TransientExternalError("simulated network failure")


def test_pipeline_records_skipped_on_patch_failure(tmp_path, monkeypatch):
    # Patch SkipRecorder directory
    skipped_dir = tmp_path / "skipped"
    skipped_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(SkipRecorder, "SKIPPED_DIR", skipped_dir)

    # Construct a PATCH payload that will trigger failure
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

    consumer = DirectConsumer([(key, value)])
    pipeline = ConsumerPipeline(
        consumer, processor=FailingPatchProcessor(publish=False), force=True
    )

    pipeline.run()

    # Verify skipped file written
    files = list(skipped_dir.glob("*.jsonl"))
    assert len(files) == 1
    content = files[0].read_text(encoding="utf-8").strip().splitlines()
    assert len(content) == 1
    obj = json.loads(content[0])
    assert obj["__infos__"]["reason"].startswith("TRANSIENT external")
