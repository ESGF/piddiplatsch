import pytest

from piddiplatsch.config import config
from piddiplatsch.handles.jsonl_backend import JsonlHandleBackend
from piddiplatsch.handles.recording_backend import RecordingHandleBackend
from piddiplatsch.jsonl_stream import iter_jsonl_records


class FailingBackend:
    prefix = "21.TEST"

    def __init__(self):
        self.calls = []

    def add(self, pid, record):
        self.calls.append((pid, record))
        raise RuntimeError("Handle service unavailable")

    def get(self, pid):
        return {"URL": f"https://example.test/{pid}"}


def test_records_jsonl_before_direct_publication_failure(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("handle", "prefix", "21.TEST")
    primary = FailingBackend()
    backend = RecordingHandleBackend(primary, project="cmip6")

    with pytest.raises(RuntimeError, match="unavailable"):
        backend.add(
            "abc",
            {
                "URL": "https://example.test/abc",
                "AGGREGATION_LEVEL": "DATASET",
            },
        )

    path = next((tmp_path / "cmip6" / "handles").glob("handles_*.jsonl"))
    records = [record for _, _, record in iter_jsonl_records([path])]
    assert records == [
        {
            "handle": "21.TEST/abc",
            "URL": "https://example.test/abc",
            "data": {"AGGREGATION_LEVEL": "DATASET"},
            "timestamp": records[0]["timestamp"],
            "project": "cmip6",
        }
    ]
    assert len(primary.calls) == 1


def test_get_delegates_without_writing_jsonl(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    primary = FailingBackend()
    recorder = JsonlHandleBackend(project="cmip6")
    backend = RecordingHandleBackend(primary, recorder=recorder)

    assert backend.get("abc") == {"URL": "https://example.test/abc"}
    assert list(tmp_path.rglob("*.jsonl")) == []
