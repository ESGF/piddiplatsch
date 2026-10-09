import json
from contextlib import closing
from pathlib import Path

import pytest

from piddiplatsch.exceptions import JsonlReadError
from piddiplatsch.jsonl_stream import iter_jsonl_records


def test_window_spans_files_and_preserves_physical_lines(tmp_path):
    first, second = tmp_path / "1.jsonl", tmp_path / "2.jsonl"
    first.write_text('\n{"id": 1}\n\n{"id": 2}\n')
    second.write_text('\n{"id": 3}\nmalformed tail\n')
    assert list(iter_jsonl_records([first, second], offset=1, limit=2)) == [
        (first, 4, {"id": 2}),
        (second, 2, {"id": 3}),
    ]


@pytest.mark.parametrize("bad_line", ["not-json", "null", "[]", "42"])
def test_error_policy_reports_location_and_can_continue(tmp_path, bad_line):
    source = tmp_path / "input.jsonl"
    source.write_text(bad_line + '\n{"id": 2}\n')
    with pytest.raises(JsonlReadError, match=r"input.jsonl at line 1"):
        list(iter_jsonl_records([source]))
    records = list(iter_jsonl_records([source], yield_errors=True))
    assert isinstance(records[0][2], JsonlReadError)
    assert records[1] == (source, 2, {"id": 2})


def test_missing_input_is_not_silently_ignored(tmp_path):
    missing = tmp_path / "missing.jsonl"
    with pytest.raises(JsonlReadError, match="Could not read"):
        list(iter_jsonl_records([missing]))
    records = list(iter_jsonl_records([missing], yield_errors=True))
    assert isinstance(records[0][2], JsonlReadError)


@pytest.mark.parametrize("kwargs", [{"offset": -1}, {"limit": 0}])
def test_rejects_invalid_window(kwargs):
    with pytest.raises(ValueError):
        list(iter_jsonl_records([], **kwargs))


def test_early_close_releases_current_source(tmp_path, monkeypatch):
    source = tmp_path / "input.jsonl"
    source.write_text(json.dumps({"id": 1}) + "\n" + json.dumps({"id": 2}) + "\n")
    opened = []
    original = Path.open

    def tracked_open(path, *args, **kwargs):
        stream = original(path, *args, **kwargs)
        opened.append(stream)
        return stream

    monkeypatch.setattr(Path, "open", tracked_open)
    with closing(iter_jsonl_records([source])) as records:
        assert next(records)[2] == {"id": 1}
        assert not opened[0].closed
    assert len(opened) == 1
    assert opened[0].closed
