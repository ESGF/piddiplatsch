import json

from piddiplatsch.persist.retry import RetryRunner


def test_retry_on_skipped_jsonl(tmp_path):
    # Prepare a skipped JSONL file with a valid item payload
    f = tmp_path / "skipped_items.jsonl"
    record = {
        "data": {
            "payload": {
                "item": {
                    "id": "cmip6.foo.bar.baz.exp.r1i1p1f1.table.var.grid.v20200101",
                    "type": "Feature",
                    "collection": "cmip6",
                    "properties": {},
                    "links": [],
                    "assets": {},
                }
            }
        },
        "metadata": {"time": "2024-01-01T00:00:00Z"},
    }
    f.write_text(json.dumps(record) + "\n", encoding="utf-8")

    runner = RetryRunner(
        projects=["cmip6"],
        failure_dir=tmp_path / "failures",
        delete_after=False,
        dry_run=True,
    )
    result = runner.run_file(f)

    # It should attempt to process the single message
    assert result.total == 1
