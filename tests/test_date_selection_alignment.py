"""Queue selectors use local creation days; publication preserves work order."""

import json
from contextlib import closing
from copy import deepcopy
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from piddiplatsch.catalog import CatalogEntry
from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.commands.helper import parse_date_selector
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.result import PublishResult
from piddiplatsch.runners.mapping import map_catalog
from piddiplatsch.runners.sync import SyncWorker


def seed(tmp_path, testdata_path):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("consumer", "projects", ["cmip6"])
    config._set("batch", None, {"max_items": 2, "trigger_items": 1})
    original = json.loads(
        (testdata_path / "publication_samples/cmip6.json").read_text()
    )
    with closing(CatalogHarvester(tmp_path)) as harvester:
        for index in range(3):
            message = deepcopy(original)
            item = message["data"]["payload"]["item"]
            item["id"] = item["id"].rsplit(".", 1)[0] + f".v{20000101 + index}"
            with patch(
                "piddiplatsch.catalog.sqlite._now",
                return_value=f"2026-08-0{index + 1}T12:00:00+00:00",
            ):
                harvester.process_with_source(
                    "seed", message, KafkaSource("topic", 0, index)
                )


def test_date_window_applies_before_offset_limit(tmp_path, testdata_path):
    seed(tmp_path, testdata_path)
    result = map_catalog(
        projects=["cmip6"],
        from_date=datetime(2026, 8, 2),
        to_date=datetime(2026, 8, 3),
        offset=1,
        limit=1,
        version_history=True,
    )
    assert result.total == result.succeeded == 1
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader._execute(
            "SELECT id FROM work_items WHERE state='mapped'"
        ).fetchall() == [(3,)]
    path = next((tmp_path / "batches/cmip6/ready").glob("*/handles.jsonl"))
    dataset = json.loads(path.read_text().splitlines()[0])
    assert dataset["data"]["PREVIOUS_VERSION"].endswith(".v20000102")


def test_last_then_all_only_maps_remaining_pending_work(tmp_path, testdata_path):
    seed(tmp_path, testdata_path)
    assert map_catalog(projects=["cmip6"], input_date="last").total == 1
    assert map_catalog(projects=["cmip6"], input_date="all").total == 2
    assert map_catalog(projects=["cmip6"]).total == 0


def test_date_filter_does_not_decode_excluded_work(tmp_path, testdata_path):
    seed(tmp_path, testdata_path)
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db") as writer, writer.transaction():
        writer._execute("UPDATE work_items SET stac_blob=? WHERE id=1", (b"corrupt",))
    assert (
        map_catalog(projects=["cmip6"], input_date=datetime(2026, 8, 3)).succeeded == 1
    )


def test_publication_orders_work_before_batch_creation_time(tmp_path, testdata_path):
    seed(tmp_path, testdata_path)
    assert map_catalog(projects=["cmip6"], input_date="last").total == 1
    calls = []

    class Publisher:
        def run(self, paths, **kwargs):
            manifest = json.loads((paths[0].parent / "manifest.json").read_text())
            calls.append([outcome["id"] for outcome in manifest["outcomes"]])
            for _, position, record in kwargs.get("record_source", ()):
                kwargs["journal"].record(
                    SimpleNamespace(
                        line_number=position, handle=record["handle"], error=None
                    )
                )
            return PublishResult(
                total=manifest["handle_count"], succeeded=manifest["handle_count"]
            )

    worker = SyncWorker(publisher=Publisher())
    worker.run(force_map=True)
    assert not worker.errors
    assert calls == [[1, 2], [3]]


@pytest.mark.parametrize("command", ["map", "publish"])
@pytest.mark.parametrize(
    "options, message",
    [
        (["--from", "today"], "must be provided together"),
        (["--to", "today"], "must be provided together"),
        (["--from", "today", "--to", "yesterday"], "on or before"),
        (["--from", "last", "--to", "today"], "range boundary"),
        (["--from", "all", "--to", "today"], "range boundary"),
        (["--date", "today", "--from", "today", "--to", "today"], "cannot be combined"),
    ],
)
def test_shared_invalid_date_selections_are_rejected_before_processing(
    command, options, message
):
    result = CliRunner().invoke(cli, [command, "--project", "cmip6", *options])
    assert result.exit_code == 2
    assert message in result.output


@pytest.mark.parametrize(
    "timestamp, expected",
    [
        ("2026-10-12T00:30:00+02:00", date(2026, 10, 11)),
        ("2026-10-12T10:00:00", date(2026, 10, 12)),
        (None, None),
        ("invalid", None),
    ],
)
def test_catalog_event_day_is_normalized_to_utc(timestamp, expected):
    value = CatalogEntry("CMIP6", {"id": "dataset.v1"}, "dataset", 1, timestamp)
    assert value.event_date == expected


def test_relative_parser_accepts_today_zero_and_all():
    assert parse_date_selector("today-0", today=date(2026, 10, 12)) == datetime(
        2026, 10, 12
    )
    assert parse_date_selector("all") == "all"
