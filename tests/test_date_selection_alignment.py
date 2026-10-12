"""Map's source days and publish's daily files use the same selectors."""

import json
import sqlite3
import zlib
from contextlib import closing
from copy import deepcopy
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from piddiplatsch.catalog import CatalogEntry, CatalogError
from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.commands.helper import parse_date_selector
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.result import PublishResult
from piddiplatsch.runners.mapping import map_catalog


def seed(tmp_path, testdata_path, days):
    config._set("consumer", "output_dir", str(tmp_path))
    original = json.loads(
        (testdata_path / "publication_samples/cmip6.json").read_text()
    )
    with closing(CatalogHarvester(tmp_path / "catalog")) as harvester:
        for index, day in enumerate(days):
            message = deepcopy(original)
            message["metadata"]["time"] = f"{day.isoformat()}T12:00:00Z"
            item = message["data"]["payload"]["item"]
            item["id"] = item["id"].rsplit(".", 1)[0] + f".v{20000101 + index}"
            harvester.process_with_source(
                "seed", message, KafkaSource("topic", 0, index)
            )


@pytest.mark.parametrize(
    "selector, days_ago",
    [("today", 0), ("today-1", 1), ("yesterday", 1), ("today-2", 2)],
)
def test_same_relative_selector_maps_and_publishes_matching_batch(
    tmp_path, testdata_path, monkeypatch, selector, days_ago
):
    today = date.today()
    seed(tmp_path, testdata_path, [today - timedelta(days=n) for n in (2, 1, 0)])
    selected = today - timedelta(days=days_ago)
    result = CliRunner().invoke(
        cli, ["--no-progress", "map", "--project", "cmip6", "--date", selector]
    )
    assert result.exit_code == 0, result.output
    assert "Mapped 1/1" in result.output
    files = tuple((tmp_path / "cmip6/handles").glob("*.jsonl"))
    assert files == (
        tmp_path / "cmip6/handles" / f"handles_{selected.isoformat()}.jsonl",
    )
    publisher = MagicMock()
    publisher.return_value.run.return_value = PublishResult(total=2, succeeded=2)
    monkeypatch.setattr("piddiplatsch.commands.publish.HandlePublisher", publisher)
    result = CliRunner().invoke(
        cli, ["--no-progress", "publish", "--project", "cmip6", "--date", selector]
    )
    assert result.exit_code == 0, result.output
    assert publisher.return_value.run.call_args.args[0] == files


def test_inclusive_range_maps_and_publishes_the_same_days(
    tmp_path, testdata_path, monkeypatch
):
    today = date.today()
    days = [today - timedelta(days=n) for n in (4, 2, 1, 0)]
    seed(tmp_path, testdata_path, days)
    options = ["--project", "cmip6", "--from", "today-4", "--to", "today-1"]
    result = CliRunner().invoke(cli, ["--no-progress", "map", *options])
    assert result.exit_code == 0, result.output
    assert "Mapped 3/3" in result.output
    expected = tuple(
        tmp_path / "cmip6/handles" / f"handles_{day.isoformat()}.jsonl"
        for day in days[:3]
    )
    assert sorted((tmp_path / "cmip6/handles").glob("*.jsonl")) == list(expected)
    publisher = MagicMock()
    publisher.return_value.run.return_value = PublishResult(total=6, succeeded=6)
    monkeypatch.setattr("piddiplatsch.commands.publish.HandlePublisher", publisher)
    result = CliRunner().invoke(
        cli, ["--no-progress", "publish", *options, "--offset", "1", "--limit", "2"]
    )
    assert result.exit_code == 0, result.output
    assert publisher.return_value.run.call_args.args[0] == expected
    assert publisher.return_value.run.call_args.kwargs["offset"] == 1


def test_last_and_all_selectors_are_supported_by_both_commands(
    tmp_path, testdata_path, monkeypatch
):
    days = [date(2026, 8, 1), date(2026, 8, 5)]
    seed(tmp_path, testdata_path, days)
    assert map_catalog(projects=["cmip6"], input_date="last").total == 1
    assert (tmp_path / "cmip6/handles/handles_2026-08-05.jsonl").is_file()
    assert map_catalog(projects=["cmip6"], input_date="all").total == 2
    publisher = MagicMock()
    publisher.return_value.run.return_value = PublishResult(total=4, succeeded=4)
    monkeypatch.setattr("piddiplatsch.commands.publish.HandlePublisher", publisher)
    result = CliRunner().invoke(
        cli, ["--no-progress", "publish", "--project", "cmip6", "--date", "all"]
    )
    assert result.exit_code == 0, result.output
    assert [path.name for path in publisher.return_value.run.call_args.args[0]] == [
        "handles_2026-08-01.jsonl",
        "handles_2026-08-05.jsonl",
    ]


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


def test_date_window_applies_before_offset_limit_and_keeps_history_outside_window(
    tmp_path, testdata_path
):
    days = [date(2026, 8, 1), date(2026, 8, 2), date(2026, 8, 3)]
    seed(tmp_path, testdata_path, days)
    result = map_catalog(
        projects=["cmip6"],
        from_date=datetime(2026, 8, 2),
        to_date=datetime(2026, 8, 3),
        offset=1,
        limit=1,
        version_history=True,
    )
    assert result.total == result.succeeded == 1
    files = list((tmp_path / "cmip6/handles").glob("*.jsonl"))
    assert [path.name for path in files] == ["handles_2026-08-03.jsonl"]
    dataset = json.loads(files[0].read_text().splitlines()[0])
    assert dataset["data"]["PREVIOUS_VERSION"].endswith(".v20000102")


def test_date_filter_does_not_decompress_excluded_items(tmp_path, testdata_path):
    import sqlite3

    seed(tmp_path, testdata_path, [date(2026, 8, 1), date(2026, 8, 2)])
    path = tmp_path / "catalog/cmip6.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE items SET payload = ? WHERE event_date = ?",
            (b"corrupt", "2026-08-01"),
        )
    assert (
        map_catalog(projects=["cmip6"], input_date=datetime(2026, 8, 2)).succeeded == 1
    )
    with pytest.raises(CatalogError, match="Invalid stored"):
        map_catalog(projects=["cmip6"], input_date=datetime(2026, 8, 1))


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


def test_indexed_event_day_updates_when_item_is_replaced(tmp_path):
    path = tmp_path / "cmip6.sqlite"
    with SQLiteCatalog("cmip6", path) as catalog:
        first = CatalogEntry(
            "CMIP6", {"id": "dataset.v1"}, "dataset", 1, "2026-10-02T10:00:00Z"
        )
        second = CatalogEntry(
            "CMIP6", {"id": "dataset.v2"}, "dataset", 2, "2026-10-01T10:00:00Z"
        )
        with catalog.transaction() as transaction:
            transaction.put_item(first)
            transaction.put_item(second)
            assert transaction.latest_event_date() == date(2026, 10, 2)
        assert [
            value.item_id
            for value in catalog.iter_items(
                from_date=date(2026, 10, 1), to_date=date(2026, 10, 2)
            )
        ] == [second.item_id, first.item_id]
        moved = CatalogEntry("CMIP6", second.item, "dataset", 2, "2026-10-03T10:00:00Z")
        with catalog.transaction() as transaction:
            transaction.put_item(moved)
        assert (
            list(
                catalog.iter_items(
                    from_date=date(2026, 10, 1), to_date=date(2026, 10, 1)
                )
            )
            == []
        )
        assert catalog.latest_event_date() == date(2026, 10, 3)


def test_harvest_startup_backfills_legacy_date_index_without_new_messages(tmp_path):
    path = tmp_path / "cmip6.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE catalog_metadata (singleton INTEGER PRIMARY KEY, project TEXT, schema_version INTEGER)"
        )
        connection.execute("INSERT INTO catalog_metadata VALUES (1, 'cmip6', 2)")
        connection.execute(
            "CREATE TABLE items (collection_id TEXT, item_id TEXT, dataset_id TEXT, "
            "version_number INTEGER, payload BLOB, publication_time TEXT, PRIMARY KEY (collection_id, item_id))"
        )
        for index in range(500):
            item = {"id": f"dataset.v{index}"}
            day = 1 + index % 5
            connection.execute(
                "INSERT INTO items VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "CMIP6",
                    item["id"],
                    "dataset",
                    index,
                    zlib.compress(json.dumps(item).encode()),
                    f"2026-10-{day:02}T12:00:00Z",
                ),
            )
        compressed_size = connection.execute(
            "SELECT SUM(length(payload)) FROM items"
        ).fetchone()[0]
    with SQLiteCatalog("cmip6", path, read_only=True) as old:
        assert len(list(old.iter_items())) == 500
        with pytest.raises(CatalogError, match="run harvest once"):
            old.latest_event_date()
    with closing(CatalogHarvester(tmp_path)):
        pass
    with SQLiteCatalog("cmip6", path, read_only=True) as upgraded:
        assert upgraded.latest_event_date() == date(2026, 10, 5)
        assert (
            len(
                list(
                    upgraded.iter_items(
                        from_date=date(2026, 10, 3), to_date=date(2026, 10, 3)
                    )
                )
            )
            == 100
        )
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute("SELECT SUM(length(payload)) FROM items").fetchone()[0]
            == compressed_size
        )
        plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT payload FROM items WHERE event_date >= ? AND event_date <= ? ORDER BY event_date, collection_id, item_id",
            ("2026-10-02", "2026-10-04"),
        ).fetchall()
        assert any("items_event_dates" in row[3] for row in plan)
        assert not any("TEMP B-TREE" in row[3] for row in plan)
