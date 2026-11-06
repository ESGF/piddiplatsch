"""Unknown-project policy preserves archives and shared source progress."""

import json
from contextlib import closing
from unittest.mock import Mock

import pytest

from piddiplatsch.catalog import CatalogError
from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.config import config
from piddiplatsch.config.schema import CatalogConfig
from piddiplatsch.core.messages import KafkaSource, SourceMessage
from piddiplatsch.core.pipeline import ProcessingPipeline
from piddiplatsch.monitoring.stats import stats
from piddiplatsch.persist.dump import DumpRecorder


def publication(project):
    return {
        "data": {
            "payload": {
                "collection_id": project,
                "method": "POST",
                "item": {
                    "id": f"{project}.dataset.v1",
                    "collection": project,
                    "type": "Feature",
                    "properties": {},
                    "assets": {},
                    "links": [],
                },
            }
        }
    }


def process(harvester, messages):
    pipeline = ProcessingPipeline(messages, harvester, dump_messages=True)
    try:
        return pipeline.run()
    finally:
        pipeline.close_progress()


@pytest.mark.parametrize("policy", [None, False])
def test_unknown_project_stops_by_default_without_acknowledging(tmp_path, policy):
    config._set("consumer", "output_dir", str(tmp_path))
    if policy is not None:
        config._set("catalog", "ignore_unknown_projects", policy)
    unknown, following = Mock(), Mock()
    with closing(CatalogHarvester(tmp_path)) as harvester:
        with pytest.raises(CatalogError, match="not registered"):
            process(
                harvester,
                [
                    SourceMessage(
                        "unknown",
                        publication("FUTURE"),
                        KafkaSource("topic", 0, 0),
                        unknown,
                    ),
                    SourceMessage(
                        "known",
                        publication("CMIP6"),
                        KafkaSource("topic", 0, 1),
                        following,
                    ),
                ],
            )
    unknown.assert_not_called()
    following.assert_not_called()
    assert not (tmp_path / "piddi.db").exists()
    records = [
        json.loads(line)
        for path in (tmp_path / "dump").glob("dump_messages_*.jsonl")
        for line in path.read_text().splitlines()
    ]
    assert len(records) == 1
    assert records[0]["payload"] == publication("FUTURE")


def test_ignore_archives_and_counts_unknown_before_ack_and_continues_disabled_known_project(
    tmp_path, caplog
):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("consumer", "projects", ["cmip6"])
    config._set("catalog", "ignore_unknown_projects", True)
    first, last = Mock(), Mock()

    def acknowledge_unknown():
        assert list((tmp_path / "dump").glob("unknown_projects_*.jsonl"))
        with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as reader:
            assert reader.get_checkpoint("topic", 0).next_offset == 2

    unknown = Mock(side_effect=acknowledge_unknown)
    with closing(CatalogHarvester(tmp_path)) as harvester:
        result = process(
            harvester,
            [
                SourceMessage(
                    "first", publication("CMIP6"), KafkaSource("topic", 0, 0), first
                ),
                SourceMessage(
                    "unknown",
                    publication("FUTURE"),
                    KafkaSource("topic", 0, 1),
                    unknown,
                ),
                SourceMessage(
                    "disabled", publication("CMIP7"), KafkaSource("topic", 0, 2), last
                ),
            ],
        )
    for callback in (first, unknown, last):
        callback.assert_called_once()
    assert (result.succeeded, result.filtered, result.failed) == (2, 1, 0)
    assert stats.summary()["projects"]["future"]["filtered"] == 1
    assert stats.filtered_messages == 1
    assert "Ignored unknown project" in caplog.text
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader.get_checkpoint("topic", 0).next_offset == 3
        assert reader._execute(
            "SELECT project FROM work_items ORDER BY id"
        ).fetchall() == [("cmip6",), ("cmip7",)]
        assert reader._execute("SELECT COUNT(*) FROM stats").fetchone() == (0,)
    archive = next((tmp_path / "dump").glob("unknown_projects_*.jsonl"))
    assert json.loads(archive.read_text()) == {
        "key": "unknown",
        "payload": publication("FUTURE"),
        "source": {"topic": "topic", "partition": 0, "offset": 1},
    }


def test_ignore_requires_durable_dump_before_advancing_checkpoint(
    tmp_path, monkeypatch
):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("catalog", "ignore_unknown_projects", True)
    original = DumpRecorder.record

    def fail_unknown_archive(self, *args, **kwargs):
        if self.prefix == "unknown_projects":
            raise OSError("archive disk failed")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(DumpRecorder, "record", fail_unknown_archive)
    acknowledge = Mock()
    with closing(CatalogHarvester(tmp_path)) as harvester:
        with pytest.raises(OSError, match="archive disk failed"):
            process(
                harvester,
                [
                    SourceMessage(
                        "unknown",
                        publication("FUTURE"),
                        KafkaSource("topic", 0, 0),
                        acknowledge,
                    )
                ],
            )
    acknowledge.assert_not_called()
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader.get_checkpoint("topic", 0) is None
        assert reader._execute("SELECT COUNT(*) FROM work_items").fetchone() == (0,)


def test_ignore_also_archives_direct_calls_and_deduplicates_replay(tmp_path):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("catalog", "ignore_unknown_projects", True)
    source = KafkaSource("topic", 0, 0)
    with closing(CatalogHarvester(tmp_path)) as harvester:
        assert (
            harvester.process_with_source(
                "key", publication("FUTURE"), source
            ).filtered_reason
            == "Unknown project ignored"
        )
    with closing(CatalogHarvester(tmp_path)) as harvester:
        assert (
            harvester.process_with_source(
                "key", publication("FUTURE"), source
            ).filtered_reason
            == "Kafka message already harvested"
        )
    assert (
        len(
            next((tmp_path / "dump").glob("unknown_projects_*.jsonl"))
            .read_text()
            .splitlines()
        )
        == 1
    )


@pytest.mark.parametrize("problem", ["missing", "conflicting"])
def test_ignore_policy_does_not_hide_invalid_project_identity(tmp_path, problem):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("catalog", "ignore_unknown_projects", True)
    message = publication("FUTURE")
    if problem == "missing":
        message["data"]["payload"].pop("collection_id")
        message["data"]["payload"]["item"].pop("collection")
    else:
        message["data"]["payload"]["item"]["collection"] = "CMIP6"
    with closing(CatalogHarvester(tmp_path)) as harvester:
        with pytest.raises((CatalogError, ValueError)):
            harvester.process_with_source("key", message, KafkaSource("topic", 0, 0))
    assert not list((tmp_path / "dump").glob("unknown_projects_*.jsonl"))
    assert not (tmp_path / "piddi.db").exists()


def test_unknown_project_policy_is_validated():
    assert CatalogConfig().ignore_unknown_projects is False
    assert CatalogConfig(ignore_unknown_projects=True).ignore_unknown_projects is True
    with pytest.raises(ValueError):
        CatalogConfig(ignore_unknown_projects="ignore")


def test_live_status_exposes_ignored_project_count(tmp_path):
    from piddiplatsch.monitoring.status import read_status

    config._set("consumer", "output_dir", str(tmp_path))
    config._set("catalog", "ignore_unknown_projects", True)
    stats.configure_for_run(
        enable_db=True, command="harvest", selected_projects=("cmip6",)
    )
    with closing(CatalogHarvester(tmp_path)) as harvester:
        process(
            harvester,
            [
                SourceMessage(
                    "unknown", publication("FUTURE"), KafkaSource("topic", 0, 0)
                )
            ],
        )
    stats.close(status="completed")
    status = read_status(tmp_path / "piddi.db")
    project = next(
        row for row in status["runs"][0]["projects"] if row["project"] == "future"
    )
    assert project["filtered"] == 1
    assert project["failed"] == project["catalog_writes"] == 0


@pytest.mark.parametrize(
    "configured,flag,ignored",
    [
        (False, None, False),
        (True, None, True),
        (False, "--ignore-unknown-projects", True),
        (True, "--stop-on-unknown-projects", False),
    ],
)
def test_harvest_unknown_project_flags_override_configuration_for_one_run(
    tmp_path, monkeypatch, configured, flag, ignored
):
    from click.testing import CliRunner

    from piddiplatsch.cli import cli

    config._set("consumer", "output_dir", str(tmp_path / "outputs"))
    config._set("catalog", "ignore_unknown_projects", configured)
    monkeypatch.setattr("piddiplatsch.consumer.signal.signal", Mock())
    archive = tmp_path / "input.jsonl"
    archive.write_text(
        json.dumps(
            {
                "source": {"topic": "topic", "partition": 0, "offset": 0},
                "key": "unknown",
                "payload": publication("FUTURE"),
            }
        )
        + "\n"
    )
    args = ["--silent", "harvest", "--archive", str(archive)]
    if flag:
        args.append(flag)
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == (0 if ignored else 65), result.output
    assert config.get("catalog", "ignore_unknown_projects") is configured
    assert (
        bool(list((tmp_path / "outputs/dump").glob("unknown_projects_*.jsonl")))
        is ignored
    )


def test_unknown_project_flags_require_catalog():
    from click.testing import CliRunner

    from piddiplatsch.cli import cli

    result = CliRunner().invoke(
        cli, ["harvest", "--no-catalog", "--ignore-unknown-projects"]
    )
    assert result.exit_code == 2
    assert "require --catalog" in result.output
