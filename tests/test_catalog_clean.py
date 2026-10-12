"""Project cleaning, replay identity, and recovery before Kafka subscription."""

import json
from contextlib import closing
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from piddiplatsch.catalog import CatalogEntry, CatalogError, KafkaCheckpoint
from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.catalog.state import HarvestState
from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource


def seed(directory, project):
    with SQLiteCatalog(
        project.casefold(), directory / f"{project.casefold()}.sqlite"
    ) as catalog:
        with catalog.transaction() as transaction:
            transaction.put_item(
                CatalogEntry(
                    project,
                    {"id": f"{project}.old.v1", "collection": project},
                    f"{project}.old",
                    1,
                )
            )
            transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 100))


def publication(project="CMIP6"):
    return {
        "data": {
            "payload": {
                "method": "POST",
                "collection_id": project,
                "item": {
                    "id": f"{project}.new.v1",
                    "collection": project,
                    "type": "Feature",
                    "properties": {},
                    "assets": {},
                    "links": [],
                },
            }
        }
    }


def kafka_message(value, offset):
    message = MagicMock()
    message.error.return_value = None
    message.key.return_value = b"key"
    message.value.return_value = json.dumps(value).encode()
    message.topic.return_value = "topic"
    message.partition.return_value = 0
    message.offset.return_value = offset
    return message


@pytest.fixture
def kafka_client(monkeypatch):
    consumer_class = MagicMock()
    consumer_class.return_value.commit.return_value = []
    monkeypatch.setattr("confluent_kafka.Consumer", consumer_class)
    monkeypatch.setattr("piddiplatsch.consumer.signal.signal", MagicMock())
    return consumer_class


def test_selected_project_reset_reclaims_storage_and_leaves_other_projects(tmp_path):
    seed(tmp_path, "CMIP6")
    seed(tmp_path, "CMIP7")
    unrelated = tmp_path / "notes.json"
    unrelated.write_text("keep")
    with closing(CatalogHarvester(tmp_path, clean_projects=("cmip6",))) as harvester:
        assert harvester.kafka_group_suffix.startswith(".harvest.")
    with SQLiteCatalog("cmip6", tmp_path / "cmip6.sqlite") as catalog:
        assert list(catalog.iter_items()) == []
        assert catalog.get_checkpoint("topic", 0) is None
    with SQLiteCatalog("cmip7", tmp_path / "cmip7.sqlite") as catalog:
        assert catalog.get_item("CMIP7", "CMIP7.old.v1") is not None
        assert catalog.get_checkpoint("topic", 0).next_offset == 100
    assert unrelated.read_text() == "keep"
    with closing(CatalogHarvester(tmp_path)) as restarted:
        assert restarted.kafka_group_suffix == harvester.kafka_group_suffix


def test_cli_clean_all_uses_fresh_group_and_rebuilds_from_early_offsets(
    tmp_path, kafka_client
):
    directory = tmp_path / "outputs/catalog"
    seed(directory, "CMIP6")
    seed(directory, "CMIP7")
    kafka_client.return_value.poll.return_value = kafka_message(publication(), 0)
    result = CliRunner().invoke(
        cli, ["--no-progress", "harvest", "--clean-catalog", "--limit", "1"]
    )
    assert result.exit_code == 0, result.output
    first_group = kafka_client.call_args.args[0]["group.id"]
    assert first_group.startswith(config.get("kafka", "group.id") + ".harvest.")
    assert kafka_client.call_args.args[0]["auto.offset.reset"] == "earliest"
    with SQLiteCatalog("cmip6", directory / "cmip6.sqlite") as catalog:
        assert catalog.get_item("CMIP6", "CMIP6.old.v1") is None
        assert catalog.get_item("CMIP6", "CMIP6.new.v1") is not None
        assert catalog.get_checkpoint("topic", 0).next_offset == 1
    with SQLiteCatalog("cmip7", directory / "cmip7.sqlite") as catalog:
        assert list(catalog.iter_items()) == []
    assert not (directory / "cmip6plus.sqlite").exists()
    # Normal restart retains the rebuild group and doesn't reset populated items.
    result = CliRunner().invoke(cli, ["--no-progress", "harvest", "--limit", "1"])
    assert result.exit_code == 0, result.output
    assert kafka_client.call_args.args[0]["group.id"] == first_group
    with SQLiteCatalog("cmip6", directory / "cmip6.sqlite") as catalog:
        assert catalog.get_item("CMIP6", "CMIP6.new.v1") is not None
    # An explicitly requested second clean gets another fresh group.
    result = CliRunner().invoke(
        cli, ["--no-progress", "harvest", "--clean-catalog", "--limit", "1"]
    )
    assert result.exit_code == 0, result.output
    assert kafka_client.call_args.args[0]["group.id"] != first_group


def test_cli_clean_project_preserves_other_catalog_during_replay(
    tmp_path, kafka_client
):
    directory = tmp_path / "outputs/catalog"
    seed(directory, "CMIP6")
    seed(directory, "CMIP7")
    kafka_client.return_value.poll.side_effect = [
        kafka_message(publication("CMIP7"), 0),
        kafka_message(publication(), 1),
    ]
    result = CliRunner().invoke(
        cli, ["--no-progress", "harvest", "--clean-project", "CMIP6", "--limit", "2"]
    )
    assert result.exit_code == 0, result.output
    with SQLiteCatalog("cmip6", directory / "cmip6.sqlite") as catalog:
        assert catalog.get_item("CMIP6", "CMIP6.old.v1") is None
        assert catalog.get_item("CMIP6", "CMIP6.new.v1") is not None
    with SQLiteCatalog("cmip7", directory / "cmip7.sqlite") as catalog:
        assert catalog.get_item("CMIP7", "CMIP7.old.v1") is not None
        assert catalog.get_item("CMIP7", "CMIP7.new.v1") is None
        assert catalog.get_checkpoint("topic", 0).next_offset == 100


def test_pending_clean_is_recovered_after_interrupted_reset(tmp_path, monkeypatch):
    seed(tmp_path, "CMIP6")
    seed(tmp_path, "CMIP7")
    reset = SQLiteCatalog.reset

    def interrupt(catalog):
        if catalog.project == "cmip7":
            raise CatalogError("interrupted reset")
        reset(catalog)

    with monkeypatch.context() as patch:
        patch.setattr(SQLiteCatalog, "reset", interrupt)
        with pytest.raises(CatalogError, match="interrupted reset"):
            CatalogHarvester(tmp_path, clean_projects=("cmip6", "cmip7"))
    pending = HarvestState(tmp_path)
    assert pending.pending_clean == ("cmip6", "cmip7")
    with closing(CatalogHarvester(tmp_path)) as restarted:
        assert restarted.kafka_group_suffix == pending.kafka_group_suffix
        restarted.process_with_source("post", publication(), KafkaSource("topic", 0, 0))
    assert HarvestState(tmp_path).pending_clean == ()
    with SQLiteCatalog("cmip7", tmp_path / "cmip7.sqlite") as catalog:
        assert list(catalog.iter_items()) == []
        assert catalog.get_checkpoint("topic", 0) is None


def test_failed_state_write_does_not_clear_catalogs(tmp_path, monkeypatch):
    seed(tmp_path, "CMIP6")
    monkeypatch.setattr(
        "piddiplatsch.catalog.state.os.fsync",
        MagicMock(side_effect=OSError("disk failure")),
    )
    with pytest.raises(CatalogError, match="disk failure"):
        CatalogHarvester(tmp_path, clean_projects=("cmip6",))
    assert not (tmp_path / ".harvest-state.json").exists()
    assert not list(tmp_path.glob(".harvest-state-*.tmp"))
    with SQLiteCatalog("cmip6", tmp_path / "cmip6.sqlite") as catalog:
        assert catalog.get_item("CMIP6", "CMIP6.old.v1") is not None


@pytest.mark.parametrize(
    "arguments",
    [
        ["--clean-catalog", "--no-catalog"],
        ["--clean-catalog", "--clean-project", "cmip6"],
        ["--clean-project", "unknown"],
        ["--clean-project", "../cmip6"],
        ["--clean-project", "cmip6", "--clean-project", "CMIP6"],
    ],
)
def test_invalid_clean_options_do_not_touch_storage_or_subscribe(
    tmp_path, kafka_client, arguments
):
    result = CliRunner().invoke(cli, ["--no-progress", "harvest", *arguments])
    assert result.exit_code == 2, result.output
    kafka_client.assert_not_called()
    assert not (tmp_path / "outputs/catalog").exists()


def test_clean_requires_enabled_catalog_but_explicit_catalog_overrides_config(
    kafka_client,
):
    config._set("catalog", "enabled", False)
    result = CliRunner().invoke(cli, ["--no-progress", "harvest", "--clean-catalog"])
    assert result.exit_code == 2
    kafka_client.assert_not_called()
    kafka_client.return_value.poll.return_value = kafka_message(publication(), 0)
    result = CliRunner().invoke(
        cli,
        [
            "--no-progress",
            "harvest",
            "--catalog",
            "--clean-project",
            "cmip6",
            "--limit",
            "1",
        ],
    )
    assert result.exit_code == 0, result.output


def test_repeatable_project_clean_resets_each_selected_project(tmp_path, kafka_client):
    directory = tmp_path / "outputs/catalog"
    seed(directory, "CMIP6")
    seed(directory, "CMIP7")
    kafka_client.return_value.poll.return_value = kafka_message(publication(), 0)
    result = CliRunner().invoke(
        cli,
        [
            "--no-progress",
            "harvest",
            "--clean-project",
            "cmip6",
            "--clean-project",
            "cmip7",
            "--limit",
            "1",
        ],
    )
    assert result.exit_code == 0, result.output
    with SQLiteCatalog("cmip7", directory / "cmip7.sqlite") as catalog:
        assert list(catalog.iter_items()) == []


@pytest.mark.parametrize(
    "state",
    [
        "not JSON",
        "{}",
        json.dumps({"version": 99, "generation": "a" * 32, "pending_clean": []}),
        json.dumps(
            {"version": 1, "generation": "a" * 32, "pending_clean": ["../cmip6"]}
        ),
    ],
)
def test_invalid_state_is_fatal_instead_of_falling_back_to_old_offsets(tmp_path, state):
    (tmp_path / ".harvest-state.json").write_text(state)
    with pytest.raises(CatalogError, match="read harvest state"):
        CatalogHarvester(tmp_path)
