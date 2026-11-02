"""Shared Kafka checkpoints require a complete harvest rebuild."""

import json
from contextlib import closing

import pytest

from piddiplatsch.catalog import CatalogError
from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.catalog.state import HarvestState
from piddiplatsch.core.messages import KafkaSource


def seed(directory, publication_sample):
    with closing(CatalogHarvester(directory)) as harvester:
        for offset, project in enumerate(("cmip6", "cmip7")):
            message = publication_sample(project)
            harvester.process_with_source(
                "seed", message, KafkaSource("topic", 0, offset)
            )


def test_partial_clean_is_rejected_before_changing_shared_progress(
    tmp_path, publication_sample
):
    seed(tmp_path, publication_sample)
    with pytest.raises(CatalogError, match="shared Kafka checkpoints"):
        CatalogHarvester(tmp_path, clean_projects=("cmip6",))
    assert not HarvestState(tmp_path).pending_clean
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader.get_checkpoint("topic", 0).next_offset == 2
        assert reader.pending_work().count == 1


def test_complete_clean_is_atomic_and_never_compacts(
    tmp_path, publication_sample, monkeypatch
):
    seed(tmp_path, publication_sample)
    monkeypatch.setattr(
        SQLiteCatalog,
        "compact",
        lambda *args: pytest.fail("Automatic VACUUM is forbidden"),
    )
    with closing(
        CatalogHarvester(tmp_path, clean_projects=("cmip6", "cmip7"))
    ) as harvester:
        assert harvester.kafka_group_suffix.startswith(".harvest.")
    with SQLiteCatalog("cmip7", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader._execute("SELECT COUNT(*) FROM catalog_items").fetchone()[0] == 0
        assert reader._execute("SELECT COUNT(*) FROM work_items").fetchone()[0] == 0
        assert reader.get_checkpoint("topic", 0) is None


def test_interrupted_clean_restarts_before_new_events(tmp_path, publication_sample):
    seed(tmp_path, publication_sample)
    state = HarvestState(tmp_path)
    state.request_clean(("cmip6", "cmip7"))
    with closing(CatalogHarvester(tmp_path)):
        pass
    assert not HarvestState(tmp_path).pending_clean
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as reader:
        assert reader.get_checkpoint("topic", 0) is None


@pytest.mark.parametrize(
    "payload",
    [{}, {"version": 99}, {"version": 1, "generation": "bad", "pending_clean": []}],
)
def test_invalid_control_state_is_fatal(tmp_path, payload):
    (tmp_path / ".harvest-state.json").write_text(json.dumps(payload))
    with pytest.raises(CatalogError):
        CatalogHarvester(tmp_path)
