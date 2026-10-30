"""Mapping consumes complete local state and resolves history without networking."""

import json
from contextlib import closing
from copy import deepcopy

import pytest
from click.testing import CliRunner

from piddiplatsch.catalog.harvest import CatalogHarvester
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.catalog.state import HarvestState
from piddiplatsch.cli import cli
from piddiplatsch.config import config
from piddiplatsch.core.messages import KafkaSource
from piddiplatsch.exceptions import CatalogError, MaxErrorsExceededError
from piddiplatsch.runners.mapping import map_catalog


def sample(testdata_path, project):
    return json.loads(
        (testdata_path / "publication_samples" / f"{project}.json").read_text()
    )


def harvest(directory, messages):
    with closing(CatalogHarvester(directory)) as harvester:
        for offset, message in enumerate(messages):
            harvester.process_with_source(
                "harvest", message, KafkaSource("topic", 0, offset)
            )


def handles(output_dir, project):
    return [
        json.loads(line)
        for path in (output_dir / project / "handles").glob("*.jsonl")
        for line in path.read_text().splitlines()
    ]


@pytest.mark.parametrize("project", ["cmip6", "cmip6plus", "cmip7", "cordex-cmip6"])
@pytest.mark.parametrize("history", [True, False])
def test_map_predecessor_is_nearest_strictly_older_version_without_remote_lookup(
    tmp_path, testdata_path, monkeypatch, project, history
):
    output_dir = tmp_path / "outputs"
    config._set("consumer", "output_dir", str(output_dir))
    config._set("lookup", "enabled", True)
    config._set("consumer", "transient", {"preflight_stac": True})
    current = sample(testdata_path, project)
    item = current["data"]["payload"]["item"]
    older, later = deepcopy(current), deepcopy(current)
    dataset_id = item["id"].rsplit(".", 1)[0]
    older["data"]["payload"]["item"]["id"] = f"{dataset_id}.v20000101"
    later["data"]["payload"]["item"]["id"] = f"{dataset_id}.v20990101"
    harvest(output_dir / "catalog", [older, current, later])

    def unexpected_network(*args, **kwargs):
        pytest.fail("Catalog mapping must not use remote STAC or Handle services")

    monkeypatch.setattr("requests.get", unexpected_network)
    monkeypatch.setattr("requests.sessions.Session.request", unexpected_network)
    monkeypatch.setattr("piddiplatsch.lookup.api.get_lookup", unexpected_network)
    result = map_catalog(projects=[project], offset=1, limit=1, version_history=history)
    assert result.total == result.succeeded == 1
    dataset = handles(output_dir, project)[0]
    if history:
        assert (
            dataset["data"]["PREVIOUS_VERSION"]
            == older["data"]["payload"]["item"]["id"]
        )
    else:
        assert "PREVIOUS_VERSION" not in dataset["data"]


def test_harvested_patch_is_mapped_once_with_its_original_publication_time(
    tmp_path, testdata_path
):
    config._set("consumer", "output_dir", str(tmp_path))
    post = sample(testdata_path, "cmip7")
    item = post["data"]["payload"]["item"]
    time = "2026-10-12T10:15:00Z"
    patch = {
        "metadata": {"time": time},
        "data": {
            "payload": {
                "collection_id": item["collection"],
                "item_id": item["id"],
                "method": "PATCH",
                "patch": [
                    {"op": "add", "path": "/properties/retracted", "value": True}
                ],
            }
        },
    }
    harvest(tmp_path / "catalog", [post, patch])
    result = map_catalog(projects=["cmip7"])
    assert result.total == result.succeeded == 1
    dataset = handles(tmp_path, "cmip7")[0]
    assert dataset["data"]["RETRACTED_ON"].startswith("2026-10-12T10:15:00")
    with SQLiteCatalog(
        "cmip7", tmp_path / "catalog/cmip7.sqlite", read_only=True
    ) as catalog:
        stored = catalog.get_item(item["collection"], item["id"])
        assert stored.publication_time == time
        assert stored.item["properties"]["retracted"] is True


def test_map_all_skips_unharvested_projects_and_preserves_catalog_bytes(
    tmp_path, testdata_path
):
    config._set("consumer", "output_dir", str(tmp_path))
    harvest(tmp_path / "catalog", [sample(testdata_path, "cmip6")])
    path = tmp_path / "catalog/cmip6.sqlite"
    original = path.read_bytes()
    result = map_catalog(projects="all")
    assert result.total == result.succeeded == 1
    assert path.read_bytes() == original
    assert not (tmp_path / "catalog/cmip7.sqlite").exists()


def test_missing_explicit_catalog_is_checked_before_any_handle_output(
    tmp_path, testdata_path
):
    config._set("consumer", "output_dir", str(tmp_path))
    harvest(tmp_path / "catalog", [sample(testdata_path, "cmip6")])
    with pytest.raises(CatalogError, match="run harvest first"):
        map_catalog(projects=["cmip6", "cmip7"])
    assert not list(tmp_path.rglob("handles_*.jsonl"))


def test_pending_clean_blocks_mapping(tmp_path, testdata_path):
    config._set("consumer", "output_dir", str(tmp_path))
    harvest(tmp_path / "catalog", [sample(testdata_path, "cmip6")])
    HarvestState(tmp_path / "catalog").request_clean(("cmip6",))
    with pytest.raises(CatalogError, match="clean is incomplete"):
        map_catalog(projects=["cmip6"])


def test_catalog_mode_rejects_patch_envelopes_instead_of_fetching_or_reapplying(
    tmp_path, testdata_path
):
    from piddiplatsch.plugins.cmip6.processor import CMIP6Processor

    harvest(tmp_path, [sample(testdata_path, "cmip6")])
    with SQLiteCatalog("cmip6", tmp_path / "cmip6.sqlite", read_only=True) as catalog:
        processor = CMIP6Processor(catalog_reader=catalog)
        assert processor.stac_client is None
        with pytest.raises(ValueError, match="apply patches during harvest"):
            processor.process("patch", {"data": {"payload": {"method": "PATCH"}}})


def test_cli_history_option_overrides_catalog_configuration(tmp_path, testdata_path):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("catalog", "version_history", True)
    original = sample(testdata_path, "cmip6")
    older = deepcopy(original)
    older["data"]["payload"]["item"]["id"] = (
        original["data"]["payload"]["item"]["id"].rsplit(".", 1)[0] + ".v20000101"
    )
    harvest(tmp_path / "catalog", [older, original])
    result = CliRunner().invoke(
        cli,
        [
            "--no-progress",
            "map",
            "--project",
            "cmip6",
            "--offset",
            "1",
            "--limit",
            "1",
            "--no-version-history",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "PREVIOUS_VERSION" not in handles(tmp_path, "cmip6")[0]["data"]


def test_mapping_failure_is_inspectable_and_command_can_be_repeated(
    tmp_path, testdata_path, monkeypatch
):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("consumer", "max_errors", 1)
    original = sample(testdata_path, "cmip6")
    older = deepcopy(original)
    older_id = (
        original["data"]["payload"]["item"]["id"].rsplit(".", 1)[0] + ".v20000101"
    )
    older["data"]["payload"]["item"]["id"] = older_id
    harvest(tmp_path / "catalog", [older, original])

    def fail_handle(*args, **kwargs):
        raise RuntimeError("handle output failed")

    with monkeypatch.context() as patch:
        patch.setattr("piddiplatsch.handles.api.HandleAPI.add", fail_handle)
        with pytest.raises(MaxErrorsExceededError):
            map_catalog(projects=["cmip6"], offset=1, limit=1, version_history=True)
    failure_path = next((tmp_path / "cmip6/failures/r0").glob("*.jsonl"))
    failure = json.loads(failure_path.read_text())
    assert failure["metadata"]["previous_version"] == older_id
    before = failure_path.read_bytes()
    repeated = CliRunner().invoke(
        cli,
        [
            "--no-progress",
            "map",
            "--project",
            "cmip6",
            "--date",
            "last",
            "--offset",
            "1",
            "--limit",
            "1",
            "--version-history",
        ],
    )
    assert repeated.exit_code == 0, repeated.output
    assert failure_path.read_bytes() == before
    assert handles(tmp_path, "cmip6")[0]["data"]["PREVIOUS_VERSION"] == older_id


def test_recovered_remote_item_maps_dataset_and_files_without_network(
    tmp_path, testdata_path, monkeypatch
):
    config._set("consumer", "output_dir", str(tmp_path))
    current = sample(testdata_path, "cmip7")
    item = current["data"]["payload"]["item"]
    fetched = deepcopy(item)
    # This operation must not be applied to the fetched current state.
    patch = {
        "metadata": current.get("metadata", {}),
        "data": {
            "payload": {
                "method": "PATCH",
                "collection_id": item["collection"],
                "item_id": item["id"],
                "patch": [{"op": "remove", "path": "/assets"}],
            }
        },
    }
    monkeypatch.setattr(
        "piddiplatsch.utils.stac.RemoteStacClient.get_item", lambda *args: fetched
    )
    harvest(tmp_path / "catalog", [patch])

    def unexpected_network(*args, **kwargs):
        pytest.fail("Mapping a recovered catalog entry must remain offline")

    monkeypatch.setattr("requests.get", unexpected_network)
    monkeypatch.setattr("requests.sessions.Session.request", unexpected_network)
    result = map_catalog(projects=["cmip7"])
    assert result.succeeded == 1
    assert len(handles(tmp_path, "cmip7")) == 1 + len(item["assets"])
