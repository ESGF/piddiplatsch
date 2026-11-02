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
from piddiplatsch.exceptions import BatchError, CatalogError
from piddiplatsch.runners.mapping import map_catalog


def harvest(directory, messages):
    config._set("catalog", "directory", str(directory))
    with closing(CatalogHarvester(directory)) as harvester:
        for offset, message in enumerate(messages):
            harvester.process_with_source(
                "harvest", message, KafkaSource("topic", 0, offset)
            )


def handles(output_dir, project):
    return [
        json.loads(line)
        for path in (output_dir / "batches" / project).glob("ready/*/handles.jsonl")
        for line in path.read_text().splitlines()
    ]


@pytest.mark.parametrize("project", ["cmip6", "cmip6plus", "cmip7", "cordex-cmip6"])
@pytest.mark.parametrize("history", [True, False])
def test_map_predecessor_is_nearest_strictly_older_version_without_remote_lookup(
    tmp_path, publication_sample, monkeypatch, caplog, project, history
):
    output_dir = tmp_path / "outputs"
    config._set("consumer", "output_dir", str(output_dir))
    config._set("lookup", "enabled", True)
    current = publication_sample(project)
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
    with caplog.at_level("INFO"):
        result = map_catalog(
            projects=[project], offset=1, limit=1, version_history=history
        )
    assert result.total == result.succeeded == 1
    dataset = handles(output_dir, project)[0]
    if history:
        assert (
            dataset["data"]["PREVIOUS_VERSION"]
            == older["data"]["payload"]["item"]["id"]
        )
    else:
        assert "PREVIOUS_VERSION" not in dataset["data"]
    summaries = [line for line in caplog.messages if "version lookups:" in line]
    if history:
        assert len(summaries) == 1
        assert f"[sync/map] {project} | batch " in summaries[0]
        assert (
            "version lookups: 1 | older version found: 1 | none: 0 | errors: 0"
            in summaries[0]
        )
        assert "s total | " in summaries[0]
        assert "ms avg | " in summaries[0]
        assert "% of mapping time" in summaries[0]
    else:
        assert summaries == []


def test_version_lookup_summary_counts_absent_versions_and_resets_each_batch(
    tmp_path, publication_sample, caplog
):
    config._set("consumer", "output_dir", str(tmp_path))
    harvest(
        tmp_path / "catalog",
        [
            publication_sample("cmip6", version=version)
            for version in (20000101, 20000102)
        ],
    )
    with caplog.at_level("INFO"):
        assert (
            map_catalog(projects=["cmip6"], limit=1, version_history=True).succeeded
            == 1
        )
        assert (
            map_catalog(projects=["cmip6"], limit=1, version_history=True).succeeded
            == 1
        )
    summaries = [line for line in caplog.messages if "version lookups:" in line]
    assert len(summaries) == 2
    assert (
        "version lookups: 1 | older version found: 0 | none: 1 | errors: 0"
        in summaries[0]
    )
    assert (
        "version lookups: 1 | older version found: 1 | none: 0 | errors: 0"
        in summaries[1]
    )


def test_version_lookup_failure_is_logged_separately_from_absent_version(
    tmp_path, publication_sample, monkeypatch, caplog
):
    config._set("consumer", "output_dir", str(tmp_path))
    harvest(tmp_path / "catalog", [publication_sample("cmip6")])

    def fail_lookup(*args):
        raise CatalogError("lookup failed")

    monkeypatch.setattr(SQLiteCatalog, "find_previous_version", fail_lookup)
    with caplog.at_level("INFO"):
        with pytest.raises(BatchError, match="lookup failed"):
            map_catalog(projects=["cmip6"], version_history=True)
    summaries = [line for line in caplog.messages if "version lookups:" in line]
    assert len(summaries) == 1
    assert (
        "version lookups: 1 | older version found: 0 | none: 0 | errors: 1"
        in summaries[0]
    )


def test_harvested_patch_is_mapped_once_with_its_original_publication_time(
    tmp_path, publication_sample
):
    config._set("consumer", "output_dir", str(tmp_path))
    post = publication_sample("cmip7")
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
    assert result.total == result.succeeded == 2
    dataset = handles(tmp_path, "cmip7")[2]
    assert dataset["data"]["RETRACTED_ON"].startswith("2026-10-12T10:15:00")
    with SQLiteCatalog(
        "cmip7", tmp_path / "catalog/piddi.db", read_only=True
    ) as catalog:
        stored = catalog.get_item(item["collection"], item["id"])
        assert stored.publication_time == time
        assert stored.item["properties"]["retracted"] is True


def test_map_all_skips_unharvested_projects_and_keeps_catalog_records(
    tmp_path, publication_sample
):
    config._set("consumer", "output_dir", str(tmp_path))
    harvest(tmp_path / "catalog", [publication_sample("cmip6")])
    path = tmp_path / "catalog/piddi.db"
    original = path.read_bytes()
    result = map_catalog(projects="all")
    assert result.total == result.succeeded == 1
    assert original
    with SQLiteCatalog("cmip6", path, read_only=True) as catalog:
        assert len(list(catalog.iter_items())) == 1
        assert catalog.pending_work().count == 0


def test_missing_database_produces_no_mapping_output(tmp_path):
    assert map_catalog(directory=tmp_path, projects=["cmip6"]).total == 0
    assert not list(tmp_path.rglob("handles.jsonl"))


def test_pending_clean_blocks_mapping(tmp_path, publication_sample):
    config._set("consumer", "output_dir", str(tmp_path))
    harvest(tmp_path / "catalog", [publication_sample("cmip6")])
    HarvestState(tmp_path / "catalog").request_clean(("cmip6",))
    with pytest.raises(CatalogError, match="clean is incomplete"):
        map_catalog(projects=["cmip6"])


def test_catalog_mode_rejects_patch_envelopes_instead_of_fetching_or_reapplying(
    tmp_path, publication_sample
):
    from piddiplatsch.plugins.cmip6.processor import CMIP6Processor

    harvest(tmp_path, [publication_sample("cmip6")])
    with SQLiteCatalog("cmip6", tmp_path / "piddi.db", read_only=True) as catalog:
        processor = CMIP6Processor(catalog_reader=catalog)
        assert not hasattr(processor, "stac_client")
        with pytest.raises(ValueError, match="apply patches during harvest"):
            processor.process("patch", {"data": {"payload": {"method": "PATCH"}}})


def test_cli_history_option_overrides_catalog_configuration(
    tmp_path, publication_sample
):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set("catalog", "version_history", True)
    original = publication_sample("cmip6")
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


def test_manual_map_uses_project_error_limit(tmp_path, publication_sample):
    config._set("consumer", "output_dir", str(tmp_path))
    config._set(
        "plugins", "cmip6plus", {**config.get_plugin("cmip6plus"), "max_errors": 10000}
    )
    good = publication_sample("cmip6plus")
    bad = deepcopy(good)
    item = bad["data"]["payload"]["item"]
    item["id"] = item["id"].rsplit(".", 1)[0] + ".v20000101"
    next(iter(item["assets"].values()))["file:checksum"] = "bad"
    harvest(tmp_path / "catalog", [bad, good])
    result = map_catalog(projects=["cmip6plus"], input_date="all")
    assert result.total == 2
    assert result.failed == result.succeeded == 1


def test_mapping_failure_is_inspectable_and_not_automatically_remapped(
    tmp_path, publication_sample, monkeypatch
):
    config._set("consumer", "output_dir", str(tmp_path))
    original = publication_sample("cmip6")
    harvest(tmp_path / "catalog", [original])

    def fail_handle(*args, **kwargs):
        raise RuntimeError("invalid mapping")

    with monkeypatch.context() as patch:
        patch.setattr("piddiplatsch.handles.api.HandleAPI.add", fail_handle)
        assert map_catalog(projects=["cmip6"]).failed == 1
    failures = next((tmp_path / "batches/cmip6/ready").glob("*/failures.jsonl"))
    assert (
        "invalid mapping" in failures.read_text()
        or "Failed to register" in failures.read_text()
    )
    assert map_catalog(projects=["cmip6"]).total == 0
    with SQLiteCatalog(
        "cmip6", tmp_path / "catalog/piddi.db", read_only=True
    ) as reader:
        assert reader._execute("SELECT state FROM work_items").fetchone() == (
            "invalid",
        )


def test_recovered_remote_item_maps_dataset_and_files_without_network(
    tmp_path, publication_sample, monkeypatch
):
    config._set("consumer", "output_dir", str(tmp_path))
    current = publication_sample("cmip7")
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
