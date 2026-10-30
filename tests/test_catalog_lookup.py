"""Local version lookup uses indexed metadata and respects snapshot ownership."""

import json
from unittest.mock import MagicMock

import pytest

from piddiplatsch.catalog import CatalogEntry, CatalogError
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.config import config
from piddiplatsch.core.registry import get_plugin
from piddiplatsch.lookup.api import get_lookup
from piddiplatsch.lookup.catalog import CatalogLookup


@pytest.mark.parametrize("project", ["cmip6", "cmip6plus", "cmip7", "cordex-cmip6"])
def test_default_lookup_uses_index_and_project_collection(
    tmp_path, testdata_path, monkeypatch, project
):
    config._set("catalog", "directory", str(tmp_path))
    config._set("lookup", None, {"enabled": True})
    item = json.loads(
        (testdata_path / "publication_samples" / f"{project}.json").read_text()
    )["data"]["payload"]["item"]
    dataset_id, _ = get_plugin(project).catalog_index(item["id"])
    collection = item["collection"]
    config._set("plugins", project, {"stac": {"collection": collection}})
    ids = [f"{dataset_id}.v{version}" for version in (1, 2, 10)]
    with SQLiteCatalog(project, tmp_path / f"{project}.sqlite") as catalog:
        with catalog.transaction() as transaction:
            for version, item_id in zip((1, 2, 10), ids, strict=True):
                transaction.put_item(
                    CatalogEntry(
                        collection, {**item, "id": item_id}, dataset_id, version
                    )
                )
            transaction.put_item(
                CatalogEntry(
                    "other", {"id": ids[-1], "collection": "other"}, dataset_id, 99
                )
            )

    def unexpected_payload_or_network(*args, **kwargs):
        pytest.fail(
            "Indexed version lookup must not decompress payloads or contact services"
        )

    monkeypatch.setattr("zlib.decompress", unexpected_payload_or_network)
    monkeypatch.setattr("requests.get", unexpected_payload_or_network)
    monkeypatch.setattr(
        "piddiplatsch.lookup.stac.STACLookup.find_versions",
        unexpected_payload_or_network,
    )
    lookup = get_lookup(project)
    assert isinstance(lookup, CatalogLookup)
    assert lookup.find_versions(ids[1]) == list(reversed(ids))
    assert lookup.find_versions(ids[1], limit=2) == list(reversed(ids))[:2]
    assert lookup.find_previous_version(collection, dataset_id, 10) == ids[1]
    assert lookup.find_previous_version(collection, dataset_id, 1) is None
    assert lookup.find_versions(f"{dataset_id}.v100", collection_id="missing") == []
    with pytest.raises(ValueError, match="positive integer"):
        lookup.find_versions(ids[1], limit=0)


def test_injected_snapshot_stays_local_and_caller_owned(monkeypatch):
    reader = MagicMock()
    reader.find_versions.return_value = ["CMIP6.dataset.v2"]
    reader.find_previous_version.return_value = "CMIP6.dataset.v1"
    config._set("lookup", None, {"enabled": True, "backend": "stac"})
    lookup = get_lookup("cmip6", catalog_reader=reader)
    assert isinstance(lookup, CatalogLookup)
    assert lookup.find_versions("CMIP6.dataset.v2", collection_id="CMIP6") == [
        "CMIP6.dataset.v2"
    ]
    assert (
        lookup.find_previous_version("CMIP6", "CMIP6.dataset", 2) == "CMIP6.dataset.v1"
    )
    reader.find_versions.assert_called_once_with("CMIP6", "CMIP6.dataset", 100)
    reader.close.assert_not_called()


def test_missing_catalog_is_an_error_and_is_not_created(tmp_path):
    config._set("catalog", "directory", str(tmp_path))
    lookup = get_lookup("cmip6", enabled=True)
    with pytest.raises(CatalogError):
        lookup.find_versions("CMIP6.dataset.v1")
    assert not list(tmp_path.glob("*.sqlite"))
