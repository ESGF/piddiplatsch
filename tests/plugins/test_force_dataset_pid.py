"""Dataset PID correction must preserve relationships and file identities."""

import json
from copy import deepcopy
from unittest.mock import Mock

import pytest

from piddiplatsch.config import config
from piddiplatsch.core.handle_models import AliasHandleModel
from piddiplatsch.core.registry import get_plugin
from piddiplatsch.plugins.cmip6.record import CMIP6DatasetRecord
from piddiplatsch.plugins.cmip6plus.record import CMIP6PlusDatasetRecord
from piddiplatsch.plugins.cmip7.record import CMIP7DatasetRecord
from piddiplatsch.plugins.cordex_cmip6.record import CordexCMIP6DatasetRecord
from piddiplatsch.utils.models import asset_pid, item_pid

pytestmark = pytest.mark.plugin

RECORDS = [
    CMIP6DatasetRecord,
    CMIP6PlusDatasetRecord,
    CMIP7DatasetRecord,
    CordexCMIP6DatasetRecord,
]
ITEM_ID = "CMIP6.CMIP.MOHC.UKESM1-0-LL.historical.r1i1p1f2.Amon.tas.gn.v20190406"
FILE_PID = "7c4a583c-0bfe-4517-98fa-325084b02684"


def make_item(record_type):
    return {
        "id": ITEM_ID,
        "properties": {
            record_type.dataset_pid_fields[0]: item_pid(ITEM_ID.rsplit(".", 1)[0]),
        },
        "assets": {
            "data": {
                record_type.file_record.tracking_id_fields[
                    0
                ]: f"hdl:21.TEST/{FILE_PID}",
                "href": "https://example.org/tas.nc",
            },
        },
    }


@pytest.mark.parametrize("record_type", RECORDS)
@pytest.mark.parametrize("force", [None, False, True])
def test_dataset_pid_policy_and_relationships(record_type, force):
    if force is not None:
        config._set("plugins", record_type.plugin_name, {"force_dataset_pid": force})
    item = make_item(record_type)
    original = deepcopy(item)
    expected = item_pid(ITEM_ID if force else ITEM_ID.rsplit(".", 1)[0])

    dataset = record_type(item)
    file = record_type.file_record(item, "data")

    assert dataset.pid == expected
    assert dataset.url.endswith(f"/21.TEST/{expected}")
    assert file.parent == f"hdl:21.TEST/{expected}"
    assert file.pid == FILE_PID
    assert dataset.has_parts == [f"hdl:21.TEST/{FILE_PID}"]
    if force:
        alias = dataset.alias_record
        assert alias.pid == item_pid(ITEM_ID.rsplit(".", 1)[0])
        assert isinstance(alias.model, AliasHandleModel)
        assert alias.as_record() == {
            "URL": f"https://hdl.handle.net/21.TEST/{expected}"
        }
    else:
        assert dataset.alias_record is None
    assert item == original


@pytest.mark.parametrize("record_type", RECORDS)
@pytest.mark.parametrize("force", [False, True])
def test_missing_source_pids_still_generate(record_type, force):
    config._set("plugins", record_type.plugin_name, {"force_dataset_pid": force})
    item = make_item(record_type)
    item["properties"].clear()
    item["assets"]["data"].pop(record_type.file_record.tracking_id_fields[0])

    dataset = record_type(item)
    file = record_type.file_record(item, "data")

    assert dataset.pid == item_pid(ITEM_ID)
    assert file.parent == f"hdl:21.TEST/{dataset.pid}"
    assert file.pid == asset_pid(ITEM_ID, "data")
    assert dataset.has_parts == [f"hdl:21.TEST/{file.pid}"]
    assert dataset.alias_record is None


@pytest.mark.parametrize("record_type", RECORDS)
def test_forced_dataset_pids_distinguish_versions(record_type):
    config._set("plugins", record_type.plugin_name, {"force_dataset_pid": True})
    item = make_item(record_type)
    next_item = deepcopy(item)
    next_item["id"] = ITEM_ID.rsplit(".", 1)[0] + ".v20260917"

    assert record_type(item).pid != record_type(next_item).pid
    assert record_type(next_item).pid == item_pid(next_item["id"])


def test_force_bypasses_conflicting_dataset_fields_only():
    item = make_item(CMIP6DatasetRecord)
    item["properties"]["pid"] = "conflicting-dataset-pid"
    with pytest.raises(ValueError, match="Conflicting source PID"):
        _ = CMIP6DatasetRecord(item).pid

    config._set("plugins", "cmip6", {"force_dataset_pid": True})
    assert CMIP6DatasetRecord(item).pid == item_pid(ITEM_ID)
    assert CMIP6DatasetRecord.file_record(item, "data").parent.endswith(
        item_pid(ITEM_ID)
    )

    item["assets"]["data"]["tracking_id"] = "conflicting-file-pid"
    with pytest.raises(ValueError, match="Conflicting source PID"):
        _ = CMIP6DatasetRecord.file_record(item, "data").pid


def test_force_is_scoped_to_project():
    config._set("plugins", "cmip7", {"force_dataset_pid": True})
    cmip6_item = make_item(CMIP6DatasetRecord)
    assert CMIP6DatasetRecord(cmip6_item).pid == item_pid(ITEM_ID.rsplit(".", 1)[0])


@pytest.mark.parametrize("record_type", RECORDS)
def test_unchanged_dataset_pid_does_not_create_alias(record_type):
    config._set("plugins", record_type.plugin_name, {"force_dataset_pid": True})
    item = make_item(record_type)
    item["properties"][record_type.dataset_pid_fields[0]] = item_pid(ITEM_ID)
    assert record_type(item).alias_record is None


def test_conflicting_source_pids_cannot_create_an_ambiguous_alias():
    config._set("plugins", "cmip6", {"force_dataset_pid": True})
    item = make_item(CMIP6DatasetRecord)
    item["properties"]["pid"] = FILE_PID
    with pytest.raises(ValueError, match="Conflicting source PID"):
        _ = CMIP6DatasetRecord(item).alias_record


@pytest.mark.parametrize("record_type", RECORDS)
def test_project_processors_publish_dataset_aliases_only(record_type, testdata_path):
    project = record_type.plugin_name
    config._set(
        "plugins", project, {**config.get_plugin(project), "force_dataset_pid": True}
    )
    message = json.loads(
        (testdata_path / "publication_samples" / f"{project}.json").read_text()
    )
    item = message["data"]["payload"]["item"]
    source_pid = item_pid(item["id"].rsplit(".", 1)[0])
    for field in record_type.dataset_pid_fields:
        if field in item["properties"]:
            item["properties"][field] = f"hdl:21.TEST/{source_pid}"
    generated_pid = item_pid(item["id"])
    backend = Mock()
    processor = get_plugin(project).make_processor(handle_backend=backend)

    result = processor.process("dataset-alias", message)

    assert result.success
    assert result.num_handles == 3
    assert result.num_alias_handles == 1
    assert backend.add.call_count == 3
    backend.add.assert_any_call(
        source_pid, {"URL": f"https://hdl.handle.net/21.TEST/{generated_pid}"}
    )
    file_call = backend.add.call_args_list[-1]
    assert file_call.args[1]["AGGREGATION_LEVEL"] == "FILE"
    assert file_call.args[1]["IS_PART_OF"] == f"hdl:21.TEST/{generated_pid}"
