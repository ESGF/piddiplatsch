from datetime import UTC, datetime

import pytest

from piddiplatsch.config import config
from piddiplatsch.core.handle_models import DatasetHandleModel, FileHandleModel
from piddiplatsch.plugins.cmip7.model import CMIP7DatasetModel, CMIP7FileModel
from piddiplatsch.plugins.cmip7.processor import CMIP7Processor
from piddiplatsch.plugins.cmip7.record import CMIP7DatasetRecord, CMIP7FileRecord

pytestmark = [pytest.mark.plugin, pytest.mark.cmip7]

DATASET_PID = "1f062cde-b12d-335d-a30b-988188098842"
FILE_PID = "7c4a583c-0bfe-4517-98fa-325084b02684"
ITEM_ID = (
    "MIP-DRS7.CMIP7.CMIP.CCCma.CanESM5-1.piControl.r1i1p2f1.glb.mon."
    "tas.tavg-h2m-hxy-u.g120.v20190429"
)
ASSET_KEY = "tas_tavg-h2m-hxy-u_mon_glb_g120_CanESM5-1_piControl.nc"


@pytest.fixture
def cmip7_item():
    return {
        "type": "Feature",
        "id": ITEM_ID,
        "collection": "CMIP7",
        "properties": {
            "project": "CMIP7",
            "created": "2026-08-06T17:22:58Z",
            "retracted": False,
            "cmip7:pid": f"hdl:21.14107/{DATASET_PID}",
        },
        "assets": {
            ASSET_KEY: {
                "href": f"https://crd-esgf-drc.ec.gc.ca/thredds/fileServer/{ASSET_KEY}",
                "alternate:name": "crd-esgf-drc.ec.gc.ca",
                "file:size": 22753608,
                "file:checksum": (
                    "122088ebf351f596eb286d31519ed333e6b33606ca386735525d5fb4a247f6bc715f"
                ),
                "cmip7:tracking_id": f"hdl:21.14107/{FILE_PID}",
                "created": "2026-08-05T19:22:58Z",
                "roles": ["data"],
            }
        },
    }


def test_cmip7_models_share_the_handle_schema():
    assert issubclass(CMIP7DatasetModel, DatasetHandleModel)
    assert issubclass(CMIP7FileModel, FileHandleModel)


def test_cmip7_source_pids_and_relationships_are_preserved(cmip7_item):
    dataset = CMIP7DatasetRecord(cmip7_item)
    file = CMIP7FileRecord(cmip7_item, ASSET_KEY)

    assert dataset.pid == DATASET_PID
    assert dataset.has_parts == [f"hdl:21.TEST/{FILE_PID}"]
    assert file.pid == FILE_PID
    assert file.parent == f"hdl:21.TEST/{DATASET_PID}"


def test_cmip7_real_asset_shape_maps_to_shared_output(cmip7_item):
    dataset = CMIP7DatasetRecord(cmip7_item).as_handle_model()
    file = CMIP7FileRecord(cmip7_item, ASSET_KEY).as_handle_model()

    assert dataset.DATASET_VERSION == "v20190429"
    assert dataset.HOSTING_NODE.host == "crd-esgf-drc.ec.gc.ca"
    assert dataset.HOSTING_NODE.published_on == datetime(
        2026, 8, 5, 19, 22, 58, tzinfo=UTC
    )
    assert file.FILE_NAME == ASSET_KEY
    assert file.FILE_SIZE == 22753608
    assert file.CHECKSUM_METHOD == "sha2-256"


def test_cmip7_processor_handles_publication_envelope(cmip7_item):
    config._set("lookup", "enabled", False)
    processor = CMIP7Processor(publish=False)
    message = {
        "metadata": {"time": "2026-08-06T17:22:58Z"},
        "data": {
            "payload": {
                "collection_id": "CMIP7",
                "method": "POST",
                "item": cmip7_item,
            }
        },
    }

    result = processor.process("cmip7-event", message)

    assert result.success
    assert result.num_handles == 2


def test_cmip7_rejects_only_malformed_consumed_input_shape():
    processor = CMIP7Processor(publish=False)
    with pytest.raises(ValueError, match="MISSING payload object"):
        processor.process("bad-event", {"data": {"payload": []}})


def test_cmip7_accepts_real_publisher_patch_shape(cmip7_item):
    processor = CMIP7Processor(publish=False)

    class StacClient:
        def get_item(self, collection_id, item_id):
            assert (collection_id, item_id) == ("CMIP7", ITEM_ID)
            return cmip7_item

    processor.stac_client = StacClient()
    patched = processor._apply_patch_to_stac_item(
        {
            "collection_id": "CMIP7",
            "item_id": ITEM_ID,
            "patch": [
                {"op": "replace", "path": "/properties/retracted", "value": True},
                {"op": "replace", "path": "/assets", "value": {}},
            ],
        }
    )

    assert patched["properties"]["retracted"] is True
    assert patched["assets"] == {}
    assert CMIP7DatasetRecord(patched).as_handle_model().HAS_PARTS == []
