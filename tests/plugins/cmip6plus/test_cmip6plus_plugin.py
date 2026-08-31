from datetime import UTC, datetime

import pytest

from piddiplatsch.plugins.cmip6plus.processor import CMIP6PlusProcessor
from piddiplatsch.plugins.cmip6plus.record import (
    CMIP6PlusDatasetRecord,
    CMIP6PlusFileRecord,
)

pytestmark = [pytest.mark.plugin, pytest.mark.cmip6plus]

DATASET_PID = "bc85369b-44a5-3e57-8d91-251b63c8b9d3"
FILE_PID = "4485e7f1-06fb-46a5-99b3-2fb951eeb80d"
ITEM_ID = (
    "CMIP6Plus.TBIMIP.CSIRO-ARCCSS.ACCESS-CM2.tbi-pace-I-clim-mod.r1i1p1f1."
    "AE6hrPt.zg500.gn.v20251201"
)
ASSET_KEY = (
    "zg500_AE6hrPt_ACCESS-CM2_tbi-pace-I-clim-mod_r1i1p1f1_gn_"
    "100001010600-101001010000.nc"
)


@pytest.fixture
def cmip6plus_item():
    return {
        "type": "Feature",
        "id": ITEM_ID,
        "collection": "CMIP6Plus",
        "properties": {
            "project": "CMIP6Plus",
            "created": "2026-08-13T03:02:51Z",
            "retracted": False,
            "cmip6plus:pid": f"hdl:21.14100/{DATASET_PID}",
        },
        "assets": {
            ASSET_KEY: {
                "href": f"https://esgf.nci.org.au/thredds/fileServer/{ASSET_KEY}",
                "alternate:name": "esgf.nci.org.au",
                "file:size": 806802928,
                "file:checksum": (
                    "1220231110fbed237f91b6dd46c1111008970c8c124b72709b9cc7c52216073039db"
                ),
                "cmip6plus:tracking_id": f"hdl:21.14100/{FILE_PID}",
                "created": "2025-12-07T22:18:25Z",
                "roles": ["data"],
            }
        },
    }


def test_cmip6plus_source_pids_and_drs_mapping(cmip6plus_item):
    dataset = CMIP6PlusDatasetRecord(cmip6plus_item)
    file = CMIP6PlusFileRecord(cmip6plus_item, ASSET_KEY)

    assert dataset.pid == DATASET_PID
    assert dataset.dataset_version == "v20251201"
    assert dataset.has_parts == [f"hdl:21.TEST/{FILE_PID}"]
    assert file.pid == FILE_PID
    assert file.parent == f"hdl:21.TEST/{DATASET_PID}"


def test_cmip6plus_real_asset_shape_maps_to_shared_output(cmip6plus_item):
    dataset = CMIP6PlusDatasetRecord(cmip6plus_item).as_handle_model()
    file = CMIP6PlusFileRecord(cmip6plus_item, ASSET_KEY).as_handle_model()

    assert dataset.HOSTING_NODE.host == "esgf.nci.org.au"
    assert dataset.HOSTING_NODE.published_on == datetime(
        2025, 12, 7, 22, 18, 25, tzinfo=UTC
    )
    assert file.FILE_SIZE == 806802928
    assert file.CHECKSUM_METHOD == "sha2-256"


def test_cmip6plus_processor_handles_publication_envelope(cmip6plus_item):
    processor = CMIP6PlusProcessor(publish=False)
    message = {
        "metadata": {"time": "2026-08-13T03:02:53Z"},
        "data": {
            "payload": {
                "collection_id": "CMIP6Plus",
                "method": "POST",
                "item": cmip6plus_item,
            }
        },
    }

    result = processor.process("cmip6plus-event", message)

    assert result.success
    assert result.num_handles == 2
