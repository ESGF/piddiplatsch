from datetime import UTC, datetime

import pytest

from piddiplatsch.plugins.cordex_cmip6.processor import CordexCMIP6Processor
from piddiplatsch.plugins.cordex_cmip6.record import (
    CordexCMIP6DatasetRecord,
    CordexCMIP6FileRecord,
)

pytestmark = [pytest.mark.plugin, pytest.mark.cordex_cmip6]

DATASET_PID = "b3eaa573-aee5-3f33-b36f-8970df2eba9a"
FILE_PID = "415fb9b8-f11a-47ae-ab62-a5c5e17c77bf"
ITEM_ID = (
    "CORDEX-CMIP6.DD.EUR-12.CLMcom-BTU.CNRM-ESM2-1.historical.r1i1p1f2."
    "ICON-CLM-202407-1-1.v1-r1.day.rsdsdir.v20260415"
)
ASSET_KEY = (
    "rsdsdir_EUR-12_CNRM-ESM2-1_historical_r1i1p1f2_CLMcom-BTU_"
    "ICON-CLM-202407-1-1_v1-r1_day_19500101-19501231.nc"
)


@pytest.fixture
def cordex_item():
    return {
        "type": "Feature",
        "id": ITEM_ID,
        "collection": "CORDEX-CMIP6",
        "properties": {
            "project": "CORDEX-CMIP6",
            "created": "2026-07-21T10:48:13Z",
            "retracted": False,
            "cordex-cmip6:pid": f"hdl:21.14103/{DATASET_PID}",
        },
        "assets": {
            "globus": {
                "href": "https://app.globus.org/file-manager",
                "alternate:name": "esgf1.dkrz.de",
                "cordex-cmip6:tracking_id": f"hdl:21.14103/{DATASET_PID}",
                "created": "2026-05-04T09:14:43Z",
            },
            ASSET_KEY: {
                "href": f"https://esgf1.dkrz.de/thredds/fileServer/{ASSET_KEY}",
                "alternate:name": "esgf1.dkrz.de",
                "file:size": 200818284,
                "file:checksum": (
                    "1220274092b5cd54fc4ee292dce45ccd5d43e4d83f90e75d100e62971e618d87ed1f"
                ),
                "cordex-cmip6:tracking_id": f"hdl:21.14103/{FILE_PID}",
                "created": "2026-05-04T09:14:43Z",
                "roles": ["data"],
            },
        },
    }


def test_cordex_source_pids_and_drs_mapping(cordex_item):
    dataset = CordexCMIP6DatasetRecord(cordex_item, exclude_keys=["globus"])
    file = CordexCMIP6FileRecord(cordex_item, ASSET_KEY)

    assert dataset.pid == DATASET_PID
    assert dataset.dataset_version == "v20260415"
    assert dataset.has_parts == [f"hdl:21.TEST/{FILE_PID}"]
    assert file.pid == FILE_PID
    assert file.parent == f"hdl:21.TEST/{DATASET_PID}"


def test_cordex_real_asset_shape_maps_to_shared_output(cordex_item):
    dataset = CordexCMIP6DatasetRecord(
        cordex_item, exclude_keys=["globus"]
    ).as_handle_model()
    file = CordexCMIP6FileRecord(cordex_item, ASSET_KEY).as_handle_model()

    assert dataset.HOSTING_NODE.host == "esgf1.dkrz.de"
    assert dataset.HOSTING_NODE.published_on == datetime(
        2026, 5, 4, 9, 14, 43, tzinfo=UTC
    )
    assert file.FILE_SIZE == 200818284
    assert file.CHECKSUM_METHOD == "sha2-256"


def test_cordex_processor_excludes_non_file_assets(cordex_item):
    processor = CordexCMIP6Processor(publish=False)
    message = {
        "metadata": {"time": "2026-07-21T10:48:14Z"},
        "data": {
            "payload": {
                "collection_id": "CORDEX-CMIP6",
                "method": "POST",
                "item": cordex_item,
            }
        },
    }

    result = processor.process("cordex-event", message)

    assert result.success
    assert result.num_handles == 2
