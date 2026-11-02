import pytest

from piddiplatsch.config import config
from piddiplatsch.plugins.cmip6.model import CMIP6FileModel

pytestmark = [pytest.mark.plugin, pytest.mark.cmip6]

# --- Helper constants ---
VALID_FILE_DATA = {
    "URL": "https://example.com/data.nc",
    "FILE_NAME": "data.nc",
    "IS_PART_OF": "dataset-001",
    "CHECKSUM": "12205994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5",
    "CHECKSUM_METHOD": "sha2-256",
    "FILE_SIZE": 123,
    "DOWNLOAD_URL": "https://example.com/data.nc",
}


# --- CMIP6FileModel tests ---


def test_file_model_checksum_validation():
    config._set("schema", "strict_mode", True)

    file_model = CMIP6FileModel(**VALID_FILE_DATA)
    assert file_model.CHECKSUM_METHOD == "sha2-256"

    data = VALID_FILE_DATA.copy()
    data["CHECKSUM"] = "invalidchecksum"
    data["CHECKSUM_METHOD"] = "unknown"
    with pytest.raises(ValueError):
        CMIP6FileModel(**data)


def test_file_model_url_serialization():
    replicas = [
        "https://replica1.example.com/data.nc",
        "https://replica2.example.com/data.nc",
    ]
    file_model = CMIP6FileModel(**VALID_FILE_DATA, REPLICA_DOWNLOAD_URLS=replicas)
    data = file_model.model_dump()
    assert data["URL"] == VALID_FILE_DATA["URL"]
    assert data["DOWNLOAD_URL"] == VALID_FILE_DATA["DOWNLOAD_URL"]
    assert data["REPLICA_DOWNLOAD_URLS"] == replicas
