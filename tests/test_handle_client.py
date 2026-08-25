import pytest

from piddiplatsch.config import config
from piddiplatsch.handles.pyhandle_backend import HandleClient
from piddiplatsch.utils.models import prepare_handle_data


@pytest.fixture
def example_record():
    return {
        "URL": "https://example.org/handle",
        "AGGREGATION_LEVEL": "DATASET",
        "HAS_PARTS": [
            "a00ed634-4260-3bbd-b7a8-075266d8fd2d",
            "8f72d01f-4bc8-3272-b246-cebe15511d49",
        ],
        "HOSTING_NODE": {"host": "ceda.ac.uk", "published_on": None},
    }


def test_prepare_handle_data(example_record):
    prepared = prepare_handle_data(example_record)

    assert isinstance(prepared, dict)
    assert prepared["URL"] == example_record["URL"]
    assert prepared["AGGREGATION_LEVEL"] == "DATASET"


def test_handle_client_verifies_https_by_default(monkeypatch):
    captured = {}

    class FakeCredentials:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    class FakeClient:
        def instantiate_with_credentials(self, credentials):
            return self

    monkeypatch.setattr(
        "piddiplatsch.handles.pyhandle_backend.PIDClientCredentials",
        FakeCredentials,
    )
    monkeypatch.setattr(
        "piddiplatsch.handles.pyhandle_backend.pyhandle.handleclient.PyHandleClient",
        lambda _client: FakeClient(),
    )
    config._set("handle", "verify_https", True)

    HandleClient.from_config()

    assert captured["HTTPS_verify"] is True
