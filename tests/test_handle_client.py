import pytest

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
