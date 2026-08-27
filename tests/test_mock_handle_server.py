import base64

import pytest

from piddiplatsch.testing.mock_handle_server import (
    DUMMY_PASSWORD,
    DUMMY_USERNAME,
    HANDLE_PREFIX,
    create_app,
)

TEST_SUFFIX = "pytest-test"
TEST_HANDLE = f"{HANDLE_PREFIX}/{TEST_SUFFIX}"
TEST_RECORD = {
    "values": [{"index": 1, "type": "URL", "data": {"value": "https://example.com"}}]
}


@pytest.fixture
def client():
    with create_app().test_client() as test_client:
        yield test_client


@pytest.fixture
def auth_headers():
    # Match the REST client's percent-encoded indexed username.
    credentials = f"{DUMMY_USERNAME.replace(':', '%3A')}:{DUMMY_PASSWORD}"
    token = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {token}"}


def test_get_nonexistent_handle(client):
    response = client.get(f"/api/handles/{HANDLE_PREFIX}/nonexistent")

    assert response.status_code == 200
    assert response.get_json()["responseCode"] == 100


def test_put_handle_without_overwrite(client, auth_headers):
    url = f"/api/handles/{TEST_HANDLE}"

    first_response = client.put(url, json=TEST_RECORD, headers=auth_headers)
    second_response = client.put(url, json=TEST_RECORD, headers=auth_headers)

    assert first_response.status_code == 201
    assert second_response.status_code == 409
    assert second_response.get_json()["responseCode"] == 101


def test_put_handle_with_overwrite(client, auth_headers):
    first_response = client.put(
        f"/api/handles/{TEST_HANDLE}?overwrite=true",
        json=TEST_RECORD,
        headers=auth_headers,
    )
    second_response = client.put(
        f"/api/handles/{TEST_HANDLE}?overwrite=true",
        json=TEST_RECORD,
        headers=auth_headers,
    )

    assert first_response.status_code == 201
    assert second_response.status_code == 200
    assert second_response.get_json()["responseCode"] == 1


def test_get_existing_handle(client, auth_headers):
    client.put(f"/api/handles/{TEST_HANDLE}", json=TEST_RECORD, headers=auth_headers)

    response = client.get(f"/api/handles/{TEST_HANDLE}")

    assert response.get_json() == {
        **TEST_RECORD,
        "handle": TEST_HANDLE,
        "responseCode": 1,
    }


def test_handle_suffix_may_contain_slashes(client, auth_headers):
    handle = f"{HANDLE_PREFIX}/collections/example"

    put_response = client.put(
        f"/api/handles/{handle}", json=TEST_RECORD, headers=auth_headers
    )
    get_response = client.get(f"/api/handles/{handle}")

    assert put_response.status_code == 201
    assert get_response.get_json()["handle"] == handle


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer token"},
        {"Authorization": "Basic invalid-base64"},
    ],
)
def test_put_requires_valid_basic_authentication(client, headers):
    response = client.put(
        f"/api/handles/{TEST_HANDLE}", json=TEST_RECORD, headers=headers
    )

    assert response.status_code == 401
    assert response.get_json()["responseCode"] == 402


@pytest.mark.parametrize(
    ("body", "content_type"),
    [
        ("not json", "application/json"),
        ('{"other": []}', "application/json"),
        ('{"values": {}}', "application/json"),
        ('{"values": []}', "text/plain"),
    ],
)
def test_put_rejects_invalid_request_body(client, auth_headers, body, content_type):
    response = client.put(
        f"/api/handles/{TEST_HANDLE}",
        data=body,
        content_type=content_type,
        headers=auth_headers,
    )

    assert response.status_code == 400


def test_app_instances_have_isolated_state(auth_headers):
    first_app = create_app()
    second_app = create_app()
    with first_app.test_client() as first_client:
        first_client.put(
            f"/api/handles/{TEST_HANDLE}", json=TEST_RECORD, headers=auth_headers
        )
    with second_app.test_client() as second_client:
        response = second_client.get(f"/api/handles/{TEST_HANDLE}")

    assert response.get_json()["responseCode"] == 100


def test_configured_delay_applies_only_to_valid_puts(auth_headers):
    delays = []
    app = create_app(put_delay_seconds=0.05, sleep=delays.append)
    with app.test_client() as delayed_client:
        delayed_client.get(f"/api/handles/{TEST_HANDLE}")
        delayed_client.put(
            f"/api/handles/{TEST_HANDLE}",
            data="not-json",
            content_type="application/json",
            headers=auth_headers,
        )
        delayed_client.put(
            f"/api/handles/{TEST_HANDLE}", json=TEST_RECORD, headers=auth_headers
        )

    assert delays == [0.05]


def test_delay_can_be_configured_from_environment(monkeypatch, auth_headers):
    delays = []
    monkeypatch.setenv("PIDDI_MOCK_HANDLE_PUT_DELAY_SECONDS", "0.025")
    app = create_app(sleep=delays.append)
    with app.test_client() as delayed_client:
        delayed_client.put(
            f"/api/handles/{TEST_HANDLE}", json=TEST_RECORD, headers=auth_headers
        )

    assert delays == [0.025]


@pytest.mark.parametrize("delay", [-0.1, "invalid"])
def test_rejects_invalid_delay_configuration(monkeypatch, delay):
    monkeypatch.setenv("PIDDI_MOCK_HANDLE_PUT_DELAY_SECONDS", str(delay))

    with pytest.raises(ValueError, match="PIDDI_MOCK_HANDLE_PUT_DELAY_SECONDS"):
        create_app()
