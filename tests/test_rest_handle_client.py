import base64

import pytest
import requests

from piddiplatsch.config import config
from piddiplatsch.handles.api import get_handle_backend
from piddiplatsch.handles.pyhandle_backend import HandleClient
from piddiplatsch.handles.rest_backend import RestHandleClient


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def put(self, url, **kwargs):
        self.calls.append(("PUT", url, kwargs))
        return self.response

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self.response


def make_client(response):
    session = FakeSession(response)
    test_password = "sec" + "ret"
    client = RestHandleClient(
        server_url="https://handles.example.test/",
        prefix="21.TEST",
        username="300:21.TEST/testuser",
        password=test_password,
        verify_https=False,
        timeout=4.5,
        session=session,
    )
    return client, session


def test_rest_client_puts_single_handle_with_overwrite():
    client, session = make_client(FakeResponse({"responseCode": 1}))

    client.add(
        "abc",
        {
            "URL": "https://example.test/abc",
            "AGGREGATION_LEVEL": "DATASET",
        },
    )

    method, url, request = session.calls[0]
    assert method == "PUT"
    assert url == "https://handles.example.test/api/handles/21.TEST/abc"
    assert request["params"] == {"overwrite": "true"}
    assert request["timeout"] == 4.5
    assert request["verify"] is False
    assert request["json"]["values"] == [
        {
            "index": 100,
            "type": "HS_ADMIN",
            "data": {
                "format": "admin",
                "value": {
                    "index": "200",
                    "handle": "0.NA/21.TEST",
                    "permissions": "011111110011",
                },
            },
        },
        {"index": 1, "type": "URL", "data": "https://example.test/abc"},
        {"index": 2, "type": "AGGREGATION_LEVEL", "data": "DATASET"},
    ]
    token = base64.b64encode(b"300%3A21.TEST/testuser:secret").decode()
    assert request["headers"]["Authorization"] == f"Basic {token}"


def test_rest_client_decodes_handle_values():
    client, session = make_client(
        FakeResponse(
            {
                "responseCode": 1,
                "values": [
                    {"index": 100, "type": "HS_ADMIN", "data": {}},
                    {
                        "index": 1,
                        "type": "URL",
                        "data": {
                            "format": "string",
                            "value": "https://example.test/abc",
                        },
                    },
                    {"index": 2, "type": "PROFILE", "data": "21.T11148/abc"},
                ],
            }
        )
    )

    assert client.get("abc") == {
        "URL": "https://example.test/abc",
        "PROFILE": "21.T11148/abc",
    }
    method, _, request = session.calls[0]
    assert method == "GET"
    assert request["params"] == {"auth": "true"}


@pytest.mark.parametrize(
    "response",
    [FakeResponse({"responseCode": 100}), FakeResponse({}, status_code=404)],
)
def test_rest_client_returns_none_for_missing_handle(response):
    client, _ = make_client(response)

    assert client.get("missing") is None


def test_rest_client_rejects_unsuccessful_handle_response():
    client, _ = make_client(FakeResponse({"responseCode": 402, "message": "No auth"}))

    with pytest.raises(requests.HTTPError, match=r"No auth.*responseCode=402"):
        client.add("abc", {"URL": "https://example.test/abc"})


def test_handle_backend_factory_selects_rest(monkeypatch):
    sentinel = object()
    config._set("handle", "backend", "rest")
    monkeypatch.setattr("piddiplatsch.handles.api.RestHandleClient.from_config", lambda: sentinel)

    assert get_handle_backend() is sentinel


def test_handle_backend_factory_keeps_pyhandle(monkeypatch):
    sentinel = object()
    config._set("handle", "backend", "pyhandle")
    monkeypatch.setattr("piddiplatsch.handles.api.HandleClient.from_config", lambda: sentinel)

    assert get_handle_backend() is sentinel
    assert HandleClient is not RestHandleClient
