"""Opt-in contract tests against a disposable real Handle service prefix."""

from __future__ import annotations

import json
import os
from uuid import uuid4

import pytest

from piddiplatsch.config import config
from piddiplatsch.handles.publish import HandlePublisher
from piddiplatsch.handles.rest_backend import RestHandleClient

pytestmark = pytest.mark.live


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"set {name} to run real Handle service tests")
    return value


@pytest.fixture
def live_client() -> RestHandleClient:
    prefix = _required_env("PIDDI_LIVE_HANDLE_PREFIX")
    config._set("handle", "prefix", prefix)
    return RestHandleClient(
        server_url=_required_env("PIDDI_LIVE_HANDLE_SERVER_URL"),
        prefix=prefix,
        username=_required_env("PIDDI_LIVE_HANDLE_USERNAME"),
        password=_required_env("PIDDI_LIVE_HANDLE_PASSWORD"),
        verify_https=os.environ.get("PIDDI_LIVE_HANDLE_VERIFY_HTTPS", "true").lower()
        not in {"0", "false", "no"},
        timeout=15,
    )


def test_real_service_create_update_and_read(live_client):
    pid = f"piddiplatsch-contract-{uuid4()}"

    live_client.add(pid, {"URL": f"https://example.test/{pid}/v1", "VERSION": "1"})
    live_client.add(pid, {"URL": f"https://example.test/{pid}/v2", "VERSION": "2"})

    record = live_client.get(pid)
    assert record is not None
    assert record["URL"] == f"https://example.test/{pid}/v2"
    assert record["VERSION"] == "2"


def test_real_service_parallel_create_and_ordered_update(live_client, tmp_path):
    prefix = live_client.prefix
    updated_pid = f"piddiplatsch-parallel-{uuid4()}"
    other_pids = [f"piddiplatsch-parallel-{uuid4()}" for _ in range(4)]
    records = [
        {
            "handle": f"{prefix}/{updated_pid}",
            "URL": f"https://example.test/{updated_pid}/v1",
            "data": {"VERSION": "1"},
        },
        *[
            {
                "handle": f"{prefix}/{pid}",
                "URL": f"https://example.test/{pid}",
                "data": {"VERSION": "1"},
            }
            for pid in other_pids
        ],
        {
            "handle": f"{prefix}/{updated_pid}",
            "URL": f"https://example.test/{updated_pid}/v2",
            "data": {"VERSION": "2"},
        },
    ]
    source = tmp_path / "handles.jsonl"
    source.write_text(
        "".join(f"{json.dumps(record)}\n" for record in records),
        encoding="utf-8",
    )

    result = HandlePublisher(live_client).run([source], workers=4, retries=2)

    assert result.succeeded == len(records), result.errors
    assert live_client.get(updated_pid)["VERSION"] == "2"
    assert all(live_client.get(pid) is not None for pid in other_pids)
