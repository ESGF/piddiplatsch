"""Small, in-memory stand-in for the Handle REST API used by tests."""

from __future__ import annotations

import base64
import binascii
import logging
import os
import time
from collections.abc import Callable
from copy import deepcopy
from hmac import compare_digest
from threading import RLock
from typing import Any
from urllib.parse import unquote

from flask import Flask, jsonify, request

logger = logging.getLogger(__name__)

HANDLE_PREFIX = "21.TEST"
DUMMY_USERNAME_HANDLE = f"{HANDLE_PREFIX}/testuser"
DUMMY_USERNAME = f"300:{DUMMY_USERNAME_HANDLE}"
DUMMY_PASSWORD = "testpass"
PUT_DELAY_ENV = "PIDDI_MOCK_HANDLE_PUT_DELAY_SECONDS"


def _admin_handle_record() -> dict[str, Any]:
    """Return a fresh admin record so resets cannot reuse mutated state."""
    return {
        "handle": DUMMY_USERNAME_HANDLE,
        "values": [
            {
                "index": 100,
                "type": "HS_ADMIN",
                "data": {
                    "format": "admin",
                    "value": {
                        "index": "200",
                        "handle": f"0.NA/{HANDLE_PREFIX}",
                        "permissions": "011111110011",
                    },
                },
            }
        ],
    }


class HandleStore:
    """Thread-safe store that does not expose its mutable records."""

    def __init__(self) -> None:
        self._lock = RLock()
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self._handles = {DUMMY_USERNAME_HANDLE: _admin_handle_record()}

    def get(self, handle: str) -> dict[str, Any] | None:
        with self._lock:
            record = self._handles.get(handle)
            return deepcopy(record) if record is not None else None

    def put(self, handle: str, record: dict[str, Any], *, overwrite: bool) -> bool:
        with self._lock:
            if handle in self._handles and not overwrite:
                return False
            self._handles[handle] = deepcopy(record)
            return True


def _is_authorized(authorization: str | None) -> bool:
    if not authorization:
        return False

    scheme, separator, token = authorization.partition(" ")
    if separator != " " or scheme.lower() != "basic":
        return False

    try:
        credentials = base64.b64decode(token, validate=True).decode()
    except (binascii.Error, UnicodeDecodeError):
        return False

    username, separator, password = credentials.rpartition(":")
    return (
        bool(separator)
        and compare_digest(unquote(username), DUMMY_USERNAME)
        and compare_digest(password, DUMMY_PASSWORD)
    )


def _configured_put_delay() -> float:
    raw_delay = os.environ.get(PUT_DELAY_ENV, "0")
    try:
        delay = float(raw_delay)
    except ValueError as exc:
        raise ValueError(
            f"{PUT_DELAY_ENV} must be a number, got {raw_delay!r}"
        ) from exc
    if delay < 0:
        raise ValueError(f"{PUT_DELAY_ENV} cannot be negative")
    return delay


def create_app(
    *,
    put_delay_seconds: float | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Flask:
    """Create a mock server with its own isolated in-memory store."""
    put_delay = (
        _configured_put_delay()
        if put_delay_seconds is None
        else float(put_delay_seconds)
    )
    if put_delay < 0:
        raise ValueError("put_delay_seconds cannot be negative")

    mock_app = Flask(__name__)
    store = HandleStore()
    mock_app.extensions["handle_store"] = store
    mock_app.extensions["handle_put_delay_seconds"] = put_delay

    @mock_app.route("/api/handles/<path:handle>", methods=["GET", "PUT"])
    def handle_record(handle: str) -> Any:
        if "/" not in handle:
            return jsonify(message="A handle must contain a prefix and suffix"), 400

        if request.method == "GET":
            logger.debug("Getting handle %s", handle)
            record = store.get(handle)
            if record is None:
                return jsonify(message=f"Handle {handle} not found", responseCode=100)
            return jsonify({**record, "handle": handle, "responseCode": 1})

        if not _is_authorized(request.headers.get("Authorization")):
            return (
                jsonify(message="Authentication failed", responseCode=402),
                401,
                {"WWW-Authenticate": 'Basic realm="Handle API"'},
            )

        data = request.get_json(silent=True)
        if not isinstance(data, dict) or not isinstance(data.get("values"), list):
            return jsonify(message="Request body must contain a values list"), 400

        record = {**data, "handle": handle}
        overwrite = request.args.get("overwrite", "false").lower() == "true"
        if put_delay:
            sleep(put_delay)
        if not store.put(handle, record, overwrite=overwrite):
            return (
                jsonify(message=f"Handle {handle} already exists", responseCode=101),
                409,
            )

        logger.debug("Stored handle %s", handle)
        return jsonify(
            responseCode=1,
            handle=handle,
            message=f"Handle {handle} registered",
        )

    return mock_app


app = create_app()


def _reset_handles(flask_app: Flask = app) -> None:
    """Reset an app's store; retained for compatibility with existing tests."""
    flask_app.extensions["handle_store"].reset()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
