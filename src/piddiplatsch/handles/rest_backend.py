from __future__ import annotations

import base64
import threading
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import quote

import requests
import urllib3

from piddiplatsch.config import config
from piddiplatsch.handles.base import HandleBackend


@dataclass(frozen=True)
class HandleWriteResult:
    """Result metadata for a successful Handle REST write."""

    action: Literal["created", "updated", "published"]
    url: str


class RestHandleClient(HandleBackend):
    """Small client for the Handle REST API's single-record operations."""

    api_path = "/api/handles/"
    admin_permissions = "011111110011"

    def __init__(
        self,
        server_url: str,
        prefix: str,
        username: str,
        password: str,
        verify_https: bool = True,
        timeout: float = 10.0,
        session: requests.Session | None = None,
    ):
        self.server_url = server_url.rstrip("/")
        self.prefix = prefix
        self.username = username
        self.password = password
        self.verify_https = verify_https
        self.timeout = timeout
        if not verify_https:
            # The operator explicitly opted out of certificate verification.
            # Avoid emitting one urllib3 warning per request, which otherwise
            # corrupts tqdm output during large parallel publications.
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self._injected_session = session
        self._thread_local = threading.local()

    @classmethod
    def from_config(cls) -> RestHandleClient:
        return cls(
            server_url=config.get("handle", "server_url"),
            prefix=config.get("handle", "prefix"),
            username=config.get("handle", "username"),
            password=config.get("handle", "password"),
            verify_https=config.get("handle", "verify_https", True),
            timeout=config.get("handle", "timeout", 10.0),
        )

    def _store(self, handle: str, handle_data: dict[str, Any]) -> HandleWriteResult:
        response = self._session().put(
            self._url(handle),
            params={"overwrite": "true"},
            json={"values": self._values(handle_data)},
            headers={
                "Accept": "application/json",
                "Authorization": f"Basic {self._authentication_token()}",
            },
            timeout=self.timeout,
            verify=self.verify_https,
        )
        response.raise_for_status()
        self._require_success(response)
        action: Literal["created", "updated", "published"]
        if response.status_code == 201:
            action = "created"
        elif response.status_code == 200:
            action = "updated"
        else:
            action = "published"
        return HandleWriteResult(action=action, url=self.record_url(handle))

    def _retrieve(self, handle: str) -> dict[str, Any] | None:
        response = self._session().get(
            self._url(handle),
            params={"auth": "true"},
            headers={"Accept": "application/json"},
            timeout=self.timeout,
            verify=self.verify_https,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()

        payload = response.json()
        if payload.get("responseCode") == 100:
            return None
        self._require_success(response, payload)

        result: dict[str, Any] = {}
        for entry in payload.get("values", []):
            if not isinstance(entry, dict):
                continue
            entry_type = entry.get("type")
            if not entry_type or entry_type == "HS_ADMIN":
                continue
            value = entry.get("data")
            if isinstance(value, dict) and "value" in value:
                value = value["value"]
            result[entry_type] = value
        return result or None

    def _session(self) -> requests.Session:
        if self._injected_session is not None:
            return self._injected_session
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            self._thread_local.session = session
        return session

    def _url(self, handle: str) -> str:
        return f"{self.server_url}{self.api_path}{quote(handle, safe='/')}"

    def record_url(self, handle: str) -> str:
        """Return the clean REST URL for manually inspecting a Handle."""
        return self._url(handle)

    def _authentication_token(self) -> str:
        # Handle credentials use an indexed handle as the username. Its colon
        # is escaped before encoding so it cannot be confused with the Basic
        # authentication username/password separator.
        credentials = f"{quote(self.username)}:{self.password}"
        return base64.b64encode(credentials.encode()).decode()

    def _values(self, handle_data: dict[str, Any]) -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = [
            {
                "index": 100,
                "type": "HS_ADMIN",
                "data": {
                    "format": "admin",
                    "value": {
                        "index": "200",
                        "handle": f"0.NA/{self.prefix}",
                        "permissions": self.admin_permissions,
                    },
                },
            }
        ]

        url = handle_data.get("URL")
        values.append({"index": 1, "type": "URL", "data": url})
        values.extend(
            {"index": index, "type": key, "data": value}
            for index, (key, value) in enumerate(
                ((key, value) for key, value in handle_data.items() if key != "URL"),
                start=2,
            )
        )
        return values

    @staticmethod
    def _require_success(response, payload: dict[str, Any] | None = None) -> None:
        payload = payload if payload is not None else response.json()
        response_code = payload.get("responseCode")
        if response_code not in (None, 1):
            message = payload.get("message", "Handle server rejected the request")
            raise requests.HTTPError(
                f"{message} (responseCode={response_code})",
                response=response,
            )
