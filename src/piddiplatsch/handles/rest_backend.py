from __future__ import annotations

import base64
from typing import Any
from urllib.parse import quote

import requests

from piddiplatsch.config import config
from piddiplatsch.handles.base import HandleBackend


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
        self.session = session or requests.Session()

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

    def _store(self, handle: str, handle_data: dict[str, Any]) -> None:
        response = self.session.put(
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

    def _retrieve(self, handle: str) -> dict[str, Any] | None:
        response = self.session.get(
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

    def _url(self, handle: str) -> str:
        return f"{self.server_url}{self.api_path}{quote(handle, safe='/')}"

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
