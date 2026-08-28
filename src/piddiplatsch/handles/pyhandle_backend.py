import logging
from typing import Any

import pyhandle
from pyhandle.clientcredentials import PIDClientCredentials
from pyhandle.handleexceptions import HandleAlreadyExistsException

from piddiplatsch.config import config
from piddiplatsch.handles.base import HandleBackend


class HandleClient(HandleBackend):
    """PyHandle client implementing HandleBackend interface."""

    def __init__(
        self,
        server_url: str,
        prefix: str,
        username: str,
        password: str,
        verify_https: bool = True,
    ):
        self.server_url = server_url
        self.prefix = prefix
        self.username = username
        self.password = password
        self.verify_https = verify_https
        handle_cfg = {
            "client": "rest",
            "handle_server_url": server_url,
            "username": username,
            "password": password,
            "HTTPS_verify": verify_https,
            "prefix": prefix,
        }
        cred = PIDClientCredentials(**handle_cfg)
        self.client = pyhandle.handleclient.PyHandleClient(
            "rest"
        ).instantiate_with_credentials(cred)

    @classmethod
    def from_config(
        cls,
        *,
        project: str | None = None,
        profile: str | None = None,
    ) -> "HandleClient":
        handle_config = config.get_handle(project=project, profile=profile)
        return cls(
            server_url=handle_config.get("server_url"),
            prefix=handle_config.get("prefix"),
            username=handle_config.get("username"),
            password=handle_config.get("password"),
            verify_https=handle_config.get("verify_https", True),
        )

    def _store(self, handle: str, handle_data: dict[str, Any]) -> None:
        location = handle_data.pop("URL", None)
        try:
            self.client.register_handle(
                handle=handle,
                location=location,
                overwrite=True,
                **handle_data,
            )
        except HandleAlreadyExistsException:
            logging.debug("Handle already exists (ignored with overwrite=True)")
        except Exception as e:
            logging.error(f"Failed to register handle {handle}: {e}")
            raise

    def _retrieve(self, handle: str) -> dict[str, Any] | None:
        """Retrieve the handle record via PyHandle.

        First tries `retrieve_handle_record()` (simple key/value dict). If that fails,
        falls back to `retrieve_handle_record_json()` and converts entries to a dict.
        """
        # Primary: simple dict of key/value pairs
        try:
            record = self.client.retrieve_handle_record(handle, auth=True)
            if isinstance(record, dict):
                return record
            if not record:
                return None
        except Exception as e:
            logging.debug(f"retrieve_handle_record failed for {handle}: {e}")

        # Fallback: full JSON representation with entries list
        try:
            data = self.client.retrieve_handle_record_json(handle, auth=True)
            if not data:
                return None

            entries = None
            if isinstance(data, dict):
                # Common key for entries is 'values'
                entries = data.get("values") or data.get("entries")
            elif isinstance(data, list):
                entries = data

            if not entries or not isinstance(entries, list):
                return None

            result: dict[str, Any] = {}
            url_value = None

            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                etype = entry.get("type") or entry.get("HS_TYPE")
                edata = entry.get("data")
                # 'data' may be a dict with 'value' or a raw string
                if isinstance(edata, dict):
                    value = (
                        edata.get("value") if "value" in edata else edata.get("data")
                    )
                else:
                    value = edata

                if etype == "URL":
                    url_value = value
                elif etype:
                    result[etype] = value

            if url_value is not None:
                result["URL"] = url_value

            return result or None
        except Exception as e:
            logging.error(f"Failed to retrieve handle {handle}: {e}")
            return None
