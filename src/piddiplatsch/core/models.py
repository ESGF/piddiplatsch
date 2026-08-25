from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from piddiplatsch.config import config

ALLOWED_CHECKSUM_METHODs = {
    "md5",
    "sha1",
    "sha2-256",
    "sha2-512",
    "sha3-256",
    "sha3-512",
    "blake2b-256",
}


def strict_mode() -> bool:
    return config.get("schema", {}).get("strict_mode", False)


class HostingNode(BaseModel):
    host: str
    published_on: datetime | None = None
