import json
import logging
from datetime import datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid3

from dateutil.parser import isoparse
from multiformats import multihash

from piddiplatsch.config import config


def build_handle(
    pid: str,
    as_uri: bool = False,
    *,
    prefix: str | None = None,
    project: str | None = None,
) -> str:
    scheme = "hdl"
    if prefix is None:
        prefix = config.get_handle(project=project).get("prefix", "")
    if as_uri:
        handle = f"{scheme}:{prefix}/{pid}"
    else:
        handle = f"{prefix}/{pid}"
    return handle


def prepare_handle_data(record: dict[str, Any]) -> dict[str, str]:
    """Prepare handle record fields: serialize list/dict values, skip None, convert datetime."""
    prepared: dict[str, str] = {}

    for key, value in record.items():
        if value is None:
            continue

        if isinstance(value, list | dict):

            def serialize(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return obj

            value = json.dumps(value, default=serialize)
        elif isinstance(value, datetime):
            value = value.isoformat()

        prepared[key] = value

    return prepared


def item_pid(item_id: str) -> str:
    """Return a deterministic UUIDv3 PID for the given STAC item ID."""
    return str(uuid3(NAMESPACE_URL, item_id))


def asset_pid(item_id: str, asset_key: str) -> str:
    """Return a deterministic UUIDv3 PID for a given asset of a STAC item."""
    return str(uuid3(NAMESPACE_URL, f"{item_id}#{asset_key}"))


def drop_empty(d):
    """Recursively clean empty fields"""
    result = {}
    for k, v in d.items():
        if v in ("", [], {}, None):
            continue
        if isinstance(v, dict):
            nested = drop_empty(v)
            if nested:
                result[k] = nested
        else:
            result[k] = v
    return result


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return isoparse(value)
    except Exception:
        logging.warning(f"Failed to parse datetime: {value}")
        return None


def parse_pid(value: str) -> str:
    if value and isinstance(value, str) and "/" in value:
        pid_ = value.split("/")[1]
    else:
        pid_ = value
    return pid_


def parse_multihash_checksum(checksum: str) -> tuple[str, str]:
    """
    Parse a multihash checksum string into (checksum_method, checksum_hex).

    Args:
        checksum: multihash as hex string, e.g.
            '12205994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5'

    Returns:
        Tuple of:
        - checksum_method: human-readable hash name, e.g., 'sha2-256'
        - checksum_hex: raw digest as hex string
    """
    # Convert hex string to bytes
    mh_bytes = bytes.fromhex(checksum)

    # Decode the multihash
    code, digest_bytes = multihash.unwrap_raw(mh_bytes)

    # Get human-readable hash name
    checksum_type = multihash.get(code).name

    # Convert digest bytes to hex
    checksum_hex = digest_bytes.hex()

    return checksum_type, checksum_hex


def is_hex(s: str) -> bool:
    try:
        bytes.fromhex(s)
        return True
    except ValueError:
        return False
