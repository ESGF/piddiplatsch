"""Opt-in, named compatibility rules around standard JSON Patch application.

Rules prepare only known missing structures immediately before an operation.
They never swallow failures or modify the archived publication.
"""

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

import jsonpatch
from jsonpointer import JsonPointer


@dataclass(frozen=True)
class PatchRepair:
    rule: str
    path: str


def _missing_asset_alternate(item: Any, operation: dict) -> str | None:
    if operation.get("op") != "add" or not isinstance(item, dict):
        return None
    parts = JsonPointer(operation["path"]).parts
    if (
        len(parts) != 4
        or parts[0] != "assets"
        or parts[2] != "alternate"
        or not parts[3]
    ):
        return None
    assets = item.get("assets")
    asset = assets.get(parts[1]) if isinstance(assets, dict) else None
    if not isinstance(asset, dict) or "alternate" in asset:
        return None
    asset["alternate"] = {}
    return JsonPointer.from_parts(parts[:3]).path


# Extend this registry with separately tested, narrowly scoped rules. List order
# in configuration determines rule order for each operation.
PATCH_RULES: dict[str, Callable[[Any, dict], str | None]] = {
    "missing_asset_alternate": _missing_asset_alternate,
}


def validate_patch_rules(names: list[str]) -> list[str]:
    if not isinstance(names, list) or any(not isinstance(name, str) for name in names):
        raise ValueError(
            "catalog.patch_compatibility_rules must be a list of rule names"
        )
    unknown = set(names) - PATCH_RULES.keys()
    if unknown:
        raise ValueError(
            f"Unknown patch compatibility rules: {', '.join(sorted(unknown))}"
        )
    if len(set(names)) != len(names):
        raise ValueError("Patch compatibility rules must not be repeated")
    return list(names)


def apply_patch(
    item: dict, operations: list[dict], rules: list[str]
) -> tuple[dict, tuple[PatchRepair, ...]]:
    """Apply operations on detached state; return repairs only if all succeed."""
    names = validate_patch_rules(rules)
    # Later operations can modify a value inserted by an earlier operation.
    # Copy operations too, so those changes never alter the original event.
    patch = jsonpatch.JsonPatch(deepcopy(operations))
    if not names:
        return patch.apply(item), ()
    working = deepcopy(item)
    repairs = []
    for operation in patch:
        for name in names:
            path = PATCH_RULES[name](working, operation)
            if path is not None:
                repairs.append(PatchRepair(name, path))
        working = jsonpatch.JsonPatch([operation]).apply(working, in_place=True)
    return working, tuple(repairs)
