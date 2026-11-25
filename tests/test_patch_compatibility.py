from copy import deepcopy

import jsonpatch
import pytest
from pydantic import ValidationError

from piddiplatsch.catalog.patches import apply_patch
from piddiplatsch.config.schema import CatalogConfig

RULE = "missing_asset_alternate"


def test_rules_are_explicitly_enabled_and_preserve_original_inputs():
    item = {"assets": {"file/a~b.nc": {"href": "original"}}}
    operations = [
        {
            "op": "add",
            "path": "/assets/file~1a~0b.nc/alternate/replica",
            "value": {"href": "replica"},
        }
    ]
    original = deepcopy((item, operations))
    with pytest.raises(jsonpatch.JsonPointerException):
        apply_patch(item, operations, [])
    patched, repairs = apply_patch(item, operations, [RULE])
    assert patched["assets"]["file/a~b.nc"]["alternate"] == {
        "replica": {"href": "replica"}
    }
    assert [(repair.rule, repair.path) for repair in repairs] == [
        (RULE, "/assets/file~1a~0b.nc/alternate")
    ]
    assert (item, operations) == original


@pytest.mark.parametrize(
    "path",
    [
        "/assets/missing/alternate/replica",
        "/properties/alternate/replica",
        "/assets/file/other/replica",
        "/assets/file/alternate/replica/nested",
    ],
)
def test_unknown_missing_structures_still_fail(path):
    item = {"assets": {"file": {}}}
    with pytest.raises((jsonpatch.JsonPatchException, jsonpatch.JsonPointerException)):
        apply_patch(item, [{"op": "add", "path": path, "value": {}}], [RULE])
    assert item == {"assets": {"file": {}}}


@pytest.mark.parametrize("existing", [None, [], "invalid", 7])
def test_existing_invalid_alternate_is_not_overwritten(existing):
    item = {"assets": {"file": {"alternate": existing}}}
    with pytest.raises(
        (jsonpatch.JsonPatchException, jsonpatch.JsonPointerException, TypeError)
    ):
        apply_patch(
            item,
            [{"op": "add", "path": "/assets/file/alternate/replica", "value": {}}],
            [RULE],
        )
    assert item["assets"]["file"]["alternate"] == existing


def test_existing_alternates_are_preserved_without_a_repair():
    item = {"assets": {"file": {"alternate": {"old": {"href": "keep"}}}}}
    patched, repairs = apply_patch(
        item, [{"op": "add", "path": "/assets/file/alternate/new", "value": {}}], [RULE]
    )
    assert patched["assets"]["file"]["alternate"] == {
        "old": {"href": "keep"},
        "new": {},
    }
    assert repairs == ()


@pytest.mark.parametrize(
    "operation",
    [
        {"op": "replace", "path": "/assets/file/alternate/replica", "value": {}},
        {"op": "remove", "path": "/assets/file/alternate/replica"},
        {"op": "test", "path": "/assets/file/alternate/replica", "value": {}},
    ],
)
def test_only_add_operations_can_be_repaired(operation):
    with pytest.raises((jsonpatch.JsonPatchException, jsonpatch.JsonPointerException)):
        apply_patch({"assets": {"file": {}}}, [operation], [RULE])


def test_rules_respect_operation_order_and_do_not_mutate_inserted_event_values():
    operations = [
        {"op": "add", "path": "/assets/file", "value": {"href": "original"}},
        {"op": "add", "path": "/assets/file/alternate/replica", "value": {}},
        {"op": "replace", "path": "/assets/file/href", "value": "updated"},
    ]
    before = deepcopy(operations)
    item, repairs = apply_patch({"assets": {}}, operations, [RULE])
    assert item["assets"]["file"]["href"] == "updated"
    assert len(repairs) == 1
    assert operations == before


@pytest.mark.parametrize("names", [["unknown"], [RULE, RULE], RULE, [True]])
def test_invalid_rule_configuration_is_rejected(names):
    with pytest.raises(ValidationError):
        CatalogConfig(patch_compatibility_rules=names)


def test_strict_behavior_is_default():
    assert CatalogConfig().patch_compatibility_rules == []


def test_new_named_rule_can_be_added_without_changing_patch_engine(monkeypatch):
    from piddiplatsch.catalog.patches import PATCH_RULES

    def extra_rule(item, operation):
        if (
            operation["op"] == "add"
            and operation["path"] == "/properties/example/value"
        ):
            item["properties"]["example"] = {}
            return "/properties/example"
        return None

    monkeypatch.setitem(PATCH_RULES, "example_rule", extra_rule)
    settings = CatalogConfig(patch_compatibility_rules=[RULE, "example_rule"])
    item, repairs = apply_patch(
        {"assets": {"file": {}}, "properties": {}},
        [
            {"op": "add", "path": "/assets/file/alternate/replica", "value": {}},
            {"op": "add", "path": "/properties/example/value", "value": "added"},
        ],
        settings.patch_compatibility_rules,
    )
    assert item["properties"]["example"]["value"] == "added"
    assert [repair.rule for repair in repairs] == [RULE, "example_rule"]
