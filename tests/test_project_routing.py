from __future__ import annotations

from typing import Any

import pytest

from piddiplatsch.consumer import feed_messages_direct
from piddiplatsch.core import registry
from piddiplatsch.core.plugin import PluginSpec
from piddiplatsch.core.routing import ProjectRouter, extract_project_id
from piddiplatsch.result import ProcessingResult


class RecordingProcessor:
    def __init__(self, name: str, calls: list, **kwargs: Any) -> None:
        self.name = name
        self.calls = calls
        self.kwargs = kwargs

    def preflight_check(self, stop_on_transient_skip: bool = True) -> None:
        self.calls.append((self.name, "preflight", stop_on_transient_skip))

    def process(self, key: str, value: dict[str, Any]) -> ProcessingResult:
        self.calls.append((self.name, "process", key))
        return ProcessingResult(key=key, success=True, num_handles=1)


@pytest.fixture
def isolated_registry(monkeypatch):
    monkeypatch.setattr(registry, "_PLUGINS", dict(registry._PLUGINS))


def publication(project: str, *, properties_project: str | None = None) -> dict:
    properties = {}
    if properties_project is not None:
        properties["project"] = properties_project
    return {
        "data": {
            "type": "STAC",
            "payload": {
                "collection_id": project,
                "method": "POST",
                "item": {"collection": project, "properties": properties},
            },
        }
    }


def register_recording_plugin(name: str, project_id: str, calls: list) -> None:
    registry.register_plugin(
        PluginSpec(
            name=name,
            project_ids=(project_id,),
            make_processor=lambda **kwargs: RecordingProcessor(name, calls, **kwargs),
        )
    )


def test_extract_project_id_prefers_publication_envelope():
    assert extract_project_id(publication("CMIP6")) == "CMIP6"


def test_extract_project_id_uses_full_item_fallbacks():
    message = publication("CMIP7")
    del message["data"]["payload"]["collection_id"]
    assert extract_project_id(message) == "CMIP7"

    del message["data"]["payload"]["item"]["collection"]
    message["data"]["payload"]["item"]["properties"]["project"] = ["CMIP7"]
    assert extract_project_id(message) == "CMIP7"


def test_extract_project_id_rejects_conflicting_fields():
    message = publication("CMIP6", properties_project="CMIP7")
    with pytest.raises(ValueError, match="Conflicting project identifiers"):
        extract_project_id(message)


def test_router_filters_unselected_projects(isolated_registry):
    calls = []
    router = ProjectRouter(["cmip6"], dry_run=True)

    result = router.process("cordex-key", publication("CORDEX-CMIP6"))

    assert result.success
    assert result.filtered
    assert result.project == "cordex-cmip6"
    assert calls == []


def test_router_dispatches_to_one_of_several_plugins(isolated_registry):
    calls = []
    register_recording_plugin("cmip7-test", "CMIP7-TEST", calls)
    register_recording_plugin("cordex-test", "CORDEX-TEST", calls)
    router = ProjectRouter(["cmip7-test", "cordex-test"], dry_run=True)

    result = router.process("cordex-key", publication("cordex-test"))

    assert result.success
    assert not result.filtered
    assert result.project == "cordex-test"
    assert result.plugin == "cordex-test"
    assert calls == [("cordex-test", "process", "cordex-key")]


def test_router_all_selects_every_registered_plugin(isolated_registry):
    calls = []
    register_recording_plugin("cmip7-all-test", "CMIP7-ALL-TEST", calls)
    router = ProjectRouter("all", dry_run=True)

    result = router.process("cmip7-key", publication("CMIP7-ALL-TEST"))

    assert result.plugin == "cmip7-all-test"


def test_router_preflights_only_selected_plugins(isolated_registry):
    calls = []
    register_recording_plugin("selected-test", "SELECTED-TEST", calls)
    register_recording_plugin("disabled-test", "DISABLED-TEST", calls)
    router = ProjectRouter(["selected-test"], dry_run=True)

    router.preflight_check(stop_on_transient_skip=False)

    assert calls == [("selected-test", "preflight", False)]


def test_registry_rejects_overlapping_project_identifiers(isolated_registry):
    calls = []
    register_recording_plugin("first-overlap-test", "OVERLAP-TEST", calls)

    with pytest.raises(ValueError, match="project identifiers already registered"):
        register_recording_plugin("second-overlap-test", "overlap-test", calls)


def test_filtered_result_is_counted_without_failure(isolated_registry):
    router = ProjectRouter(["cmip6"], dry_run=True)

    result = feed_messages_direct(
        [("other-key", publication("CMIP7"))],
        processor=router,
        dry_run=True,
    )

    assert result.total == 1
    assert result.succeeded == 0
    assert result.failed == 0
    assert result.filtered == 1
