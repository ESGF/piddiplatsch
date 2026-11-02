"""Test to verify processor registry extensibility."""

from functools import partial

import pytest

from piddiplatsch.core import registry
from piddiplatsch.core.plugin import PluginSpec
from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.core.registry import (
    get_plugin,
    list_plugins,
    register_plugin,
)
from piddiplatsch.result import ProcessingResult


@pytest.fixture(autouse=True)
def isolate_registry(monkeypatch):
    monkeypatch.setattr(registry, "_PLUGINS", registry._PLUGINS.copy())


class StubProcessor(BaseProcessor):
    def __init__(self, custom_param=None, **kwargs):
        super().__init__(**kwargs)
        self.custom_param = custom_param

    def process(self, key, value):
        return ProcessingResult(key=key, success=True)


def register_test_plugin(name, factory=StubProcessor, *, replace=False):
    register_plugin(
        PluginSpec(
            name=name,
            project_ids=(name,),
            make_processor=factory,
        ),
        replace=replace,
    )


def make_processor(name, **kwargs):
    return get_plugin(name).make_processor(**kwargs)


def test_unknown_processor_raises_error():
    """Test that requesting unknown processor raises ValueError."""
    with pytest.raises(ValueError, match=r"unknown_processor.*Available plugins"):
        get_plugin("unknown_processor")


def test_register_custom_processor():
    """Test registering a custom processor."""

    register_test_plugin("test_processor")

    assert "test_processor" in list_plugins()

    processor = make_processor("test_processor", publish=False)
    assert isinstance(processor, StubProcessor)

    result = processor.process("test-key", {"data": "test"})
    assert result.success is True
    assert result.key == "test-key"


def test_processor_receives_kwargs():
    """Test that processor receives constructor kwargs."""

    register_test_plugin("configurable")

    processor = make_processor("configurable", custom_param="test_value", publish=False)
    assert processor.custom_param == "test_value"


def test_register_requires_explicit_replace():
    """Test that duplicate plugin names fail unless replacement is explicit."""

    first = partial(StubProcessor, custom_param="v1")
    second = partial(StubProcessor, custom_param="v2")
    register_test_plugin("overwrite_test", first)
    assert make_processor("overwrite_test").custom_param == "v1"

    with pytest.raises(ValueError, match="already registered"):
        register_test_plugin("overwrite_test", second)
    assert get_plugin("overwrite_test").make_processor is first

    register_test_plugin("overwrite_test", second, replace=True)
    assert make_processor("overwrite_test").custom_param == "v2"


def test_multiple_processors_coexist():
    """Test that multiple processors can be registered and used independently."""

    register_test_plugin("proc_a", partial(StubProcessor, custom_param="A"))
    register_test_plugin("proc_b", partial(StubProcessor, custom_param="B"))

    assert {"proc_a", "proc_b"} <= set(list_plugins())
    assert make_processor("proc_a").custom_param == "A"
    assert make_processor("proc_b").custom_param == "B"
