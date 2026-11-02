"""Test to verify processor registry extensibility."""

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


def register_test_plugin(name, processor_class, *, replace=False):
    register_plugin(
        PluginSpec(
            name=name,
            project_ids=(name,),
            make_processor=processor_class,
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

    class TestProcessor(BaseProcessor):
        def process(self, key: str, value: dict) -> ProcessingResult:
            return ProcessingResult(key=key, success=True)

    register_test_plugin("test_processor", TestProcessor)

    assert "test_processor" in list_plugins()

    processor = make_processor("test_processor", publish=False)
    assert isinstance(processor, TestProcessor)

    result = processor.process("test-key", {"data": "test"})
    assert result.success is True
    assert result.key == "test-key"


def test_processor_receives_kwargs():
    """Test that processor receives constructor kwargs."""

    class ConfigurableProcessor(BaseProcessor):
        def __init__(self, custom_param=None, **kwargs):
            super().__init__(**kwargs)
            self.custom_param = custom_param

        def process(self, key: str, value: dict) -> ProcessingResult:
            return ProcessingResult(key=key, success=True)

    register_test_plugin("configurable", ConfigurableProcessor)

    processor = make_processor("configurable", custom_param="test_value", publish=False)
    assert processor.custom_param == "test_value"


def test_register_requires_explicit_replace():
    """Test that duplicate plugin names fail unless replacement is explicit."""

    class FirstProcessor(BaseProcessor):
        processor_version = "v1"

        def process(self, key: str, value: dict) -> ProcessingResult:
            return ProcessingResult(key=key, success=True)

    class SecondProcessor(BaseProcessor):
        processor_version = "v2"

        def process(self, key: str, value: dict) -> ProcessingResult:
            return ProcessingResult(key=key, success=True)

    register_test_plugin("overwrite_test", FirstProcessor)
    processor1 = make_processor("overwrite_test", publish=False)
    assert processor1.processor_version == "v1"

    with pytest.raises(ValueError, match="already registered"):
        register_test_plugin("overwrite_test", SecondProcessor)

    register_test_plugin("overwrite_test", SecondProcessor, replace=True)
    processor2 = make_processor("overwrite_test", publish=False)
    assert processor2.processor_version == "v2"


def test_multiple_processors_coexist():
    """Test that multiple processors can be registered and used independently."""

    class ProcessorA(BaseProcessor):
        name = "A"

        def process(self, key: str, value: dict) -> ProcessingResult:
            return ProcessingResult(key=key, success=True)

    class ProcessorB(BaseProcessor):
        name = "B"

        def process(self, key: str, value: dict) -> ProcessingResult:
            return ProcessingResult(key=key, success=True)

    register_test_plugin("proc_a", ProcessorA)
    register_test_plugin("proc_b", ProcessorB)

    plugins = list_plugins()
    assert "proc_a" in plugins
    assert "proc_b" in plugins

    proc_a = make_processor("proc_a", publish=False)
    proc_b = make_processor("proc_b", publish=False)

    assert proc_a.name == "A"
    assert proc_b.name == "B"
