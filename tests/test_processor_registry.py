"""Test to verify processor registry extensibility."""

import pytest

from piddiplatsch.core.plugin import PluginSpec
from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.core.registry import (
    get_plugin,
    list_plugins,
    register_plugin,
)
from piddiplatsch.result import ProcessingResult


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


def test_cmip6_processor_available():
    """Test that CMIP6 processor is registered by default."""
    assert "cmip6" in list_plugins()


def test_get_cmip6_processor():
    """Test getting the CMIP6 processor."""
    processor = make_processor("cmip6", dry_run=True)
    assert isinstance(processor, BaseProcessor)


def test_unknown_processor_raises_error():
    """Test that requesting unknown processor raises ValueError."""
    try:
        get_plugin("unknown_processor")
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "unknown_processor" in str(e)
        assert "Available plugins" in str(e)


def test_register_custom_processor():
    """Test registering a custom processor."""

    class TestProcessor(BaseProcessor):
        def process(self, key: str, value: dict) -> ProcessingResult:
            return ProcessingResult(key=key, success=True)

    # Register it
    register_test_plugin("test_processor", TestProcessor)

    # Verify it's available
    assert "test_processor" in list_plugins()

    # Get and use it
    processor = make_processor("test_processor", dry_run=True)
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

    processor = make_processor("configurable", custom_param="test_value", dry_run=True)
    assert processor.custom_param == "test_value"


def test_list_plugins_returns_all():
    plugins = list_plugins()
    assert isinstance(plugins, list)
    assert "cmip6" in plugins
    # At least cmip6 should be there
    assert len(plugins) >= 1


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

    # Register first version
    register_test_plugin("overwrite_test", FirstProcessor)
    processor1 = make_processor("overwrite_test", dry_run=True)
    assert processor1.processor_version == "v1"

    with pytest.raises(ValueError, match="already registered"):
        register_test_plugin("overwrite_test", SecondProcessor)

    register_test_plugin("overwrite_test", SecondProcessor, replace=True)
    processor2 = make_processor("overwrite_test", dry_run=True)
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

    # Both should be in the list
    plugins = list_plugins()
    assert "proc_a" in plugins
    assert "proc_b" in plugins

    # Both should be independently accessible
    proc_a = make_processor("proc_a", dry_run=True)
    proc_b = make_processor("proc_b", dry_run=True)

    assert proc_a.name == "A"
    assert proc_b.name == "B"


def test_processor_dry_run_flag():
    """Test that dry_run flag is properly passed to processor."""
    from piddiplatsch.handles.jsonl_backend import JsonlHandleBackend

    # Get processor with dry_run=True
    processor = make_processor("cmip6", dry_run=True)

    # Should be using JSONL backend
    assert isinstance(
        getattr(processor.handle_backend, "backend", None), JsonlHandleBackend
    )
