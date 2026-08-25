"""Test to verify processor registry extensibility."""

import pytest

from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.core.registry import (
    get_processor,
    list_processors,
    register_processor,
)
from piddiplatsch.result import ProcessingResult


def test_cmip6_processor_available():
    """Test that CMIP6 processor is registered by default."""
    assert "cmip6" in list_processors()


def test_get_cmip6_processor():
    """Test getting the CMIP6 processor."""
    processor = get_processor("cmip6", dry_run=True)
    assert isinstance(processor, BaseProcessor)


def test_unknown_processor_raises_error():
    """Test that requesting unknown processor raises ValueError."""
    try:
        get_processor("unknown_processor")
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
    register_processor("test_processor", TestProcessor)

    # Verify it's available
    assert "test_processor" in list_processors()

    # Get and use it
    processor = get_processor("test_processor", dry_run=True)
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

    register_processor("configurable", ConfigurableProcessor)

    processor = get_processor("configurable", custom_param="test_value", dry_run=True)
    assert processor.custom_param == "test_value"


def test_list_processors_returns_all():
    """Test that list_processors returns all registered processors."""
    processors = list_processors()
    assert isinstance(processors, list)
    assert "cmip6" in processors
    # At least cmip6 should be there
    assert len(processors) >= 1


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
    register_processor("overwrite_test", FirstProcessor)
    processor1 = get_processor("overwrite_test", dry_run=True)
    assert processor1.processor_version == "v1"

    with pytest.raises(ValueError, match="already registered"):
        register_processor("overwrite_test", SecondProcessor)

    register_processor("overwrite_test", SecondProcessor, replace=True)
    processor2 = get_processor("overwrite_test", dry_run=True)
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

    register_processor("proc_a", ProcessorA)
    register_processor("proc_b", ProcessorB)

    # Both should be in the list
    processors = list_processors()
    assert "proc_a" in processors
    assert "proc_b" in processors

    # Both should be independently accessible
    proc_a = get_processor("proc_a", dry_run=True)
    proc_b = get_processor("proc_b", dry_run=True)

    assert proc_a.name == "A"
    assert proc_b.name == "B"


def test_processor_dry_run_flag():
    """Test that dry_run flag is properly passed to processor."""
    from piddiplatsch.handles.jsonl_backend import JsonlHandleBackend

    # Get processor with dry_run=True
    processor = get_processor("cmip6", dry_run=True)

    # Should be using JSONL backend
    assert isinstance(getattr(processor.handle_backend, "backend", None), JsonlHandleBackend)
