import pytest

from piddiplatsch.consumer import ConsumerPipeline, DirectConsumer
from piddiplatsch.core.registry import get_plugin
from piddiplatsch.core.routing import ProjectRouter
from piddiplatsch.handles.jsonl_backend import JsonlHandleBackend

pytestmark = pytest.mark.integration


def test_plugin_uses_jsonl_backend_when_publication_is_disabled():
    processor = get_plugin("cmip6").make_processor(publish=False)
    assert isinstance(
        getattr(processor.handle_backend, "backend", None), JsonlHandleBackend
    )


def test_pipeline_initializes_jsonl_backend_when_publication_is_disabled():
    consumer = DirectConsumer(messages=[])
    pipeline = ConsumerPipeline(
        consumer,
        processor=ProjectRouter(["cmip6"], publish=False),
        publish=False,
    )
    assert isinstance(
        getattr(
            pipeline.processor.processors["cmip6"].handle_backend,
            "backend",
            None,
        ),
        JsonlHandleBackend,
    )
