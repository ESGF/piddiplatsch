import pytest

from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.core.registry import get_plugin, list_plugins

pytestmark = [pytest.mark.plugin]


def test_cmip6_plugin_discovered():
    assert "cmip6" in list_plugins()


def test_cmip6_plugin_instantiation():
    proc = get_plugin("cmip6").make_processor(dry_run=True)
    assert isinstance(proc, BaseProcessor)
