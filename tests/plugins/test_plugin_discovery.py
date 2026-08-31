import pytest

from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.core.registry import get_plugin, list_plugins

pytestmark = [pytest.mark.plugin]


def test_cmip6_plugin_discovered():
    assert "cmip6" in list_plugins()
    assert "cmip6plus" in list_plugins()
    assert "cmip7" in list_plugins()
    assert "cordex-cmip6" in list_plugins()


def test_cmip6_plugin_instantiation():
    proc = get_plugin("cmip6").make_processor(publish=False)
    assert isinstance(proc, BaseProcessor)


def test_cmip7_plugin_instantiation():
    proc = get_plugin("cmip7").make_processor(publish=False)
    assert isinstance(proc, BaseProcessor)


def test_cmip6plus_plugin_instantiation():
    proc = get_plugin("cmip6plus").make_processor(publish=False)
    assert isinstance(proc, BaseProcessor)


def test_cordex_cmip6_plugin_instantiation():
    proc = get_plugin("cordex-cmip6").make_processor(publish=False)
    assert isinstance(proc, BaseProcessor)
