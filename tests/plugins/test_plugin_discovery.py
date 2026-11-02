import pytest

from piddiplatsch.core.processing import BaseProcessor
from piddiplatsch.core.registry import get_plugin, list_plugins

pytestmark = [pytest.mark.plugin]


@pytest.mark.parametrize("project", ["cmip6", "cmip6plus", "cmip7", "cordex-cmip6"])
def test_builtin_plugin_is_registered_and_creates_processor(project):
    assert project in list_plugins()
    proc = get_plugin(project).make_processor(publish=False)
    assert isinstance(proc, BaseProcessor)
