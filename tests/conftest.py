"""Shared fixtures for all tests."""

from copy import deepcopy
from pathlib import Path

import pytest

from piddiplatsch.config import config as _config
from piddiplatsch.monitoring.stats import stats as _stats


@pytest.fixture(scope="session", autouse=True)
def _load_tests_config():
    """Load shared test config for all suites (unit, integration, smoke)."""
    cfg_path = Path(__file__).parent / "config.toml"
    _config.load_user_config(str(cfg_path))


@pytest.fixture(autouse=True)
def _isolate_runtime_state(tmp_path):
    """Give every test isolated configuration and runtime output paths."""
    original_config = deepcopy(_config.config_data)
    _config._set("consumer", "output_dir", str(tmp_path / "outputs"))
    _config._set("stats", "enable_db", False)
    _stats.reset()
    try:
        yield
    finally:
        _config.config_data = original_config
        _stats.reset()


@pytest.fixture
def testdata_path() -> Path:
    """Fixture that provides the path to the testdata directory."""
    return Path(__file__).parent / "testdata"
