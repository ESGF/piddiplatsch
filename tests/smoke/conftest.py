"""Fixtures for smoke tests requiring Docker services."""

from pathlib import Path

import pytest

from piddiplatsch.handles.api import get_handle_backend
from piddiplatsch.testing.kafka_client import ensure_topic_exists_from_config


@pytest.fixture
def testfile(testdata_path: Path):
    """Fixture returning a function to resolve test file paths."""

    def _resolve(*parts) -> Path:
        p = testdata_path.joinpath(*parts)
        if not p.exists():
            raise FileNotFoundError(f"Test file not found: {p}")
        return p

    return _resolve


@pytest.fixture
def handle_client():
    """
    Returns the configured REST backend connected to the mock Handle Service.
    Requires Docker services to be running.
    """
    return get_handle_backend(publish=True)


@pytest.fixture(scope="session", autouse=True)
def ensure_kafka_topic_exists_for_smoke():
    """Ensure Kafka topic exists before running smoke tests (auto-used)."""
    ensure_topic_exists_from_config()
