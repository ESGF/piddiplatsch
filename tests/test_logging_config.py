import logging
from unittest.mock import patch

import pytest

from piddiplatsch.config.config import Config


@pytest.mark.parametrize(
    ("verbosity", "debug", "expected_level"),
    [
        (0, False, logging.WARNING),
        (1, False, logging.INFO),
        (2, False, logging.DEBUG),
        (0, True, logging.DEBUG),
    ],
)
@patch("piddiplatsch.config.config.logging.basicConfig")
@patch("piddiplatsch.config.config.WatchedFileHandler")
def test_logging_level_precedence(
    watched_handler, basic_config, verbosity, debug, expected_level
):
    configured = Config()
    configured._set(
        "logging", None, {"level": "WARNING", "file": "/var/log/piddi/piddi.log"}
    )

    configured.configure_logging(verbosity=verbosity, debug=debug)

    watched_handler.assert_called_once_with(
        "/var/log/piddi/piddi.log", encoding="utf-8"
    )
    assert basic_config.call_args.kwargs["level"] == expected_level
    assert basic_config.call_args.kwargs["force"] is True


@patch("piddiplatsch.config.config.logging.basicConfig")
@patch("piddiplatsch.config.config.RichHandler")
def test_empty_log_file_uses_console(rich_handler, basic_config):
    configured = Config()
    configured._set("logging", None, {"level": "ERROR", "file": ""})

    configured.configure_logging()

    rich_handler.assert_called_once_with(rich_tracebacks=True)
    assert basic_config.call_args.kwargs["level"] == logging.ERROR


def test_configure_logging_rejects_invalid_level():
    configured = Config()
    configured._set("logging", None, {"level": "LOUD", "file": ""})

    with pytest.raises(ValueError, match="Invalid logging level: LOUD"):
        configured.configure_logging()
