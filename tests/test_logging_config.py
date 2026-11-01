import logging
from unittest.mock import call, patch

import pytest

from piddiplatsch.config.config import Config
from piddiplatsch.config.schema import LoggingConfig


@pytest.mark.parametrize("missing_level", [False, True])
@patch("piddiplatsch.config.config.logging.basicConfig")
@patch("piddiplatsch.config.config.WatchedFileHandler")
def test_default_logging_level_is_info(watched_handler, basic_config, missing_level):
    configured = Config()
    assert configured.get("logging", "level") == LoggingConfig().level == "INFO"
    if missing_level:
        configured.config_data["logging"].pop("level")

    configured.configure_logging()

    assert basic_config.call_args.kwargs["level"] == logging.INFO


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

    assert watched_handler.call_args_list == [
        call("/var/log/piddi/piddi.log", encoding="utf-8"),
        call("/var/log/piddi/piddi.error.log", encoding="utf-8"),
    ]
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


@pytest.mark.parametrize("level", ["INFO", "DEBUG", "CRITICAL"])
@patch("piddiplatsch.config.config.logging.basicConfig")
def test_companion_error_log_keeps_errors_and_tracebacks(basic_config, tmp_path, level):
    configured = Config()
    path = tmp_path / "piddi.log"
    configured._set("logging", None, {"level": level, "file": str(path)})
    configured.configure_logging()
    options = basic_config.call_args.kwargs
    logger = logging.Logger("companion-test", level=options["level"])
    logger.handlers = options["handlers"]
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    try:
        logger.debug("debug details")
        logger.info("normal progress")
        logger.warning("a warning")
        try:
            raise ValueError("invalid item")
        except ValueError:
            logger.exception("mapping failed")
        logger.critical("critical failure")
    finally:
        for handler in logger.handlers:
            handler.close()
    errors = (tmp_path / "piddi.error.log").read_text()
    normal = path.read_text()
    assert "ERROR mapping failed" in errors
    assert "ValueError: invalid item" in errors
    assert "Traceback" in errors
    assert "CRITICAL critical failure" in errors
    assert "normal progress" not in errors
    assert "a warning" not in errors
    assert "debug details" not in errors
    assert ("normal progress" in normal) == (level != "CRITICAL")
    assert ("mapping failed" in normal) == (level != "CRITICAL")
    assert ("debug details" in normal) == (level == "DEBUG")
