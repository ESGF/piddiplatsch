"""Public config output hides credentials without modifying runtime values."""

import json
from copy import deepcopy

import pytest
import toml
from click.testing import CliRunner

from piddiplatsch.cli import cli
from piddiplatsch.config.config import Config
from piddiplatsch.config.redaction import REDACTED, redact_config


@pytest.fixture
def secret_config(monkeypatch):
    cfg = Config()
    cfg.config_data = {
        "kafka": {
            "bootstrap.servers": "broker.example:9093",
            "client.id": "resource-id",
            "group.id": "group-id",
            "sasl.username": "private-user",
            "sasl.password": "private-password",
            "ssl.key.password": "private-passphrase",
            "sasl.oauthbearer.client.secret": "private-client-secret",
            "sasl.oauthbearer.config": "opaque-private-oauth",
        },
        "handles": {
            "profiles": {
                "production": {
                    "username": "private-handle-user",
                    "password": "private-handle-password",
                }
            }
        },
        "handle": {"password": "private-legacy-password"},
        "plugins": {
            "custom": {
                "api_key": "private-api-key",
                "nested": [{"accessToken": "private-token", "enabled": True}],
            }
        },
        "stac": {
            "base_url": "https://private-url-user:private-url-pass@example.org/catalog?token=private-query&limit=10"
        },
    }
    monkeypatch.setattr("piddiplatsch.cli.config", cfg)
    monkeypatch.setattr("piddiplatsch.commands.config.config", cfg)
    monkeypatch.setattr(cfg, "load_config_layers", lambda path: None)
    monkeypatch.setattr(cfg, "configure_logging", lambda **kwargs: None)
    return cfg


@pytest.mark.parametrize("fmt", ["toml", "json"])
@pytest.mark.parametrize(
    "selection",
    [[], ["--section", "kafka"], ["--section", "kafka", "--key", "sasl.password"]],
)
def test_show_redacts_by_default_in_all_views(secret_config, fmt, selection):
    original = deepcopy(secret_config.config_data)
    result = CliRunner().invoke(cli, ["config", "show", "--format", fmt, *selection])
    assert result.exit_code == 0, result.output
    assert "private-" not in result.output
    assert REDACTED in result.output
    parsed = json.loads(result.output) if fmt == "json" else toml.loads(result.output)
    assert parsed["kafka"]["sasl.password"] == REDACTED
    assert secret_config.config_data == original
    if not selection:
        assert parsed["kafka"]["client.id"] == "resource-id"
        assert parsed["kafka"]["bootstrap.servers"] == "broker.example:9093"
        assert parsed["plugins"]["custom"]["nested"][0]["enabled"] is True
        assert "example.org/catalog" in parsed["stac"]["base_url"]
        assert "limit=10" in parsed["stac"]["base_url"]


@pytest.mark.parametrize("fmt", ["toml", "json"])
def test_show_secrets_restores_exact_values(secret_config, fmt):
    result = CliRunner().invoke(
        cli, ["config", "show", "--format", fmt, "--show-secrets"]
    )
    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output) if fmt == "json" else toml.loads(result.output)
    assert parsed == secret_config.config_data


def test_show_secrets_with_key_selection(secret_config):
    result = CliRunner().invoke(
        cli,
        [
            "config",
            "show",
            "--section",
            "kafka",
            "--key",
            "sasl.password",
            "--show-secrets",
        ],
    )
    assert result.exit_code == 0
    assert toml.loads(result.output)["kafka"]["sasl.password"] == "private-password"


def test_redaction_keeps_nonsecret_urls_unchanged():
    url = "https://example.org/path?q=a%20b&count=5"
    assert redact_config({"base_url": url, "timeout": 10}) == {
        "base_url": url,
        "timeout": 10,
    }


def test_sensitive_keys_and_malformed_urls():
    data = {
        "ssl.key.pem": "private-pem",
        "sasl.jaas.config": "private-jaas",
        "authorization": "private-header",
        "private_key": "private-key",
        "base_url": "https://user:pass@[bad-host",
    }
    assert set(redact_config(data).values()) == {REDACTED}
