"""Project explanations must agree with runtime resolution and omit credentials."""

import pytest
from click.testing import CliRunner

from piddiplatsch.cli import cli
from piddiplatsch.config.config import Config


@pytest.fixture
def explained_config(monkeypatch):
    cfg = Config()
    monkeypatch.setattr("piddiplatsch.cli.config", cfg)
    monkeypatch.setattr("piddiplatsch.commands.config.config", cfg)
    monkeypatch.setattr(cfg, "configure_logging", lambda **kwargs: None)
    monkeypatch.setattr(cfg, "load_config_layers", lambda path: None)
    return cfg


def explain(*args):
    return CliRunner().invoke(cli, ["config", "explain", "--project", "cmip6", *args])


def test_explain_defaults(explained_config):
    result = explain()
    assert result.exit_code == 0, result.output
    assert 'Handle profile: "mock"  <- handles.default' in result.output
    assert "Handle timeout: 10  <- handles.defaults.timeout" in result.output
    assert 'STAC collection: "CMIP6"  <- plugins.cmip6.stac.collection' in result.output
    assert "Lookup enabled: false" in result.output
    assert "testpass" not in result.output
    assert "300:21.TEST/testuser" not in result.output


def test_explain_profile_override_keeps_project_prefix(explained_config):
    cfg = explained_config
    cfg.config_data["handles"]["profiles"]["other"] = {
        "server_url": "https://other.example.org",
        "prefix": "21.OTHER",
        "password": "secret-password",
        "username": "secret-user",
    }
    cfg.config_data["plugins"]["cmip6"]["handle_prefix"] = "21.PROJECT"
    cfg.config_data["plugins"]["cmip6"]["stac"]["timeout"] = 42
    result = explain("--handle-profile", "other")
    assert result.exit_code == 0, result.output
    assert 'Handle profile: "other"  <- --handle-profile' in result.output
    assert (
        'Handle prefix: "21.PROJECT"  <- plugins.cmip6.handle_prefix' in result.output
    )
    assert "https://other.example.org" in result.output
    assert "STAC timeout: 42  <- plugins.cmip6.stac.timeout" in result.output
    assert "secret-" not in result.output
    assert cfg.get_handle("cmip6", "other")["prefix"] == "21.PROJECT"


def test_explain_legacy_precedence(explained_config):
    cfg = explained_config
    cfg._set("handle", None, {"prefix": "21.LEGACY", "backend": "rest"})
    cfg._set("cmip6", None, {"handle_prefix": "21.OLD", "stac": {"collection": "OLD"}})
    result = explain("--handle-profile", "unknown")
    assert result.exit_code == 0, result.output
    assert "legacy [handle]" in result.output
    assert 'Handle prefix: "21.OLD"  <- cmip6.handle_prefix' in result.output
    assert 'STAC collection: "OLD"  <- cmip6.stac.collection' in result.output


def test_explain_unknown_profile(explained_config):
    result = explain("--handle-profile", "missing")
    assert result.exit_code != 0
    assert "Unknown Handle profile" in result.output


def test_explain_unknown_project(explained_config):
    result = CliRunner().invoke(cli, ["config", "explain", "--project", "missing"])
    assert result.exit_code != 0
    assert "not found" in result.output
