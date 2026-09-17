"""Behavioral coverage for configuration diagnostics and file selection."""

import pytest
from click.testing import CliRunner

from piddiplatsch.cli import cli
from piddiplatsch.config.config import DEFAULT_CONFIG_PATH, Config


@pytest.fixture
def configured(monkeypatch):
    cfg = Config()
    cfg._set("kafka", "security.protocol", "PLAINTEXT")
    monkeypatch.setattr("piddiplatsch.cli.config", cfg)
    monkeypatch.setattr("piddiplatsch.commands.config.config", cfg)
    monkeypatch.setattr(cfg, "configure_logging", lambda **kwargs: None)
    return cfg


def test_explain_reports_file_origins_and_resolved_paths(configured, tmp_path):
    site = tmp_path / "site.toml"
    local = tmp_path / "custom.toml"
    site.write_text('[logging]\nfile = "site.log"\n[handles.defaults]\ntimeout = 23\n')
    local.write_text(
        '[consumer]\noutput_dir = "local-output"\n'
        '[stats]\ndb_path = "local.db"\n'
        '[plugins.cmip6]\nhandle_prefix = "21.LOCAL"\n'
    )
    configured.load_config_layers(local, site)
    result = CliRunner().invoke(cli, ["config", "explain", "--project", "cmip6"])
    assert result.exit_code == 0, result.output
    assert (
        "Handle timeout: 23  <- handles.defaults.timeout (site.toml)" in result.output
    )
    assert (
        'Handle prefix: "21.LOCAL"  <- plugins.cmip6.handle_prefix (custom.toml)'
        in result.output
    )
    for label, path in (
        ("defaults", DEFAULT_CONFIG_PATH.resolve()),
        ("site.toml", site),
        ("custom.toml", local),
    ):
        assert f"  {label}: {path}" in result.output
        assert result.output.count(str(path)) == 1
    for path in ("site.log", "local-output", "local.db"):
        assert str(tmp_path / path) in result.output
    assert "testpass" not in result.output


def test_explicit_missing_config_fails_even_when_default_exists(configured, tmp_path):
    (tmp_path / "custom.toml").write_text('[consumer]\ntopic = "should-not-load"\n')
    result = CliRunner().invoke(cli, ["--config", "missing.toml", "config", "show"])
    assert result.exit_code == 2
    assert "existing configuration file" in result.output
    assert configured.get("consumer", "topic") == "CMIP6"


def test_explicit_missing_default_name_also_fails(configured):
    result = CliRunner().invoke(cli, ["--config", "custom.toml", "config", "show"])
    assert result.exit_code == 2
    # Automatic, absent local config remains optional.
    result = CliRunner().invoke(cli, ["config", "show", "--section", "consumer"])
    assert result.exit_code == 0


def test_explain_log_override_and_terminal(configured, tmp_path):
    configured._set("logging", "file", "")
    result = CliRunner().invoke(cli, ["config", "explain", "--project", "cmip6"])
    assert 'Log path: "terminal"' in result.output
    result = CliRunner().invoke(
        cli, ["--log", "override.log", "config", "explain", "--project", "cmip6"]
    )
    assert result.exit_code == 0, result.output
    assert (
        f'Log path: "{tmp_path / "override.log"}"  <- --log (command line)'
        in result.output
    )


def test_origin_tracks_literal_dotted_keys_and_same_value_overrides(tmp_path):
    cfg = Config()
    local = tmp_path / "local.toml"
    local.write_text('[kafka]\n"group.id" = "piddiplatsch-001"\n')
    cfg.load_user_config(str(local))
    assert cfg.get_source(("kafka", "group.id")) == str(local)
    cfg._set("kafka", "group.id", "runtime")
    assert cfg.get_source(("kafka", "group.id")) == "runtime override"


@pytest.mark.parametrize("missing", ["sasl.username", "sasl.password"])
def test_plain_credentials_required_without_echoing_secrets(configured, missing):
    configured._set("kafka", "security.protocol", "SASL_SSL")
    configured._set("kafka", "sasl.username", "private-user")
    configured._set("kafka", "sasl.password", "private-password")
    del configured.config_data["kafka"][missing]
    errors, _ = configured.validate()
    assert any(missing in error for error in errors)
    assert "private-" not in str(errors)


@pytest.mark.parametrize(
    "protocol,mechanism", [("PLAINTEXT", "PLAIN"), ("SASL_SSL", "OAUTHBEARER")]
)
def test_other_authentication_modes_remain_available(configured, protocol, mechanism):
    configured._set("kafka", "security.protocol", protocol)
    configured._set("kafka", "sasl.mechanisms", mechanism)
    assert configured.validate()[0] == []


@pytest.mark.parametrize(
    "section,key,value",
    [
        ("stats", "heartbeat_interval_seconds", 0),
        ("stats", "sample_interval_seconds", -1),
        ("stats", "history_minutes", float("nan")),
        ("stats", "sample_retention_days", -1),
        ("stats", "summary_interval", 0),
        ("transient", "retries", -1),
        ("transient", "backoff_initial", -1),
        ("transient", "backoff_max", 0.1),
    ],
)
def test_invalid_tuning_is_reported(configured, section, key, value):
    target = (
        configured.config_data["consumer"]["transient"]
        if section == "transient"
        else configured.config_data[section]
    )
    target[key] = value
    assert configured.validate()[0]


def test_zero_retries_delay_and_retention_remain_valid(configured):
    configured.config_data["consumer"]["transient"].update(
        retries=0, backoff_initial=0, backoff_max=0
    )
    configured._set("stats", "sample_retention_days", 0)
    assert configured.validate()[0] == []


def test_typo_warnings_leave_extension_options_available(configured):
    configured._set("consumer", "outpt_dir", "typo")
    configured._set("stats", "heartbeat_interval_second", 7)
    configured._set("kafka", "vendor.setting", "supported-by-client")
    configured.config_data["plugins"]["cmip6"]["custom_extension"] = True
    errors, warnings = configured.validate()
    assert errors == []
    assert any(
        "outpt_dir" in warning and "output_dir" in warning for warning in warnings
    )
    assert any("heartbeat_interval_second" in warning for warning in warnings)
    assert not any(
        "vendor.setting" in warning or "custom_extension" in warning
        for warning in warnings
    )


def test_only_selected_profiles_and_projects_emit_warnings(configured):
    configured.config_data["handles"]["profiles"]["production"] = {
        "server_url": "https://handle.example.org",
        "prefix": "21.PROD",
        "username": "real-user",
        "password": "real-password",
    }
    configured.config_data["plugins"]["cmip7"]["handle"] = "production"
    configured._set("consumer", "projects", ["cmip7"])
    configured.config_data["plugins"]["cmip6"]["landing_page_url"] = ""
    errors, warnings = configured.validate()
    assert errors == []
    assert warnings == []
    configured._set("consumer", "projects", "all")
    errors, warnings = configured.validate()
    assert errors == []
    assert any("mock" in warning for warning in warnings)
    assert any("plugins.cmip6" in warning for warning in warnings)


def test_legacy_profile_wins_for_warnings(configured):
    configured._set(
        "handle",
        None,
        {
            "server_url": "https://handle.example.org",
            "prefix": "21.PROD",
            "username": "real-user",
            "password": "real-password",
        },
    )
    assert configured.validate() == ([], [])


@pytest.mark.parametrize(
    "command",
    [
        ["show"],
        ["validate"],
        ["explain", "--project", "cmip6"],
    ],
)
@pytest.mark.parametrize("explicit_log", [False, True])
def test_config_commands_do_not_initialize_logging(
    monkeypatch, tmp_path, command, explicit_log
):
    cfg = Config()
    cfg._set("kafka", "security.protocol", "PLAINTEXT")
    # A nonexistent parent would make file logging fail, even when run as root.
    log_path = tmp_path / "missing" / "piddi.log"
    cfg._set("logging", "file", str(log_path))
    monkeypatch.setattr("piddiplatsch.cli.config", cfg)
    monkeypatch.setattr("piddiplatsch.commands.config.config", cfg)
    monkeypatch.setattr(cfg, "load_config_layers", lambda path: None)

    def unexpected_logging(**kwargs):
        raise AssertionError("Config inspection must not initialize logging")

    monkeypatch.setattr(cfg, "configure_logging", unexpected_logging)
    options = ["--log", str(log_path)] if explicit_log else []
    before = set(tmp_path.rglob("*"))
    result = CliRunner().invoke(cli, [*options, "config", *command])
    assert result.exit_code == 0, result.output
    assert set(tmp_path.rglob("*")) == before


def test_config_validate_reports_invalid_log_level_via_schema(monkeypatch):
    cfg = Config()
    cfg._set("kafka", "security.protocol", "PLAINTEXT")
    cfg._set("logging", "level", "INVALID")
    monkeypatch.setattr("piddiplatsch.cli.config", cfg)
    monkeypatch.setattr("piddiplatsch.commands.config.config", cfg)
    monkeypatch.setattr(cfg, "load_config_layers", lambda path: None)
    result = CliRunner().invoke(cli, ["config", "validate"])
    assert result.exit_code == 1
    assert "Errors:" in result.output
    assert "logging.level" in result.output


def test_explain_distinguishes_matching_source_filenames(configured, tmp_path):
    site = tmp_path / "site" / "custom.toml"
    site.parent.mkdir()
    site.write_text("[handles.defaults]\ntimeout = 23\n")
    local = tmp_path / "custom.toml"
    local.write_text('[plugins.cmip6]\nhandle_prefix = "21.LOCAL"\n')
    configured.load_config_layers(local, site)
    result = CliRunner().invoke(cli, ["config", "explain", "--project", "cmip6"])
    assert result.exit_code == 0, result.output
    assert f"  custom.toml: {site}" in result.output
    assert f"  custom.toml [2]: {local}" in result.output
    assert (
        "Handle timeout: 23  <- handles.defaults.timeout (custom.toml)" in result.output
    )
    assert (
        'Handle prefix: "21.LOCAL"  <- plugins.cmip6.handle_prefix (custom.toml [2])'
        in result.output
    )
