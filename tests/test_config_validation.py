from piddiplatsch.config import config
from piddiplatsch.config.schema import LookupConfig, validate_config
from piddiplatsch.core.models import strict_mode
from piddiplatsch.lookup.api import get_lookup
from piddiplatsch.lookup.base import DummyLookup


def _base_config():
    return {
        "consumer": {"projects": ["cmip6"], "topic": "CMIP6"},
        "handle": {
            "backend": "rest",
            "server_url": "https://handles.example.test",
            "prefix": "21.TEST",
            "username": "300:21.TEST/testuser",
            "password": "testpass",
        },
        # Disable lookups unless explicitly tested
        "lookup": {"enabled": False},
    }


def test_lookup_is_disabled_by_default():
    assert LookupConfig().enabled is False


def test_lookup_without_enabled_flag_uses_dummy_backend():
    config._set("lookup", None, {})

    assert isinstance(get_lookup(), DummyLookup)


def test_jsonl_is_not_a_selectable_publication_backend():
    cfg = _base_config()
    cfg["handle"]["backend"] = "jsonl"

    errors, _ = validate_config(cfg)

    assert any("handle.backend" in error for error in errors)


def test_invalid_bootstrap_servers_format():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost"}  # missing :port
    errors, _ = validate_config(cfg)
    assert errors, "Expected errors for invalid bootstrap.servers format"
    assert any(
        "bootstrap.servers" in e and "host:port" in e for e in errors
    ), f"Unexpected errors: {errors}"


def test_valid_bootstrap_servers_format():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    errors, _ = validate_config(cfg)
    assert not errors, f"Did not expect errors: {errors}"


def test_lookup_stac_requires_base_url():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    cfg["lookup"] = {"enabled": True, "backend": "stac"}
    # omit [stac].base_url
    errors, _ = validate_config(cfg)
    assert errors, "Expected error for missing [stac].base_url"
    assert any(
        "[stac].base_url" in e for e in errors
    ), f"Missing base_url error not found in: {errors}"


def test_lookup_es_requires_base_url():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    cfg["lookup"] = {"enabled": True, "backend": "es"}
    # omit [elasticsearch].base_url
    errors, _ = validate_config(cfg)
    assert errors, "Expected error for missing [elasticsearch].base_url"
    assert any(
        "[elasticsearch].base_url" in e for e in errors
    ), f"Missing base_url error not found in: {errors}"


def test_projects_must_not_be_empty_or_duplicated():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    cfg["consumer"]["projects"] = []
    errors, _ = validate_config(cfg)
    assert any("projects" in error and "empty" in error for error in errors)

    cfg["consumer"]["projects"] = ["cmip6", "CMIP6"]
    errors, _ = validate_config(cfg)
    assert any("projects" in error and "duplicates" in error for error in errors)


def test_projects_are_required():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    del cfg["consumer"]["projects"]

    errors, _ = validate_config(cfg)

    assert any("projects" in error and "required" in error for error in errors)


def test_processor_setting_is_rejected():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    cfg["consumer"]["processor"] = "cmip6"

    errors, _ = validate_config(cfg)

    assert any("processor is not supported" in error for error in errors)


def test_schema_is_strict_by_default():
    config._set("schema", None, {})

    assert strict_mode() is True


def test_named_handle_profiles_can_be_selected_per_project():
    cfg = _base_config()
    del cfg["handle"]
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    cfg["handles"] = {
        "default": "mock",
        "profiles": {
            "mock": {
                "server_url": "http://localhost:8000",
                "prefix": "21.TEST",
                "username": "300:21.TEST/testuser",
                "password": "testpass",
            },
            "dkrz-test": {
                "server_url": "https://handles.example.test",
                "prefix": "21.T14995",
                "username": "300:21.T14995/tester",
                "password": "secret",
            },
        },
    }
    cfg["plugins"] = {"cmip6": {"handle": "dkrz-test"}}

    errors, _ = validate_config(cfg)

    assert errors == []


def test_project_rejects_unknown_handle_profile():
    cfg = _base_config()
    del cfg["handle"]
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    cfg["handles"] = {
        "default": "mock",
        "profiles": {
            "mock": {
                "server_url": "http://localhost:8000",
                "prefix": "21.TEST",
                "username": "testuser",
                "password": "testpass",
            }
        },
    }
    cfg["plugins"] = {"cmip6": {"handle": "missing"}}

    errors, _ = validate_config(cfg)

    assert any("unknown Handle profile 'missing'" in error for error in errors)


def test_handle_profile_resolution_uses_project_selection():
    config._set("handle", None, {})
    config._set(
        "handles",
        None,
        {
            "default": "mock",
            "defaults": {
                "backend": "rest",
                "verify_https": True,
                "timeout": 10,
            },
            "profiles": {
                "mock": {"prefix": "21.TEST"},
                "dkrz-test": {"prefix": "21.T14995", "timeout": 20},
            },
        },
    )
    config._set("plugins", None, {"cmip6": {"handle": "dkrz-test"}})

    assert config.get_handle(project="cmip6")["prefix"] == "21.T14995"
    assert config.get_handle(project="cmip6")["backend"] == "rest"
    assert config.get_handle(project="cmip6")["timeout"] == 20
    assert config.get_handle(project="cmip7")["prefix"] == "21.TEST"
    assert config.get_handle(project="cmip7")["timeout"] == 10


def test_handle_profile_validation_applies_common_defaults():
    cfg = _base_config()
    del cfg["handle"]
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    cfg["handles"] = {
        "default": "mock",
        "defaults": {
            "backend": "rest",
            "server_url": "http://localhost:8000",
            "username": "testuser",
            "password": "testpass",
            "verify_https": True,
            "timeout": 10,
        },
        "profiles": {"mock": {"prefix": "21.TEST"}},
    }

    errors, _ = validate_config(cfg)

    assert errors == []
