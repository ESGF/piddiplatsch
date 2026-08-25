from piddiplatsch.config.config import Config
from piddiplatsch.config.schema import validate_config


def _base_config():
    return {
        "consumer": {"projects": ["cmip6"], "topic": "CMIP6"},
        # Keep handle as jsonl for tests to avoid pyhandle requirements
        "handle": {"backend": "jsonl"},
        # Disable lookups unless explicitly tested
        "lookup": {"enabled": False},
    }


def test_invalid_bootstrap_servers_format():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost"}  # missing :port
    errors, _ = validate_config(cfg)
    assert errors, "Expected errors for invalid bootstrap.servers format"
    assert any("bootstrap.servers" in e and "host:port" in e for e in errors), f"Unexpected errors: {errors}"


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
    assert any("[stac].base_url" in e for e in errors), f"Missing base_url error not found in: {errors}"


def test_lookup_es_requires_base_url():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    cfg["lookup"] = {"enabled": True, "backend": "es"}
    # omit [elasticsearch].base_url
    errors, _ = validate_config(cfg)
    assert errors, "Expected error for missing [elasticsearch].base_url"
    assert any("[elasticsearch].base_url" in e for e in errors), f"Missing base_url error not found in: {errors}"


def test_projects_must_not_be_empty_or_duplicated():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    cfg["consumer"]["projects"] = []
    errors, _ = validate_config(cfg)
    assert any("projects" in error and "empty" in error for error in errors)

    cfg["consumer"]["projects"] = ["cmip6", "CMIP6"]
    errors, _ = validate_config(cfg)
    assert any("projects" in error and "duplicates" in error for error in errors)


def test_legacy_processor_is_valid_but_deprecated():
    cfg = _base_config()
    cfg["kafka"] = {"bootstrap.servers": "localhost:39092"}
    del cfg["consumer"]["projects"]
    cfg["consumer"]["processor"] = "cmip6"

    errors, warnings = validate_config(cfg)

    assert not errors
    assert any("processor is deprecated" in warning for warning in warnings)


def test_legacy_processor_override_removes_default_projects(tmp_path):
    path = tmp_path / "legacy.toml"
    path.write_text(
        '[consumer]\nprocessor = "cmip6"\ntopic = "shared-topic"\n',
        encoding="utf-8",
    )
    legacy_config = Config()

    legacy_config.load_user_config(str(path))

    consumer = legacy_config.get("consumer")
    assert consumer["processor"] == "cmip6"
    assert "projects" not in consumer
