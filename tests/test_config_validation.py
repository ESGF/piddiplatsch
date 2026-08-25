from piddiplatsch.config.schema import validate_config


def _base_config():
    return {
        "consumer": {"projects": ["cmip6"], "topic": "CMIP6"},
        # Keep handle as jsonl for tests to avoid publication credentials
        "handle": {"backend": "jsonl"},
        # Disable lookups unless explicitly tested
        "lookup": {"enabled": False},
    }


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
