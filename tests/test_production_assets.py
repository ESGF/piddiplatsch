from pathlib import Path

from piddiplatsch.config.config import Config

PROJECT_ROOT = Path(__file__).parent.parent


def test_production_override_is_valid_with_packaged_defaults():
    configured = Config()
    configured.load_user_config(str(PROJECT_ROOT / "etc" / "piddi-production.toml"))

    errors, _ = configured.validate()

    assert errors == []
    assert configured.get("consumer", "output_dir") == "/var/lib/piddi"
    assert configured.get("logging", "file") == "/var/log/piddi/piddi.log"


def test_service_uses_production_config_and_silent_progress():
    unit = (
        PROJECT_ROOT / "deploy" / "ansible" / "templates" / "piddi.service.j2"
    ).read_text()

    assert "--config /etc/piddi/piddi.toml --silent consume" in unit
    assert "RequiresMountsFor=/var/lib/piddi" in unit


def test_ansible_deployment_is_safe_by_default():
    playbook = (PROJECT_ROOT / "deploy" / "ansible" / "piddi.yml").read_text()

    assert "piddi_enable_service: false" in playbook
    assert "ansible.builtin.user:" in playbook
    assert "ansible.builtin.pip:" in playbook
    assert "Validate merged Piddiplatsch configuration" in playbook


def test_logrotate_policy_bounds_file_size_and_retention():
    policy = (PROJECT_ROOT / "etc" / "logrotate" / "piddi").read_text()

    assert "/var/log/piddi/piddi.log" in policy
    assert "maxsize 100M" in policy
    assert "rotate 14" in policy
    assert "compress" in policy
