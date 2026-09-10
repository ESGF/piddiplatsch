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
    assert "piddi_conda_env: /opt/piddiplatsch/.conda" in playbook
    assert "ansible.builtin.user:" in playbook
    assert "ansible.builtin.pip:" not in playbook
    assert '"{{ piddi_executable }}"' in playbook
    assert "piddi_venv" not in playbook
    assert "Require a manually installed Piddiplatsch executable" in playbook
    assert "Validate merged Piddiplatsch configuration" in playbook


def test_ansible_enables_all_current_projects_by_default():
    playbook = (PROJECT_ROOT / "deploy" / "ansible" / "piddi.yml").read_text()

    assert "piddi_topic: ESGF-PUBLICATIONS" in playbook
    for project in ("cmip6", "cmip6plus", "cmip7", "cordex-cmip6"):
        assert f"      - {project}\n" in playbook


def test_custom_variables_example_only_contains_site_overrides():
    example = (PROJECT_ROOT / "deploy" / "ansible" / "custom.yml.example").read_text()

    assert "piddi_kafka:" in example
    assert "piddi_config_extra:" in example
    assert "\npiddi_projects:" not in example
    assert "\npiddi_output_dir:" not in example
    assert "\npiddi_log_level:" not in example


def test_ansible_renders_piddi_configuration_template():
    playbook = (PROJECT_ROOT / "deploy" / "ansible" / "piddi.yml").read_text()
    template = (
        PROJECT_ROOT / "deploy" / "ansible" / "templates" / "piddi.toml.j2"
    ).read_text()

    assert "templates/piddi.toml.j2" in playbook
    assert "piddi_projects" in template
    assert "piddi_kafka.items()" in template
    assert "piddi_config_extra" in template


def test_playbook_loads_local_custom_overrides_with_fallback():
    playbook = (PROJECT_ROOT / "deploy" / "ansible" / "piddi.yml").read_text()

    assert "Include variables from custom.yml when present" in playbook
    assert "with_first_found:" in playbook
    assert "- custom.yml" in playbook
    assert "- null.yml" in playbook


def test_make_play_targets_localhost():
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    playbook = (PROJECT_ROOT / "deploy" / "ansible" / "piddi.yml").read_text()

    assert "play: ## deploy piddi" in makefile
    assert "$(ANSIBLE_ARGS) -i localhost," in makefile
    assert "hosts: localhost" in playbook
    assert "connection: local" in playbook


def test_logrotate_policy_bounds_file_size_and_retention():
    policy = (PROJECT_ROOT / "etc" / "logrotate" / "piddi").read_text()

    assert "/var/log/piddi/piddi.log" in policy
    assert "maxsize 100M" in policy
    assert "rotate 14" in policy
    assert "compress" in policy


def test_vagrantfile_targets_almalinux_with_host_appropriate_providers():
    vagrantfile = (PROJECT_ROOT / "Vagrantfile").read_text()

    assert 'config.vm.box = "almalinux/9"' in vagrantfile
    assert 'RbConfig::CONFIG["host_os"]' in vagrantfile
    assert 'RbConfig::CONFIG["host_cpu"]' in vagrantfile
    assert 'config.vm.provider "parallels"' in vagrantfile
    assert 'config.vm.provider "libvirt"' in vagrantfile
    assert 'config.vm.provider "virtualbox"' in vagrantfile
    for package in (
        "ansible-core",
        "byobu",
        "git",
        "python3.11",
        "vim-enhanced",
    ):
        assert package in vagrantfile


def test_vagrantfile_installs_verified_miniforge_for_supported_architectures():
    vagrantfile = (PROJECT_ROOT / "Vagrantfile").read_text()

    assert "miniforge_version=26.7.2-0" in vagrantfile
    assert "miniforge_root=/opt/conda" in vagrantfile
    assert "aarch64)" in vagrantfile
    assert "x86_64)" in vagrantfile
    assert "mktemp --suffix=.sh" in vagrantfile
    assert "sha256sum --check --status" in vagrantfile
    assert "/etc/profile.d/miniforge.sh" in vagrantfile
