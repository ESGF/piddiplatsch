import runpy
from pathlib import Path

import pytest
import toml
import yaml

from piddiplatsch.config.config import Config

PROJECT_ROOT = Path(__file__).parent.parent


def test_esgf_example_inherits_esgf_authentication():
    configured = Config()
    configured.load_user_config(str(PROJECT_ROOT / "etc" / "esgf-example.toml"))

    assert configured.get("kafka", "security.protocol") == "SASL_SSL"
    assert configured.get("kafka", "sasl.mechanisms") == "PLAIN"
    assert configured.get("catalog", "enabled") is True
    assert configured.get("catalog", "backend") == "sqlite"
    assert configured.get("catalog", "version_history") is False
    assert configured.get("lookup", "backend") == "catalog"
    assert configured.get("lookup", "enabled") is False
    errors, _ = configured.validate()
    assert errors == []


def test_test_config_overrides_production_authentication():
    configured = Config()
    configured.load_user_config(str(PROJECT_ROOT / "tests" / "config.toml"))

    assert configured.get("kafka", "security.protocol") == "PLAINTEXT"
    assert configured.get("kafka", "bootstrap.servers") == "localhost:39092"
    errors, _ = configured.validate()
    assert errors == []


def test_ansible_render_matches_esgf_example_and_production_paths(tmp_path):
    render = runpy.run_path(str(PROJECT_ROOT / "deploy/ansible/render_config.py"))[
        "render_config"
    ]
    rendered = render(PROJECT_ROOT / "etc/esgf-example.toml")
    rendered_path = tmp_path / "production.toml"
    rendered_path.write_text(rendered)
    configured = Config()
    configured.load_user_config(str(rendered_path))
    example = Config()
    example.load_user_config(str(PROJECT_ROOT / "etc" / "esgf-example.toml"))

    errors, _ = configured.validate()
    assert errors == []
    assert configured.get("consumer", "output_dir") == "/var/lib/piddi"
    assert configured.get("logging", "file") == "/var/log/piddi/piddi.log"
    assert configured.get("stats", "enable_db") is True
    assert configured.get("stats", "db_path") == "/var/lib/piddi/piddi.db"
    assert configured.get("catalog", "enabled") is True
    assert configured.get("catalog", "backend") == "sqlite"
    assert configured.get("catalog", "version_history") is False
    assert configured.get("lookup", "backend") == "catalog"
    assert (
        configured.get_stac("cmip6")["base_url"]
        == example.get_stac("cmip6")["base_url"]
    )
    assert configured.get("kafka") == example.get("kafka")
    assert configured.get("consumer", "topic") == example.get("consumer", "topic")
    assert configured.get("consumer", "projects") == example.get("consumer", "projects")
    for project in configured.get("consumer", "projects"):
        assert configured.get_handle(project) == example.get_handle(project)
        assert configured.get_plugin(project) == example.get_plugin(project)


def test_deployment_preserves_explicit_application_overrides(tmp_path):
    render = runpy.run_path(str(PROJECT_ROOT / "deploy/ansible/render_config.py"))[
        "render_config"
    ]
    source = tmp_path / "custom.toml"
    overrides = {
        "consumer": {"output_dir": "/data/piddi", "projects": ["cmip7"]},
        "logging": {"file": "", "level": "INFO"},
        "stats": {"enable_db": False, "db_path": "/data/stats.db"},
        "catalog": {"directory": "/data/catalog", "version_history": True},
        "lookup": {"backend": "stac", "enabled": False},
        "stac": {"base_url": "https://site-stac.example.test", "timeout": 20},
        "kafka": {"sasl.mechanisms": "SCRAM-SHA-512", "ssl.ca.location": "/etc/ca.pem"},
        "plugins": {"cmip7": {"handle_prefix": "21.OTHER"}},
    }
    source.write_text(toml.dumps(overrides))
    original = source.read_bytes()
    assert toml.loads(render(source)) == overrides
    assert source.read_bytes() == original
    # Rendering another file must not retain values from the previous render.
    source.write_text('[kafka]\n"security.protocol" = "PLAINTEXT"\n')
    assert toml.loads(render(source))["consumer"]["output_dir"] == "/var/lib/piddi"


def test_deployment_requires_existing_valid_source(tmp_path):
    render = runpy.run_path(str(PROJECT_ROOT / "deploy/ansible/render_config.py"))[
        "render_config"
    ]
    source = tmp_path / "custom.toml"
    with pytest.raises(FileNotFoundError):
        render(source)
    source.write_text("[invalid")
    with pytest.raises(toml.TomlDecodeError):
        render(source)


def test_service_uses_production_config_and_silent_progress():
    unit = (
        PROJECT_ROOT / "deploy" / "ansible" / "templates" / "piddi.service.j2"
    ).read_text()

    assert "--config /etc/piddi/piddi.toml --silent harvest --idle-timeout 0" in unit
    assert "RequiresMountsFor=/var/lib/piddi" in unit


def test_ansible_deployment_is_safe_by_default():
    playbook = (PROJECT_ROOT / "deploy" / "ansible" / "piddi.yml").read_text()

    assert "piddi_enable_service: false" in playbook
    assert "piddi_conda_env: /opt/piddiplatsch/.conda" in playbook
    assert "piddi_command_link: /usr/local/sbin/piddi" in playbook
    assert "ansible.builtin.user:" in playbook
    assert "ansible.builtin.pip:" not in playbook
    assert '"{{ piddi_executable }}"' in playbook
    assert "piddi_venv" not in playbook
    assert "Require a manually installed Piddiplatsch executable" in playbook
    assert "Remove the legacy administrator command managed by Piddiplatsch" in playbook
    assert "Install Piddiplatsch administrator command" in playbook
    assert "Render and validate candidate configuration before installation" in playbook


def test_ansible_uses_one_application_config():
    playbook = yaml.safe_load((PROJECT_ROOT / "deploy/ansible/piddi.yml").read_text())[
        0
    ]
    variables = playbook["vars"]
    assert variables["piddi_config_source"] == "{{ playbook_dir }}/../../custom.toml"
    assert "piddi_kafka" not in variables
    assert "piddi_config_extra" not in variables
    assert "piddi_projects" not in variables
    example = yaml.safe_load(
        (PROJECT_ROOT / "deploy/ansible/custom.yml.example").read_text()
    )
    assert example is None  # Optional deployment controls are all commented.
    tasks = {task["name"]: task for task in playbook["tasks"]}
    render = tasks["Render and validate candidate configuration before installation"]
    assert render["no_log"] is True
    assert "piddi_config_source" in render["ansible.builtin.script"]["cmd"]
    names = [task["name"] for task in playbook["tasks"]]
    assert names.index(
        "Render and validate candidate configuration before installation"
    ) < names.index("Install site configuration")
    install = tasks["Install site configuration"]
    assert install["no_log"] is True
    assert (
        install["ansible.builtin.copy"]["content"]
        == "{{ piddi_rendered_config.stdout }}"
    )
    guard = playbook["pre_tasks"][1]["ansible.builtin.assert"]
    assert "piddi_kafka is not defined" in guard["that"]
    assert "piddi_config_extra is not defined" in guard["that"]


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
    assert "ANSIBLE_ARGS ?=\n" in makefile
    assert "ANSIBLE_ARGS ?= --ask-become-pass" not in makefile
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


def test_deployment_readme_links_to_the_maintained_guides():
    readme = (PROJECT_ROOT / "deploy" / "README.md").read_text()
    assert "../docs/deployment.md" in readme
    assert "../docs/deployment-reference.md" in readme


def test_deployment_guides_document_root_checkout_environment_and_monitoring():
    guide = (PROJECT_ROOT / "docs" / "deployment.md").read_text()
    reference = (PROJECT_ROOT / "docs" / "deployment-reference.md").read_text()

    assert "sudo -i" in guide
    assert (
        "git clone https://github.com/ESGF/piddiplatsch.git /opt/piddiplatsch" in guide
    )
    assert "make deploy" in guide
    assert "For settings changes only" in guide
    assert "run `make play`" in guide
    assert "You do not need `conda init`" in reference
    assert (
        "systemctl status piddi.service piddi-sync.timer piddi-sync.service"
        in reference
    )
    assert "cd /var/lib/piddi" in guide
    assert "piddi top" in guide
    assert "tail -f /var/log/piddi/piddi.log" in guide


def test_makefile_provides_repeatable_conda_environment_setup():
    makefile = (PROJECT_ROOT / "Makefile").read_text()

    assert "conda: ## create or update the project Conda environment" in makefile
    assert 'test -d "$(CONDA_ENV_PREFIX)/conda-meta"' in makefile
    assert "conda env create --prefix" in makefile
    assert "conda env update --prefix" in makefile
    assert "--prune" in makefile
    assert "conda run --prefix" in makefile
    assert "python -m pip install -e ." in makefile
    assert "deploy: conda play ##" in makefile


@pytest.mark.parametrize(
    "content",
    [
        '[kafka]\n"security.protocol" = "SASL_SSL"\n"sasl.mechanisms" = "PLAIN"\n',
        '[kafka]\n"security.protocol" = "PLAINTEXT"\n[stats]\nheartbeat_interval_seconds = -1\n',
    ],
)
def test_deployment_rejects_invalid_candidate_without_output(tmp_path, capsys, content):
    render = runpy.run_path(str(PROJECT_ROOT / "deploy/ansible/render_config.py"))[
        "render_config"
    ]
    source = tmp_path / "custom.toml"
    source.write_text(content)
    with pytest.raises(ValueError, match="Invalid candidate configuration"):
        render(source)
    assert capsys.readouterr().out == ""
    assert source.read_text() == content


def test_batch_deployment_installs_oneshot_and_configurable_timer():
    playbook = yaml.safe_load((PROJECT_ROOT / "deploy/ansible/piddi.yml").read_text())[
        0
    ]
    assert (
        playbook["vars"]["piddi_enable_sync_timer"]
        == "{{ piddi_enable_batch_timer | default(false) }}"
    )
    assert (
        playbook["vars"]["piddi_sync_check_interval"]
        == "{{ piddi_batch_check_interval | default('1min') }}"
    )
    tasks = {task["name"]: task for task in playbook["tasks"]}
    assert tasks["Install scheduled sync units"]["loop"] == [
        "piddi-sync.service",
        "piddi-sync.timer",
    ]
    timer = (PROJECT_ROOT / "deploy/ansible/templates/piddi-sync.timer.j2").read_text()
    assert "OnUnitInactiveSec={{ piddi_sync_check_interval }}" in timer
    assert "Unit=piddi-sync.service" in timer
    unit = (PROJECT_ROOT / "deploy/ansible/templates/piddi-sync.service.j2").read_text()
    assert "Type=oneshot" in unit
    assert "--silent sync" in unit
    assert "Restart=" not in unit


def test_maintenance_timer_is_deployed_with_the_harvest_service():
    playbook = yaml.safe_load((PROJECT_ROOT / "deploy/ansible/piddi.yml").read_text())[
        0
    ]
    assert (
        playbook["vars"]["piddi_enable_maintenance_timer"]
        == "{{ piddi_enable_service }}"
    )
    tasks = {task["name"]: task for task in playbook["tasks"]}
    assert tasks["Install maintenance units"]["loop"] == [
        "piddi-maintenance.service",
        "piddi-maintenance.timer",
    ]
    timer = (
        PROJECT_ROOT / "deploy/ansible/templates/piddi-maintenance.timer.j2"
    ).read_text()
    service = (
        PROJECT_ROOT / "deploy/ansible/templates/piddi-maintenance.service.j2"
    ).read_text()
    assert "02:00:00 UTC" in timer
    assert "Persistent=true" in timer
    assert "Unit=piddi-maintenance.service" in timer
    assert "--silent maintain" in service
    assert "User={{ piddi_user }}" in service


def test_sync_deployment_removes_old_units_before_starting_new_timer():
    playbook = yaml.safe_load((PROJECT_ROOT / "deploy/ansible/piddi.yml").read_text())[
        0
    ]
    names = [task["name"] for task in playbook["tasks"]]
    tasks = {task["name"]: task for task in playbook["tasks"]}
    assert names.index("Stop and disable legacy batch units") < names.index(
        "Remove legacy batch unit files"
    )
    assert names.index("Remove legacy batch unit files") < names.index(
        "Configure the scheduled sync timer"
    )
    assert tasks["Inspect legacy batch units"]["loop"] == [
        "piddi-batch.timer",
        "piddi-batch.service",
    ]
    stop = tasks["Stop and disable legacy batch units"]
    assert stop["ansible.builtin.systemd_service"]["enabled"] is False
    assert stop["ansible.builtin.systemd_service"]["state"] == "stopped"
    assert stop["when"] == "item.stat.exists | default(false)"
