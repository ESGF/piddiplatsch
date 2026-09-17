# Deployment

Install Conda on the host first; Miniforge is recommended. This remains a
manual host-administration step in production.

Check out Piddi manually, normally below `/opt/piddiplatsch`, then create its
configuration and run the complete deployment:

```console
cd /opt/piddiplatsch
python -m pip install ansible-core
cp etc/esgf-example.toml custom.toml
vim custom.toml
make deploy
```

`make deploy` creates or updates `.conda`, installs Piddi, and runs Ansible.
Use `make play` for later Ansible-only configuration changes.

`custom.toml` is the single application configuration for manual runs and
Ansible deployment. It contains Kafka credentials, projects, Handle profiles,
and mapping settings. Ansible reads that file and installs a generated copy at
`/etc/piddi/piddi.toml`; direct edits to the installed copy are overwritten.
For an existing checkout, keep your `custom.toml` rather than copying the example
again. Use `make play` after application configuration changes.

Ansible adds only deployment path defaults when those keys are omitted:
`consumer.output_dir = "/var/lib/piddi"`,
`logging.file = "/var/log/piddi/piddi.log"`, and
`stats.db_path = "/var/lib/piddi/piddi.db"`. Explicit TOML values take precedence.
Relative paths in the service resolve under `/var/lib/piddi`; manual runs use
their current directory. For custom absolute paths, provision their directories
and permissions, and adapt log rotation if the log path changes.

The optional `deploy/ansible/custom.yml` now contains deployment controls only,
such as `piddi_enable_service`, `piddi_conda_env`, or `piddi_config_source`
(to choose a different TOML source). Copy `custom.yml.example` only if you need
these overrides. There are no Kafka or Handle values to duplicate in YAML.

### Migrating an existing Ansible configuration

Move application values from `deploy/ansible/custom.yml` into `custom.toml`,
then remove those YAML variables. The playbook rejects the old application
variables with a migration message rather than silently ignoring them.

| Old Ansible variable | Shared TOML setting |
| --- | --- |
| `piddi_kafka`, `piddi_kafka_defaults` | Keys under `[kafka]` (site overrides win over defaults) |
| `piddi_config_extra` | Its TOML tables, merged into the file without duplicate table headers |
| `piddi_projects`, `piddi_topic`, `piddi_max_errors` | `[consumer]` `projects`, `topic`, `max_errors` |
| `piddi_output_dir` | `[consumer]` `output_dir` |
| `piddi_log_level`, `piddi_log_file` | `[logging]` `level`, `file` |
| `piddi_stats_enable_db`, `piddi_db_path` | `[stats]` `enable_db`, `db_path` |

The ESGF example provides the four projects on `ESGF-PUBLICATIONS` and
`max_parts = 0`. If your existing TOML selects other values, deployment uses
those values too. Resolve any differences between the old YAML and TOML once
before deploying. Keep credentials in the ignored local file.

Run this as root or with passwordless sudo. If Ansible needs a sudo password,
use `make play ANSIBLE_ARGS=--ask-become-pass`.

Ansible creates the service user and directories, renders
`/etc/piddi/piddi.toml`, and validates it, but does not start Piddi. Test it:

```console
sudo runuser -u piddi -- /opt/piddiplatsch/.conda/bin/piddi \
  --config /etc/piddi/piddi.toml --silent consume
```

Then set `piddi_enable_service: true` in `deploy/ansible/custom.yml` (create
it from the optional example if needed) and run `make play` again.
Check that Piddi is working:

```console
systemctl status piddi
piddi top
tail -f /var/log/piddi/piddi.log
```

## Vagrant test VM

Use one of these host/provider combinations:

- Apple Silicon macOS: Parallels
- Intel macOS: VirtualBox
- x86_64 Linux: libvirt, with VirtualBox as a fallback

Install the matching Vagrant provider when needed, then start the VM:

```console
# Apple Silicon
vagrant plugin install vagrant-parallels
vagrant up --provider=parallels

# Intel macOS
vagrant up --provider=virtualbox

# Linux
vagrant plugin install vagrant-libvirt
vagrant up --provider=libvirt

vagrant ssh
```

The VM provisioning installs Miniforge under `/opt/conda`. For an already
running VM after a Vagrantfile change, run `vagrant provision` once.

Inside the VM, perform the complete deployment as root:

```console
sudo -i
git clone https://github.com/ESGF/piddiplatsch.git /opt/piddiplatsch
cd /opt/piddiplatsch
cp etc/esgf-example.toml custom.toml
vim custom.toml
make deploy
```

`conda init` is not needed because the Make target addresses the environment
by its prefix. For optional interactive activation in the current root shell, run
`source /opt/conda/etc/profile.d/conda.sh` followed by
`conda activate /opt/piddiplatsch/.conda`.

Remove the VM later with `vagrant destroy` on the Mac.
