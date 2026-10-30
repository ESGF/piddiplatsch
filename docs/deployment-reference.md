# Deployment reference

Start with the [Deployment guide](deployment.md). This page covers optional
checks and less common setup tasks. Commands assume a root shell opened with
`sudo -i`.

## Check before publication

Start harvest using the deployment guide and let it fill the catalogs. Before
enabling sync, prepare Handle files without sending them:

```console
cd /var/lib/piddi
runuser -u piddi -- /opt/piddiplatsch/.conda/bin/piddi \
  --config /etc/piddi/piddi.toml map --all-projects --date all
```

Inspect `/var/lib/piddi/<project>/handles/`. Check the server profile, prefixes,
dataset PIDs, and file links. Version linking is disabled by default for the
first fill. See [Operations](operations.md) for date selection and version links.

Manual mapping does not clear the sync queue. When you enable sync, it maps
pending catalog items into separate batches and publishes them.

## Paths and permissions

| Default path | Contents |
| --- | --- |
| `/opt/piddiplatsch/.conda` | Application environment. |
| `/etc/piddi/piddi.toml` | Installed configuration. |
| `/var/lib/piddi/catalog` | Catalogs and `.harvest-state.json`. |
| `/var/lib/piddi/batches` | Ready and published sync batches. |
| `/var/lib/piddi/piddi.db` | Monitoring database. |
| `/var/lib/piddi/published` | Publication receipts. |
| `/var/log/piddi/piddi.log` | Application log. |

Ansible adds production data, log, and database paths only where `custom.toml`
does not set them. Explicit values take priority. For other paths, create the
directories, grant the `piddi` user access, and adjust log rotation.
Relative service paths start at `/var/lib/piddi`.

The systemd services wait for the `/var/lib/piddi` mount. If you use a separate
data disk, mount it there before starting Piddi.

Set `piddi_conda_env` in `deploy/ansible/custom.yml` if the application environment
is elsewhere. Set `piddi_config_source` to deploy a TOML file other than the
checkout's `custom.toml`.

## Service controls

`make start`, `make stop`, and `make restart` use `sudo systemctl` by default.

`make start` starts harvest, sync and log rotation timers, and an immediate sync
run. It starts sync even if the timer is disabled at boot. Ansible controls which
units start after reboot. Log rotation runs hourly when harvest is enabled.

Set `piddi_sync_check_interval` in Ansible YAML to change the timer interval.
The default is `1min`, measured after the previous run finishes. Batch thresholds
are separate [TOML settings](configuration.md#scheduled-batches).

Ansible validates application settings before installing them. It restarts
harvest when its configuration or unit changes. A sync run already in progress
keeps its loaded settings until it ends.

Update the host OS and base Conda installation separately. `make deploy` updates
the application environment. `make play` applies Ansible settings without
installing packages. Use `make audit` in a development environment to check
Python dependencies.

## Service troubleshooting

Use these checks if `piddi.log` or `piddi top` shows a problem:

```console
systemctl status piddi.service piddi-sync.timer piddi-sync.service
systemctl list-timers 'piddi-*'
journalctl -u piddi.service -u piddi-sync.service --since today
journalctl -u piddi-sync.service -f
df -h /var/lib/piddi
du -sh /var/lib/piddi/*
```

Harvest should stay active, and the sync timer should show its next run. The
sync service exits between runs, so an inactive `piddi-sync.service` can be normal.

Use `piddi top --once` for one snapshot or `piddi top --json` for JSON output.
Publication receipts are saved under `/var/lib/piddi/published/YYYY/MM/DD/`,
grouped by UTC date. Existing flat receipt files remain in place.

Failed batches stay in `batches/<project>/ready/` and block newer work for that
project. Other projects can continue. See [Scheduled batch recovery](recovery.md#scheduled-batch-recovery).

## Backups

Stop Piddi before taking a file-based backup. Save the configuration, catalog
directory (including `.harvest-state.json`), and batch directory together.
Keep daily Handle files and receipts according to your site's audit policy.
Output files and batches have no automatic retention policy.

Do not delete catalogs to fix a stopped harvest. Clean options replay retained
Kafka history and can invalidate ready batches. Read [Recovery](recovery.md)
before rebuilding.

## Older deployments

Move old application settings from Ansible YAML to `custom.toml`:

| Old YAML variable | TOML setting |
| --- | --- |
| `piddi_kafka`, `piddi_kafka_defaults` | `[kafka]` |
| `piddi_config_extra` | Matching TOML tables. |
| `piddi_projects`, `piddi_topic`, `piddi_max_errors` | `[consumer]` `projects`, `topic`, `max_errors`. |
| `piddi_output_dir` | `[consumer]` `output_dir`. |
| `piddi_log_level`, `piddi_log_file` | `[logging]` `level`, `file`. |
| `piddi_stats_enable_db`, `piddi_db_path` | `[stats]` `enable_db`, `db_path`. |

The playbook rejects these old YAML variables until they are moved.

Ansible replaces old `piddi-batch` units with `piddi-sync` units. The old controls
`piddi_enable_batch_timer` and `piddi_batch_check_interval` still work as fallbacks.
Use the new `sync` names for new settings. The TOML section remains `[batch]`.
Existing batch files remain usable.

## Vagrant test VM

Use Parallels on Apple Silicon, VirtualBox on Intel macOS, or libvirt on Linux.
Install the matching provider, then start a VM. For example, on Apple Silicon:

```console
vagrant plugin install vagrant-parallels
vagrant up --provider=parallels
vagrant ssh
```

Use `--provider=virtualbox` or `--provider=libvirt` on other hosts. Libvirt needs
the `vagrant-libvirt` plugin. The VM installs Miniforge under `/opt/conda`.
Run `vagrant provision` to update an existing VM's provisioning.

Inside the VM, follow the [Deployment guide](deployment.md) as root. If Conda
is not available, load it first:

```console
source /opt/conda/etc/profile.d/conda.sh
```

You do not need `conda init`; deployment uses the environment's full path.
Run `vagrant destroy` on the host when the test VM is no longer needed.
