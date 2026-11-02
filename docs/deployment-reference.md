# Deployment reference

Start with [Ansible deployment](deployment.md). These notes are for the host
administrator; daily monitoring is covered by [Admin](admin.md).

## Check before publication

With harvest running and the sync timer still disabled, map a small sample:

```console
cd /var/lib/piddi
runuser -u piddi -- /opt/piddiplatsch/.conda/bin/piddi map --all-projects --limit 100
```

Inspect `/var/lib/piddi/batches/<project>/ready/<batch-id>/handles.jsonl` and
`failures.jsonl`. Check prefixes, dataset PIDs, and file links. Mapping does not
publish. Enabling sync publishes these saved batches as well as later work.

## Paths

| Default path | Contents |
| --- | --- |
| `/opt/piddiplatsch/.conda` | Application environment. |
| `/etc/piddi/piddi.toml` | Installed configuration. |
| `/var/lib/piddi` | Shared database, harvest control file, dumps, batches, and receipts. |
| `/var/log/piddi/piddi.log` | Progress log. |
| `/var/log/piddi/piddi.error.log` | Errors and tracebacks. |

Ansible adds these paths where `custom.toml` has no explicit override. For custom
paths, ensure the `piddi` user has access and adjust log rotation. Services wait
for the `/var/lib/piddi` mount; mount a separate data disk there before starting.

Set `piddi_conda_env` or `piddi_config_source` in Ansible's `custom.yml` to use a
different environment or TOML source. Application settings belong in TOML;
`custom.yml` contains only deployment controls. Older `piddi_kafka`,
`piddi_projects`, `piddi_output_dir`, and similar YAML settings must move to their
matching TOML sections. Use `piddi_enable_sync_timer` and
`piddi_sync_check_interval` for the sync timer.

## Service checks

```console
systemctl status piddi.service piddi-sync.timer piddi-sync.service
systemctl list-timers 'piddi-*'
```

The sync and maintenance services normally become inactive between timer runs.
See [Admin](admin.md) for logs and status interpretation.

## Backups

Stop Piddi with `make stop` before taking a file-based backup. Save configuration
and the complete data directory, including `piddi.db`, any SQLite WAL files,
`.harvest-state.json`, and dumps. Keep batch exports and receipts according to
your site's audit policy. The database contains queued payloads and publication
progress; raw dumps alone do not preserve publication progress.

Schema 4 databases upgrade automatically to schema 5. Preserve legacy ready
batches and their progress journals until imported. Older incompatible formats
need a separate recovery plan. Do not run older code against an upgraded database.

## Vagrant test VM

Use Parallels on Apple Silicon, VirtualBox on Intel macOS, or libvirt on Linux.
For example:

```console
vagrant plugin install vagrant-parallels
vagrant up --provider=parallels
vagrant ssh
```

Inside the VM, follow [Deployment](deployment.md). Miniforge is installed under
`/opt/conda`; load `/opt/conda/etc/profile.d/conda.sh` if needed.
You do not need `conda init`; deployment uses the environment's full path.
