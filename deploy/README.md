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

### Start, stop, and restart

After deployment, manage all Piddi processing and log rotation together:

```console
make start
make stop
make restart
```

`make start` starts harvest, the sync timer, and the log rotation timer, then
requests an immediate sync tick without waiting for it to finish. This includes
real Handle publication using the configured profile. `make stop` stops both
timers first, then stops sync, harvest, and any running log rotation.
`make restart` completes the stop sequence before starting them again, including
when Make runs with parallel jobs. Interrupted sync batches are retried on the
next run, so some successful Handle writes may be repeated.

These targets manage runtime state; Ansible controls which units are enabled at
boot. Commands use `sudo systemctl` by default; as root without sudo, use
`make restart SYSTEMCTL=systemctl` (likewise for start/stop).

### Dependency security updates

After pulling dependency changes, run `make deploy` to update the application
environment; `make play` alone does not install packages. The environment requires
the October 2026 CA trust updates, pip 26.2.2, Brotli 1.2.1, Requests 2.34.2, and
urllib3 2.8.0 or newer. Restart the running harvest service after updating so it
loads the new libraries:

```console
make deploy
sudo systemctl restart piddi
```

Update the host's OS CA store and its base Conda/Miniforge installation separately
using their security updates, before resolving the application environment.
Updating Certifi alone does not update pip's vendored CA bundle or Conda's native
trust store. See the [Conda security guidance](https://conda.org/blog/2026-10-21-emudhra-security-updates/).

Use `make audit` in a development environment with `.[dev]` installed to audit
installed Python packages; CI runs it too. For existing shared environments,
also upgrade any installed `brotlicffi` to at least 1.2.1.0, or remove it if
unused. HTTP clients may select it ahead of `brotli`. A Python dependency audit
does not cover OS packages or native Conda libraries. Rebuild mock Handle Docker
images with `docker compose build --pull` to refresh their base image and packages.

### Application configuration

`custom.toml` is the single application configuration for manual runs and
Ansible deployment. It contains Kafka credentials, projects, Handle profiles,
and catalog/mapping settings. Ansible reads that file and installs a generated copy at
`/etc/piddi/piddi.toml`; direct edits to the installed copy are overwritten.
For an existing checkout, keep your `custom.toml` rather than copying the example
again. Use `make play` after application configuration changes.

Ansible adds only deployment path defaults when those keys are omitted:
`consumer.output_dir = "/var/lib/piddi"`,
`logging.file = "/var/log/piddi/piddi.log"`, and
`stats.db_path = "/var/lib/piddi/piddi.db"`. Explicit TOML values take precedence.
The catalog defaults to SQLite under `<consumer.output_dir>/catalog`
(`/var/lib/piddi/catalog` in deployment), with `catalog` as the version lookup
backend. Ansible creates the default catalog directory with service-user ownership.
No separate catalog or lookup settings are duplicated in YAML.
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

Ansible creates the service user and directories, renders a candidate, and
validates it against packaged defaults before installing `/etc/piddi/piddi.toml`.
The currently installed file is not used to fill in missing candidate settings.
An invalid candidate stops deployment before replacing the existing configuration
or restarting the service. Candidate validation does not open log files or
contact Kafka/STAC/Handle services. Piddi is not started by default. Test it:

```console
sudo runuser -u piddi -- /opt/piddiplatsch/.conda/bin/piddi \
  --config /etc/piddi/piddi.toml --silent harvest
```

The foreground command fills/updates per-project catalogs and exits after five
idle seconds. Test mapping separately, using available catalogs for initial filling:

```console
sudo runuser -u piddi -- /opt/piddiplatsch/.conda/bin/piddi \
  --config /etc/piddi/piddi.toml map --all-projects --date all
```

Inspect the project Handle JSONL before intentionally publishing a chosen day:

```console
sudo runuser -u piddi -- /opt/piddiplatsch/.conda/bin/piddi \
  --config /etc/piddi/piddi.toml publish --project cmip6 --date today-1
```

Both batch commands default to `--date last`, support `today-N`, explicit dates,
`all`, inclusive `--from DATE --to DATE` ranges, and `--offset`/`--limit`. Use
matching dates in map and publish; offsets count catalog items in map and Handle
records in publish. Repeating map appends to daily files. Keep predecessor linking
disabled for initial filling (`catalog.version_history = false`); enable it or use
`map --version-history` when ready to remap with predecessor links.

Then set `piddi_enable_service: true` in `deploy/ansible/custom.yml` (create
it from the optional example if needed) and run `make play` again.
The installed `piddi.service` now runs `harvest --idle-timeout 0` continuously.
It maintains catalogs and raw dumps, and does not prepare or publish Handles.
Map and publish remain available as explicit commands. Scheduled execution uses
`piddi-sync.service` (oneshot) and `piddi-sync.timer`; the timer is installed but
disabled until explicitly enabled.
The old `consume` command is removed. Existing deployments switching to harvest
use its separate Kafka group and may replay retained history.
Keep the catalog databases and `.harvest-state.json` together for backup/recovery.
Do not use clean options automatically in the service.

Missing PATCH bases fetch current remote items using the shared/project STAC
endpoint. The fetched item is cached without applying the queued patch; marked
items remain in refresh mode on subsequent events and harvest startups. Remote
lag can temporarily omit updates, so repeat harvest after the service catches up
before mapping recovered items. Mapping stays local; failed fetches stop harvest
without acknowledging the event. The unit retains `Restart=on-failure`.

Check that Piddi is working:

```console
systemctl status piddi
# Harvest and mapping status from the monitoring database
piddi --config /etc/piddi/piddi.toml top
tail -f /var/log/piddi/piddi.log
```

## Scheduled map and publish

After validating your Handle profile and outputs, enable the sync timer in
`deploy/ansible/custom.yml` and run `make play`:

```yaml
piddi_enable_service: true
piddi_enable_sync_timer: true
piddi_sync_check_interval: 1min
```

The command and units were renamed from `batch` to `sync`. On deployment,
Ansible stops/disables existing `piddi-batch.timer` and `piddi-batch.service`,
removes their unit files, and installs `piddi-sync` units. Existing YAML controls
`piddi_enable_batch_timer` and `piddi_batch_check_interval` remain accepted as
fallbacks; prefer the new `sync` names above. Explicit new controls take precedence.
The TOML `[batch]` settings, spool paths, and existing ready/published artifacts
are unchanged and remain usable.

Enabling the sync timer enables **real Handle publication**. Harvest remains the
only continuous process. The timer runs a single `piddi sync` tick one minute after
boot and, by default, one minute after the previous tick completes. A filesystem
lock also prevents concurrent manual/scheduled workers sharing the batch directory.
A tick publishes ready batches regardless of mapping thresholds, then maps due
projects and immediately publishes each newly ready batch. Large eligible backlogs
are drained in bounded batches, taking turns across projects. Each project captures
an upper catalog sequence per tick; later changes remain for the next run.

The shared TOML settings are:

```toml
[batch]
trigger_items = 1000
max_wait_seconds = 3600
max_items = 1000
publish_workers = 4
publish_retries = 3
# directory = "/var/lib/piddi/batches"
```

Thresholds apply per project: 1,000 distinct pending items **or** one hour since the
oldest pending change, whichever comes first. The clock starts with the first
pending change, not the last batch. Repeated updates to one item count once and
retain its first pending time. Eligibility is checked on each timer tick; backlog
or failures may delay execution. Empty queues create no batch. Each batch holds at
most 1,000 catalog items, including all their non-excluded file Handles; Handle
counts and bytes can be larger. Batch mapping requires every included asset to
validate, so incomplete mapping output is never marked ready.

Batches use `batches/<project>/ready/<batch-id>/handles.jsonl` and `manifest.json`.
Payloads are sealed with a checksum and cannot be appended by later map runs.
Mapping checkpoints advance only after durable sealing; interrupted acknowledgement
is recovered from the manifest. Publication failures keep the batch ready and block
newer mapping/publication for that project, while other projects can proceed.
A later tick republishes the same immutable batch with overwrite semantics. Success
moves it to `batches/<project>/published/<batch-id>/`; receipts retain the original
ready input path and the batch ID identifies its archived location. There is no
automatic retention or deletion of batches, receipts, or failed staging directories.

Catalog schema version 5 upgrades during harvest; existing items become pending
once, even if previously mapped manually. Manual date-based map/publish commands
remain independent and do not advance the worker's mapping checkpoint. A catalog
clean starts a new generation; old ready batches then block that project for
inspection instead of publishing stale data. Stop the worker before cleaning and
finish or deliberately archive the old ready queue first. Keep catalogs, harvest
control state, and the batch spool together when backing up.

Useful commands:

```console
# Executes one tick, including publication of ready batches
piddi --config /etc/piddi/piddi.toml sync
# Ignore map thresholds for pending items; still publishes completed batches
piddi --config /etc/piddi/piddi.toml sync --force-map
# Restrict a manual run to a configured project and override publication concurrency
piddi --config /etc/piddi/piddi.toml sync --project cmip6 --workers 4 --retries 3
systemctl status piddi piddi-sync.timer
journalctl -u piddi-sync.service
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
