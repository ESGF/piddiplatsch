# Operations

## Runtime output

Piddiplatsch writes daily JSONL files beneath `consumer.output_dir`:

| Directory | Contents |
| --- | --- |
| `dump/` | Original Kafka messages written by every `harvest` or `consume` run. |
| `<plugin>/handles/` | Handle records produced by that plugin. Direct REST/pyhandle publication writes the JSONL record before contacting the server; each line also includes `project`. |
| `published/` | One run-scoped JSONL receipt per `publish` run, containing every successful or failed Handle outcome. |
| `<plugin>/skipped/` | Records deferred after transient external failures. |
| `<plugin>/failures/r<N>/` | Records that failed processing, grouped by retry count. |
| `skipped/`, `failures/r<N>/` | Legacy or unresolved-project recovery records. |

The raw dump is intentionally global and always written before routing. It preserves
the consumed Kafka order and remains suitable for replay or investigation;
creating filtered per-plugin dumps would lose that simple ordering guarantee.

JSONL Handle output is always enabled, including direct publication. If the
audit record cannot be appended, that Handle is not sent to the server. A
server-side failure leaves the JSONL record available for inspection or later
publication.

Deferred publication writes each outcome immediately to a readable, unique
`published/published_<project>_handles_YYYY-MM-DD_HH-MM-SS.jsonl` file using UTC
and prints its path in the final CLI summary. Generic mixed-project batches use
`published_handles_...jsonl`. If another run starts during the same second,
`_2`, `_3`, and so on are appended. Parallel completion order may differ from
input order; `position`, `batch_index`, `source_file`, and `source_line` provide
stable ordering and provenance.

## Staged commands

```bash
# Kafka -> raw JSONL only
piddi harvest

# raw JSONL -> project-scoped Handle JSONL only
piddi map --project cmip6 --date 2026-08-27

# Handle JSONL -> REST Handle Service
piddi publish --project cmip6 --date 2026-08-27

# Kafka -> raw JSONL -> Handle JSONL (default production ingestion)
piddi consume

# All three stages in one process
piddi consume --publish
```

`map` accepts files or directories plus `--project`, `--all-projects`,
`--limit`, `--offset`, and `--force`. It never contacts Kafka or a Handle
Service and does not modify its input dumps.

The `--date` convenience accepts `YYYY-MM-DD`, `today`, `yesterday`, `today-N`,
or `last`. `map` uses `dump/dump_messages_<date>.jsonl`; `publish` requires a
project and uses `<project>/handles/handles_<date>.jsonl`. When neither an
explicit path nor `--date` is supplied, both commands default to `last`.
This selects the greatest valid date found in the relevant filenames,
regardless of file modification time. Explicit paths remain available for both
commands, and a path and `--date` are mutually exclusive.

`publish --project NAME` validates every selected Handle before publishing and
stops without sending anything if a project is missing or different. Plain
`publish` remains the generic mode and permits mixed-project batches. The final
summary always provides per-project counts when project metadata is available.

## Real Handle service contract test

The opt-in live tests create uniquely named Handles and do not delete them, so
use a disposable test prefix. Configure the service without storing credentials
in the repository:

```bash
export PIDDI_LIVE_HANDLE_SERVER_URL=https://handle-test.example/api-root
export PIDDI_LIVE_HANDLE_PREFIX=21.TEST
export PIDDI_LIVE_HANDLE_USERNAME='300:21.TEST/testuser'
export PIDDI_LIVE_HANDLE_PASSWORD='...'
pytest -m live tests/live/test_real_handle_service.py
```

Set `PIDDI_LIVE_HANDLE_VERIFY_HTTPS=false` only for a trusted test service with
a self-signed certificate. The live suite checks create, overwrite/update,
read-back, bounded parallel publication, and same-PID update ordering.

The Docker mock adds a 50 ms delay to each valid PUT, approximating a serial
rate of 20 Handle registrations per second. Override it when starting Docker if
you need a different latency:

```bash
PIDDI_MOCK_HANDLE_PUT_DELAY_SECONDS=0.1 docker compose up -d
```

The delay occurs outside the mock store lock, so concurrent publisher workers
can overlap requests as they would with a real service connection/database
pool. GET requests and requests rejected before storage are not delayed.

These files, `pid.log`, and `piddi.db` are ignored by Git and preserved by the
project's cleanup targets. There is no automatic retention policy. Dump and
Handle JSONL files can grow quickly, so monitor disk usage and archive or remove
old files according to the site's operational policy.

## Safe inspection and retry

Retry remaps failed events without contacting the Handle Service. Each command
creates and prints a distinct project-scoped
`retry_handles_<timestamp>.jsonl`, keeping late recovery work separate from
normal daily mapping output:

```bash
piddi retry outputs/cmip6/failures/r0
piddi publish --project cmip6 \
  outputs/cmip6/handles/retry_handles_<timestamp>.jsonl
```

Use `retry --publish` only for intentional immediate publication.
Recovery records store the canonical project in `__infos__.project`; `retry`
uses it instead of the current configured project selection. Older records
without this metadata still use the configured selection.

`--delete-after` removes an input file only when all records succeed. Malformed
JSONL and skipped records are failures for this decision, so the source remains
available for inspection.

## Logging and statistics

The CLI writes WARNING and above to `pid.log` by default. Use `-v` for INFO,
`-vv` or its memorable `--debug` alias for DEBUG, and `--log PATH` to override
the configured file. `--silent` remains an alias for hiding progress;
`--progress/--no-progress` provides the explicit form. At INFO level Piddiplatsch
records the selected plugins, the first occurrence of every filtered project
identity, publication outcomes, and periodic aggregate counts. Per-message
filter decisions are available at DEBUG level.

The `[logging]` configuration provides the service defaults:

```toml
[logging]
level = "WARNING"
file = "/var/log/piddi/piddi.log"
```

File logging uses a watched handler: after logrotate renames the active file and
creates a replacement, Piddiplatsch switches to the new file on its next log
write. Full recovery and skipped details remain available in JSONL independently
of the selected logging level.
The optional SQLite reporter is controlled by `[stats]` and is opened only by
the explicit `piddi map` command. `consume`, `harvest`, and `top` never update
the database.

The database contains a versioned current-run status model. A heartbeat is
updated independently of Kafka traffic, and processing outcomes are split by
canonical project. Inspect it without contacting Kafka or the Handle service:

```console
# live view (Ctrl-C exits)
piddi top

# one project, one terminal snapshot
piddi top --project cmip7 --once

# machine-readable status
piddi top --json

# use a six-hour history window
piddi top --history 360
```

`top` marks unfinished runs stale when their heartbeat exceeds
`stats.stale_after_seconds`. An idle topic is healthy while the process keeps
heartbeating. The heartbeat interval is configured with
`stats.heartbeat_interval_seconds`. Both default to 5 and 15 seconds,
respectively. `top` is read-only and reports a clear error for a missing or
invalid monitoring database.

The database appends cumulative per-project samples every
`stats.sample_interval_seconds` (15 seconds by default), including samples at
run startup and shutdown. The history table shows counter changes, average
message throughput, and a compact throughput trend for the last
`stats.history_minutes` (60 minutes by default). `--history MINUTES` overrides
that window. Samples older than `stats.sample_retention_days` (30 days by
default) are removed; set it to `0` to retain them indefinitely. Samples contain
counters only; raw messages and log lines are never stored.

## Shutdown behavior

SIGINT and keyboard interruption close the Kafka consumer, progress display,
and statistics reporters. Processing stops with a non-zero status after the
configured error limit or a fail-fast transient external failure.

## Local cleanup

`make clean` removes build, bytecode, and test artifacts only. `make clean-dist`
also removes other ignored development artifacts, but explicitly preserves
runtime output, logs, databases, local configuration, virtual environments, and
editor settings.

## Lightweight production deployment

The supported deployment model is one Piddiplatsch instance per VM. Every VM
uses the conventional paths `/etc/piddi/piddi.toml`, `/var/lib/piddi`, and
`/var/log/piddi/piddi.log`, with a dedicated non-login `piddi` user. A small
Ansible playbook installs the virtual environment, application and optional
plugins, configuration, systemd service, and logrotate policy. It uses only
modules included with `ansible-core`; no roles or external collections are
required.

Install `ansible-core` directly on the VM, then copy and edit only the custom
variables. The playbook targets `localhost` using Ansible's local connection.
It automatically loads `custom.yml` when present, overriding its global
defaults; otherwise it loads the committed empty `null.yml` fallback.
`custom.yml` contains the pinned Piddiplatsch reference and site configuration:

```console
python -m pip install ansible-core
cp deploy/ansible/custom.yml.example deploy/ansible/custom.yml
editor deploy/ansible/custom.yml
make play
```

The target must provide Python virtual-environment support. When installing
from a Git reference, add the distribution's Git package to
`piddi_os_packages`; Debian and Ubuntu commonly require `python3-venv` as well.
Installing a pinned package release avoids the target-side Git dependency.

The example keeps `piddi_enable_service: false`. The first run therefore
installs and validates everything without starting the consumer. Configuration
content is hidden from Ansible output, and `custom.yml` is ignored by Git
because it may contain credentials. For shared variables, use an encrypted
Ansible Vault file.

`make play` uses `--ask-become-pass` by default. Additional Ansible arguments
can be supplied when needed:

```console
make play ANSIBLE_ARGS="--ask-become-pass --ask-vault-pass"
```

Try the exact production command in the foreground on the VM:

```console
sudo runuser -u piddi -- /opt/piddi/venv/bin/piddi \
  --config /etc/piddi/piddi.toml --silent consume
```

Stop the trial with Ctrl-C. Then enable the service by changing
`piddi_enable_service` to `true` in `custom.yml` and applying the same target:

```console
make play
systemctl status piddi
sudo tail -f /var/log/piddi/piddi.log
```

The rotation policy checks hourly, rotates daily or after the log exceeds 100
MiB, keeps 14 archives, and compresses older files. The application uses
WARNING logging unless `logging.level` is changed. For temporary diagnosis,
stop the service and run the foreground command with `-v` or `--debug`.

For an external SSD, mount it directly at `/var/lib/piddi` rather than exposing
the hardware-specific mount path in application configuration. The systemd unit
uses `RequiresMountsFor=/var/lib/piddi`, so a configured mount must be available
before the consumer starts. Ensure it is declared in `/etc/fstab` before
enabling the service.

If several independent Piddiplatsch installations later need to share one VM,
prefer one Podman or Docker container per workflow, with distinct configuration,
data, log, and credential mounts. That provides a clearer isolation boundary
than multiplying system users, virtual environments, and systemd templates on
the host. The initial single-VM service does not require containers.
