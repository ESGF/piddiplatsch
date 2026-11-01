# Configuration

Start with the example file. Change only the values your site needs:

```bash
cp etc/esgf-example.toml custom.toml
```

Set the Kafka connection and credentials, projects, and Handle service profiles.
Keep real credentials in this ignored local file.

## Configuration files

Piddi loads files in this order. Later values replace earlier values:

1. Packaged `src/piddiplatsch/config/default.toml`.
2. Site file `/etc/piddi/piddi.toml`, if present.
3. Local `custom.toml`, if present.

`--config PATH` replaces the third file. That path must exist. The packaged and
site settings still load first. Relative paths start at the current working
directory. Deployed services use `/var/lib/piddi` as their working directory.

Ansible reads the checkout's `custom.toml` and installs a copy at
`/etc/piddi/piddi.toml`. It adds production paths where you have not set them.
See [Deployment](deployment.md).

## Check your settings

```bash
piddi config validate
piddi config explain --project cmip6
piddi config show --section consumer
```

`validate` checks the file structure, project names, profiles, required
credentials, and numeric limits. It suggests fixes for likely key typos.
It runs offline: it does not test connections or verify credentials.

`explain` shows the selected Handle service, prefix, STAC settings, and catalog
paths. It also shows which file supplied each value. Credentials are omitted.

`show` prints the merged configuration. It hides recognized credentials with
`***`, including credentials in URLs. Use descriptive names for custom secret
keys so they can be recognized. To include secrets explicitly:

```bash
piddi config show --show-secrets
piddi config show --format json --show-secrets
```

These inspection commands do not open the configured log file. They can run
when the production log directory is not writable.

## Kafka and projects

```toml
[consumer]
projects = ["cmip6", "cmip7"]
topic = "ESGF-PUBLICATIONS"

[kafka]
"bootstrap.servers" = "broker.example.org:9092"
"group.id" = "my-site-piddi"
"sasl.username" = "replace-me"
"sasl.password" = "replace-me"
```

Kafka defaults to `SASL_SSL` with `PLAIN` authentication. The ESGF example also
shows other authentication options and a custom CA file. Quote Kafka keys that
contain dots. Piddi passes additional `[kafka]` keys to the Kafka client.

For an unauthenticated local test broker, set:

```toml
[kafka]
"security.protocol" = "PLAINTEXT"
```

The Docker test configuration already sets this value. Use it with
`piddi --config tests/config.toml harvest`.

`consumer.projects` selects projects for mapping and sync. Use a list of plugin
names or `"all"`. Supported plugins are `cmip6`, `cmip6plus`, `cmip7`, and
`cordex-cmip6`.

Map can override the selection with repeated `--project NAME` options or
`--all-projects`. Sync can use repeated `--project NAME` options to restrict the
configured selection. Harvest always stores all registered projects it encounters.

## Multiple Handle servers

Use named profiles to choose a server and credentials:

```toml
[handles]
default = "production"

[handles.profiles.production]
server_url = "https://handles.example.org"
prefix = "21.DEFAULT"
username = "replace-me"
password = "replace-me"

[plugins.cmip6]
handle_prefix = "21.CMIP6"

[plugins.cmip7]
handle_prefix = "21.CMIP7"
```

Profiles inherit common values from `[handles.defaults]`, including HTTPS
verification and the request timeout. The default timeout is 10 seconds.
Keep HTTPS verification enabled for production.

Projects use `[handles].default` unless they set `handle = "profile-name"` under
`[plugins.<name>]`. Map and publish also accept `--handle-profile NAME` for one run.

A project's `handle_prefix` overrides its profile's prefix. In the example,
CMIP6 and CMIP7 share the server and credentials but use different prefixes.
The credentials must allow writes to both prefixes. Profiles still need a
fallback prefix, either in the profile or in `[handles.defaults]`.

Prepared Handle files keep the prefix chosen during mapping. Changing the
publication profile does not rewrite those PIDs. A mismatched prefix is rejected.
`publish` and `sync` send prepared Handles through REST.

Use `piddi config explain --project cmip6` to check the resolved settings.
You can also add `--handle-profile NAME` to inspect an override.

## STAC recovery settings

```toml
[stac]
base_url = "https://discovery.east.esgf.io"
timeout = 10

[plugins.cmip6.stac]
collection = "CMIP6"

[plugins.cmip7.stac]
collection = "CMIP7"
```

Projects inherit the shared STAC settings. Set a project-specific `base_url` or
`timeout` when needed.

Harvest fetches a complete remote item when a patch has no local base. It uses
the collection and item identity from the event. Recovered items stay marked for
remote refresh on later events and harvest startup. A failed fetch stops harvest.
See [Remote current-item recovery](operations.md#remote-current-item-recovery).

Map reads local catalogs and does not contact STAC.

## Local catalog

```toml
[catalog]
enabled = true
backend = "sqlite"
ignore_unknown_projects = false # Enable to archive, count, and continue.
version_history = false
kafka_commit_batch_size = 100
kafka_commit_interval_seconds = 1.0
# directory = "/var/lib/piddi"
```

The shared database defaults to `<consumer.output_dir>/piddi.db`. Set
`catalog.directory` to change its directory. SQLite is the supported backend.
`enabled` controls catalog/work writes during harvest. Map always reads database
work snapshots. This format requires a fresh output directory; there is no legacy
schema migration.

`ignore_unknown_projects` defaults to `false`: an unknown project stops harvest
without advancing its checkpoint or acknowledging the event. Set it to `true` to
archive the event in `dump/unknown_projects_<day>_<part>.jsonl`, count it as
filtered under its project identifier, advance the shared checkpoint, and
continue. The dedicated archive must be durable before the checkpoint advances;
an archive failure still stops harvest. It uses the same day/count rotation and
closed-file compression as the ordinary all-event archive.

Override the boolean setting for one run with `harvest --ignore-unknown-projects`
or `harvest --stop-on-unknown-projects`. These flags do not change the saved
configuration and require catalog-backed harvesting.

These filtered counts appear in run statistics, logs, and per-project live
status (`top --json`), not the daily mapping/publication summary table. Unknown
events create no catalog or work rows. Missing/conflicting project identities
and malformed envelopes still stop harvest in both modes. Registered projects are
harvested even when disabled for automatic mapping/publication.

Keep `version_history` disabled for the first fill. Enable it to link each dataset
to the nearest older version in the same local catalog. Map's `--version-history`
and `--no-version-history` options override it for one run.

The two commit settings control how often harvest acknowledges saved events to
Kafka. Local writes remain durable for each event. A batch size of `1` sends an
acknowledgement for each message and can reduce throughput.

## Scheduled batches

```toml
[batch]
trigger_items = 1000
max_wait_seconds = 3600
max_items = 2000
publish_workers = 4
publish_retries = 3
retention_days = 0 # Disabled; set e.g. 30 to enable published-work cleanup.
# directory = "/var/lib/piddi/batches"
```

Sync maps a project when either 1,000 work entries are pending or the oldest
has waited one hour. Every event has an independent snapshot, including repeated
updates to the same item. `sync --force-map` skips these waiting thresholds.
Manual `map` processes pending work immediately.

`max_items` caps each project batch at 2,000 work entries by default. It does not
limit Handles or bytes; an entry's dataset and file Handles stay together. Sync
processes one batch per eligible project in round robin order. Project processing
is sequential; `publish_workers` only controls concurrent Handle requests within
the current batch. `publish_retries` controls temporary HTTP retries. Sync's
`--workers` and `--retries` override these values for one run.

`plugins.<project>.max_errors` limits mapping errors per batch. Mapping stores
invalid work and its explanation, rolls back partial Handle output for the failed
entry, and seals the successfully processed prefix. At the limit it pauses that
project until the next run. Unprocessed work remains pending. A later correction
is a new event; it does not overwrite or automatically retry invalid snapshots.

Publication failures leave work mapped and its saved batch ready. Older pending
work and failed older publication prevent newer output from overtaking it within
the same project. Other projects continue. Database acknowledgements and daily
summaries commit together, so recovery cannot double count them.

`retention_days` defaults to 0, disabling database work cleanup. All work rows
remain until you explicitly set a positive period, such as 30 days. Once enabled,
`piddi maintain` removes only work that entered published state before that
cutoff. Pending, mapped, invalid, catalog, and summary rows remain. Maintenance
never runs `VACUUM` or deletes JSONL output.

The batch directory defaults to `<consumer.output_dir>/batches`. Keep it paired
with its database. All batch commands use a shared lock beside `piddi.db`, even
when a custom batch directory is configured. The Ansible setting
`piddi_sync_check_interval` controls how often the timer runs.

## Logging and monitoring

```toml
[logging]
level = "INFO"
file = "pid.log"

[stats]
enable_db = true
interval_seconds = 30
summary_interval = 10000
heartbeat_interval_seconds = 5
stale_after_seconds = 15
sample_interval_seconds = 15
sample_retention_days = 30
```

INFO logs show progress summaries. Use `-vv` or `--debug` for record details.
`--log PATH` overrides the log file for one command. Set `logging.file = ""`
to log to the terminal. `--silent` hides progress bars but keeps final summaries.
File logging also writes ERROR and CRITICAL messages, including tracebacks, to
a companion file: `pid.error.log` beside `pid.log`, or `piddi.error.log` beside
`piddi.log`. The normal log still follows its configured level. The companion
always includes ERROR and CRITICAL, even if the normal level is CRITICAL.
Terminal-only logging does not create a companion file.

Statistics log every 30 seconds or 10,000 messages as results arrive. Daily mapped
work, prepared Handle, published Handle, and failed-attempt totals always live in `piddi.db.stats`.
`enable_db` enables live heartbeats and rate histories in the same `stats` table.
No extra tables or separate monitoring database are created.
`piddi top` reads live harvest, mapping, and publication status. Publication
results also appear in logs and JSONL receipts.

Heartbeats update every five seconds, even when Kafka is idle. Unfinished runs
become stale after 15 seconds without a heartbeat. Each live run retains up to one hour of rate samples; top shows the last five
minutes. `sample_retention_days` controls completed run-history retention; zero
disables that cleanup. See [Monitoring and maintenance](operations.md#monitoring-and-maintenance).

## Raw dump files

Harvest archives events under `<consumer.output_dir>/dump` before processing.
Files split at the UTC day boundary or after `consumer.dump_max_events` events
(default `100000`, must be positive):

```toml
[consumer]
dump_max_events = 100000
```

Parts are named `dump_messages_2026-11-01_000001.jsonl`, then `_000002.jsonl`,
and so on. Restarting resumes the last part and its event count. Each line contains
`source` (topic, partition, offset), `key`, and the original event under `payload`. The limit counts events, not bytes;
individual events can vary greatly in size. Maintenance compresses parts from
previous UTC days, leaving today's files available for appending.

## Mapping settings

| Setting | Purpose |
| --- | --- |
| `consumer.output_dir` | Root directory for catalogs, Handle files, receipts, and diagnostic files. |
| `plugins.<name>.max_errors` | Project mapping error limit. Default: `1`; `0` also stops on the first error; `-1` removes the limit. |
| `schema.strict_mode` | Reject incomplete or unsupported records. Default: `true`. |
| `plugins.<name>.landing_page_url` | Base URL for Handle landing pages. |
| `plugins.<name>.max_parts` | Limit dataset `HAS_PARTS` entries; `-1` means unlimited. |
| `plugins.<name>.excluded_asset_keys` | Asset keys excluded from `HAS_PARTS`. |
| `plugins.<name>.force_dataset_pid` | Generate dataset PIDs from the full versioned STAC item ID. Default: `false`. |

Use `force_dataset_pid` only for projects that need it. It ignores source dataset
PIDs and updates file parent links to the generated PID. File tracking IDs stay
the same. For every project, a different source dataset PID also produces a
minimal alias Handle whose `URL` redirects to
`https://hdl.handle.net/<prefix>/<generated-pid>`.
The alias uses the configured Handle prefix and contains no dataset or file
metadata; the REST backend adds `HS_ADMIN` as usual. Missing or unchanged source
PIDs produce no alias. Alias Handles use the shared `AliasHandleModel` schema;
aliases are created only for datasets, never for files. Conflicting source
dataset PID fields must be resolved before an alias can be created. Remap
existing Handle files before publishing with the new policy.

The packaged `src/piddiplatsch/config/default.toml` lists all defaults.

Production defaults use `schema.strict_mode = true` and stop mapping on the
first error. Configure a higher limit for just one project:

```toml
[plugins.cmip6plus]
max_errors = 10000
```

`consumer.max_errors` is no longer supported. Move that setting into the
appropriate `[plugins.<project>]` table; other projects default to `1`.
Values `0` and `1` both stop at the first actual error; `-1`
allows unlimited errors. Manual map counts errors independently for each
selected project. Sync counts errors per project batch and pauses that project
for the rest of the run when its limit is reached. Other projects can continue.

Map and sync treat invalid assets as errors. Sync durably saves valid Handles
and rejected inputs before advancing past processed items, including when the
error limit stops a batch. Unprocessed items stay pending for the next run.
Catalog harvest remains fail-fast independently of these mapping settings.

## Disk headroom

```toml
[disk]
enabled = true
warn_free_gib = 40
stop_free_gib = 20
check_interval_seconds = 10
```

Piddi checks available space on the output, catalog, batch, and enabled monitoring
database locations. Separate configured receipt output paths are checked too.
Missing directories use their nearest existing parent without creating it.
Thresholds are in GiB, and the warning threshold must exceed the stop threshold.
Paths on the same filesystem share a check and a health row. The application
logs a warning once per checked filesystem while it remains below the
warning threshold, and logs recovery when space becomes healthy again.

Checks run at startup and batch boundaries, and at most every ten seconds during
processing. Below the stop threshold, harvest stops before processing or
acknowledging the next message. Mapping and publication stop new work; sync keeps
incomplete work unpublished or ready for retry. Completed server writes can be
repeated on retry. This is a headroom guard rather than a disk reservation: other
processes and large writes can consume space between checks. Free space before
restarting a stopped job. Set `enabled = false` only for a deliberate override.

## Dataset version lookup

The library's `[lookup]` section selects `catalog` or `stac` for separate version
queries. Map always uses its local catalog snapshot. Its predecessor links are
controlled by `catalog.version_history`, not by `[lookup]`.

Elasticsearch lookup is no longer supported. Remove old `[elasticsearch]` settings
and `[lookup].backend = "es"`.

## Removed configuration

Use `[handles]`, `[handles.defaults]`, and `[handles.profiles.<name>]` for Handle
services, and `[plugins.<project>]` for project settings. The old `[handle]` and
root project tables such as `[cmip6]` are rejected instead of overriding current
settings.

`[consumer.transient]` and its old flat aliases are removed. Harvest owns patch
reconstruction and stops when recovery fails; map reads full local records and
never fetches a patch base from remote STAC. The library `force` bypass and
failure-file retry runner are removed. Failure files remain diagnostics.

`[stats].history_minutes` is removed; `top` uses the last five minutes.
`[stats].db_path` is removed; the single `piddi.db` follows `catalog.directory`
or `consumer.output_dir`.
