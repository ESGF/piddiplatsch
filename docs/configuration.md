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
version_history = false
kafka_commit_batch_size = 100
kafka_commit_interval_seconds = 1.0
# directory = "/var/lib/piddi/catalog"
```

The default directory is `<consumer.output_dir>/catalog`. SQLite is the supported
backend. `enabled` controls catalog writes during harvest. Map always reads
catalogs, even if harvest catalog writes are disabled.

Keep `version_history` disabled for the first fill. Enable it to link each dataset
to the nearest older version in the same local catalog. Map's `--version-history`
and `--no-version-history` options override it for one run.

The two commit settings control how often harvest acknowledges saved events to
Kafka. Local writes remain durable for each event. A batch size of `1` sends an
acknowledgement for each message and can reduce throughput.

## Scheduled batches

```toml
[batch]
trigger_items = 10000
max_wait_seconds = 3600
max_items = 10000
publish_workers = 4
publish_retries = 3
# directory = "/var/lib/piddi/batches"
```

Sync maps a project when either 10,000 distinct items have changed or the oldest
pending change has waited one hour. Repeated changes to one item count once.
`sync --force-map` skips the wait for pending items.

`max_items` limits catalog items per batch. An item's dataset and file Handles
stay together. Ready batches publish during the same sync run, even when no new
mapping is due. Failed publication stays ready for the next run.

These limits count catalog items, not Handles or bytes. With ten Handles per
item, a full default batch contains about 100,000 Handles and takes roughly
seven minutes to publish at 250 Handles per second. Larger batches reduce
directory counts but increase the work repeated after a publication failure.
Existing site configurations retain their explicit limits; update both
`trigger_items` and `max_items` to use the larger defaults.

`publish_workers` sets concurrent Handle requests. `publish_retries` sets retries
for each temporary HTTP failure. Sync's `--workers` and `--retries` options
override these values for one run, including publication of existing ready batches.

The default directory is `<consumer.output_dir>/batches`. All sync runs must use
the same directory so they share batches and the worker lock. The Ansible setting
`piddi_sync_check_interval` controls how often the timer runs.

## Logging and monitoring

```toml
[logging]
level = "INFO"
file = "pid.log"

[stats]
enable_db = true
db_path = "piddi.db"
interval_seconds = 30
summary_interval = 10000
heartbeat_interval_seconds = 5
stale_after_seconds = 15
sample_interval_seconds = 15
history_minutes = 60
sample_retention_days = 30
```

INFO logs show progress summaries. Use `-vv` or `--debug` for record details.
`--log PATH` overrides the log file for one command. Set `logging.file = ""`
to log to the terminal. `--silent` hides progress bars but keeps final summaries.

Statistics log every 30 seconds or 10,000 messages as results arrive.
The monitoring database records harvest and mapping. `piddi top` reads it.
Publication results are in the logs and JSONL receipts.

Heartbeats update every five seconds, even when Kafka is idle. Unfinished runs
become stale after 15 seconds without a heartbeat. History samples are kept for
30 days; use `sample_retention_days = 0` to keep them indefinitely.
See [Logging and statistics](operations.md#logging-and-statistics) for examples.

## Mapping settings

| Setting | Purpose |
| --- | --- |
| `consumer.output_dir` | Root directory for catalogs, Handle files, receipts, and diagnostic files. |
| `consumer.max_errors` | Stop mapping at this error count. Default: `1`; `-1` removes the limit. |
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

Production defaults use `schema.strict_mode = true` and `consumer.max_errors = 1`.
Map and sync treat invalid assets as errors and stop the affected mapping run
at the first error. Sync leaves the failed batch unpublished and its catalog
changes pending. Malformed checksum errors identify the dataset and asset;
verify the checksum against the source file, correct its STAC metadata, and
reharvest before retrying. Other projects can continue in sync.

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

## Compatibility with older configuration

Old `[handle]` settings still work and take priority over named profiles, even
with `--handle-profile`. A project's `handle_prefix` still overrides the prefix.
Use named profiles for new configurations.

Old project tables such as `[cmip6]` override `[plugins.cmip6]`. Nested values such
as `stac` replace that table rather than merge individual keys. Use
`[plugins.<name>]` for new settings and avoid mixing both forms.

`[consumer.transient]` applies only to legacy library processing. Catalog harvest
stops when remote recovery fails; map uses local items.
