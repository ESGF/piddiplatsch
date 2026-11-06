# Configuration

Copy `etc/esgf-example.toml` to `custom.toml` and fill in your site settings.
Keep credentials out of version control.

## Configuration files

Piddi loads packaged defaults, then `/etc/piddi/piddi.toml`, then local
`custom.toml`. Later values override earlier ones. `--config PATH` selects a
different local file. Relative paths start at the current working directory.

Ansible installs the checkout's `custom.toml` as `/etc/piddi/piddi.toml` and adds
production paths where omitted. Services run from `/var/lib/piddi`.

```bash
piddi config validate
piddi config explain --project cmip6
piddi config show --section consumer
```

Validation runs offline; it does not test connections. Inspection hides known
credentials. `explain` shows the resolved service, prefix, paths, and settings.

## Connections and projects

In the example file, set:

| Section | Required site settings |
| --- | --- |
| `[consumer]` | `topic` and `projects`. |
| `[kafka]` | Broker, a stable unique group ID, client ID, and credentials. |
| `[handles]` | Default profile name. |
| `[handles.profiles.<name>]` | Server URL, prefix, username, and password. |

Kafka defaults to `SASL_SSL` with `PLAIN` authentication. Quote dotted Kafka keys,
such as `"bootstrap.servers"`. Local Docker tests use `tests/config.toml`, which
selects `PLAINTEXT` and a mock Handle server.

Supported project names are `cmip6`, `cmip6plus`, `cmip7`, and `cordex-cmip6`.
`consumer.projects` selects mapping and sync projects; `"all"` selects all plugins.
Harvest stores every registered project it encounters. Unknown projects stop
harvest by default; `catalog.ignore_unknown_projects = true` archives and skips
them. Malformed events still stop harvest.

Projects inherit the default Handle profile. Set `handle = "profile-name"` or
`handle_prefix` under `[plugins.<project>]` to override it. Keep HTTPS verification
enabled. Changing a profile does not change PIDs already prepared by mapping.

`[stac].base_url` supplies the remote service used when harvest needs a missing
PATCH base. Override it under `[plugins.<project>.stac]` if needed. Map stays offline.

## Scheduled batches

```toml
[batch]
trigger_items = 100
max_wait_seconds = 600
max_items = 2000
publish_workers = 4
publish_retries = 3
```

Sync starts mapping when either event count or waiting time reaches its threshold.
Each batch contains at most `max_items` events; each event may produce many Handles.
Manual map starts immediately and defaults its total event limit to `max_items`.

Projects run sequentially. `plugins.<project>.priority` is a weight from 0 to 255
(default 100); higher weights give more chances at batch boundaries. Zero disables
sync for that project. Manual map/publish still work. The Ansible timer checks
again 20 seconds after each sync run finishes.

## Common options

| Setting | Default and purpose |
| --- | --- |
| `consumer.output_dir` | `outputs`; root for local runtime files. |
| `catalog.directory` | Defaults to the output directory; contains shared `piddi.db`. |
| `catalog.version_history` | `true`; link to the nearest older version in the local catalog for all projects. Disable per run with `map --no-version-history`. |
| `plugins.<project>.max_errors` | `1`; mapping stops at the first error. `-1` allows unlimited errors. Harvest remains fail-fast. |
| `plugins.<project>.max_parts` | Maximum dataset file links; `0` omits them, `-1` is unlimited. |
| `plugins.<project>.force_dataset_pid` | `false`; generate dataset PIDs from versioned item IDs, with aliases for differing source PIDs. |
| `logging.level` | `INFO`; progress summaries. |
| `logging.file` | `pid.log` locally; also creates `pid.error.log`. |
| `stats.enable_db` | `true`; enables live heartbeats and rates for `top`. |
| `disk.warn_free_gib` / `disk.stop_free_gib` | `40` / `20`; warn or persistently stop on low space. |

## Retention

Maintenance compresses closed raw dumps and completed output by default.
Set `maintenance.deduplicate_dumps = true` to remove identical coordinate-bearing
Kafka events from closed raw dumps during daily maintenance. Today's UTC files,
legacy records without coordinates, and conflicting payloads are retained.
Automatic deletion is disabled:

- `batch.retention_days = 0` keeps published database work history.
- `maintenance.output_retention_days = 0` keeps completed batches and receipts.

Positive day values enable the corresponding cleanup. Pending and invalid work,
catalog records, and batch summaries are preserved. Kafka dumps are not deleted
by maintenance. Review planned changes with `piddi maintain --dry-run`.

For all settings, see the commented
[packaged defaults](https://github.com/ESGF/piddiplatsch/blob/main/src/piddiplatsch/config/default.toml).
