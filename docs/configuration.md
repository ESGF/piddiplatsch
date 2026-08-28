# Configuration

Piddiplatsch loads `src/piddiplatsch/config/default.toml` first and recursively
merges the file passed with `--config` over those defaults. Keep credentials in
a local ignored file such as `custom.toml`.

Validate and inspect the effective configuration before a run:

```bash
piddi --config custom.toml config validate
piddi --config custom.toml config show
```

## Application settings

| Section | Key | Purpose |
| --- | --- | --- |
| `consumer` | `projects` | Plugin names to run, as a list or the string `all`. |
| `consumer` | `topic` | Kafka topic to consume. |
| `consumer` | `output_dir` | Root for global dump/recovery files and `<plugin>/handles/` JSONL output. |
| `consumer` | `max_errors` | Stop after this many processing errors; `-1` disables the limit. |
| `consumer.transient` | `stop_on_skip` | Stop after a transient external failure unless `--force` is used. |
| `consumer.transient` | `retries` | Number of retries for transient STAC patch retrieval. |
| `consumer.transient` | `backoff_initial`, `backoff_max` | Exponential retry delay bounds in seconds. |
| `consumer.transient` | `preflight_stac` | Probe the configured STAC service before consuming; disabled by default. |
| `handle` | `backend` | `rest` for publication or legacy `pyhandle`; both always write JSONL first. |
| `handle` | `server_url`, `prefix`, `username`, `password` | Handle service connection and credentials. |
| `handle` | `verify_https` | Verify Handle service TLS certificates; defaults to `true`. Setting it to `false` suppresses repeated urllib3 insecure-request warnings. |
| `handle` | `timeout` | Per-request Handle REST timeout in seconds; defaults to `10`. |
| `stac` | `base_url`, `timeout`, `collection` | Optional STAC lookup and patch retrieval settings; no remote URL is configured by default. |
| `lookup` | `enabled`, `backend` | Enable version lookup using `stac` or `es`; disabled by default. |
| `elasticsearch` | `base_url`, `index` | Elasticsearch lookup settings when `lookup.backend = "es"`. |
| `schema` | `strict_mode` | Reject incomplete or unsupported records; defaults to `true`. |
| `plugins.<name>` | `landing_page_url`, `max_parts`, `excluded_asset_keys` | Project-specific Handle-record behavior. |
| `stats` | `interval_seconds`, `summary_interval` | Statistics reporting intervals. |
| `stats` | `enable_db`, `db_path` | Optional SQLite statistics reporter. |

Direct `rest` and `pyhandle` publication always appends the prepared record to
the project-scoped Handle JSONL file before contacting the service. This audit
output is not optional; `[consumer].output_dir` controls its root directory.

`piddi publish` always uses the REST Handle client, independently of the
configured `handle.backend`. The Kafka consumer always records prepared Handles
to project-scoped JSONL before publishing through either `rest` or `pyhandle`.
Plain `consume` writes JSONL without contacting a Handle service; add
`--publish` only when immediate publication is intended. The deferred command
publishes closed daily files using the configured
`server_url`, `prefix`, `username`, `password`, TLS verification, and timeout
settings.

Every successful publication writes an INFO entry to the configured `--log`
file (`pid.log` by default). The entry identifies whether the server created or
updated the Handle and includes its directly resolvable REST URL, project,
dataset ID, file name, and source position. For file assets, the publisher joins
`IS_PART_OF` to a dataset record in the selected batch so the asset log entry
also includes `DATASET_ID`. Context that is unavailable in older JSONL input is
written as `-`.

All additional keys under `[kafka]` are passed to `confluent-kafka`. Dotted
librdkafka keys must be quoted in TOML, for example
`"bootstrap.servers" = "broker:9092"`.

Use `etc/esgf-example.toml` as a starting point for authenticated ESGF Kafka
settings.

Select projects in configuration:

```toml
[consumer]
projects = ["cmip6"]
# projects = ["cmip6", "cmip7"]
# projects = "all"
```

For a single invocation, repeat `--project` or use `--all-projects`. CLI
selection overrides configuration. Separate project-specific processes reading
the same topic must use different Kafka `group.id` values; otherwise Kafka can
deliver a project's records to a process that filters them out.
