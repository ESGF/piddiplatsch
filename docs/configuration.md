# Configuration

Piddiplatsch loads configuration in this order, with each existing file
overriding values from the preceding layer:

1. packaged `src/piddiplatsch/config/default.toml`
2. site-wide `/etc/piddi/piddi.toml`
3. local `./custom.toml`

Missing optional site and local files are ignored. An explicit `--config PATH`
must name an existing file; a missing file or directory is an error. Keep credentials in the
local ignored file. `--config PATH` replaces the third layer with the selected
file; the packaged and site-wide layers are still loaded first.

## Start with a small site override

Copy `etc/esgf-example.toml` to `custom.toml` and replace its placeholders.
Set the projects/topic, Kafka connection and consumer group, Handle service and
credentials, and output directory. Packaged defaults supply `SASL_SSL` and `PLAIN`
for authenticated ESGF Kafka; the example supplies Kafka credential placeholders. Match
these values to your broker; they are connection requirements, not advanced
tuning. See `etc/esgf-example.toml` for alternative mechanisms and a custom CA.
Ansible uses the same `custom.toml` for production and installs it as
`/etc/piddi/piddi.toml`, adding production output/log/database path defaults
only where the TOML omits them. Explicit TOML values take precedence.
The optional `deploy/ansible/custom.yml` contains only deployment controls;
Kafka, Handle, project, and mapping values belong in TOML once.
See [deployment and migration](../deploy/README.md).

You do not need to copy the full packaged defaults. Leave retry delays, monitoring
intervals, mapping limits, and optional backends inherited until you need to tune them.
The packaged `default.toml` is the full reference, not a setup checklist.

```bash
piddi config validate
piddi config explain --project cmip6
piddi config explain --project cmip6 --handle-profile production
```

`explain` shows the resolved Handle service, prefix, STAC collection, and lookup
settings, with the configuration keys and files supplying them. It lists loaded
files in precedence order, listing full filenames once with short labels such
as `defaults` and `custom.toml`. Settings use those labels; repeated filenames
receive numbered labels to keep their origins distinct. It resolves output,
log, and database paths against
the current working directory. `--log` is reflected as a command-line override;
terminal logging and disabled database reporting are identified. Username/password
fields are omitted. The installed production config is attributed to
`/etc/piddi/piddi.toml`; inspect the source `custom.toml` separately when needed.
Use `config show` for the merged configuration, or narrow it with
`config show --section consumer`. Recognized credential fields (including
usernames, passwords, tokens, API keys, and private keys) display as
`***` by default, including nested plugin settings. Embedded URL
credentials and recognized secret query parameters are also redacted.
This applies to TOML/JSON output and to section/key filters. Runtime settings
are unchanged. Custom extension secrets should use descriptive credential keys
so they can be recognized.

To explicitly include the original credential values:

```bash
piddi config show --show-secrets
piddi config show --format json --show-secrets
```

`config validate` runs offline. It checks credentials when SASL PLAIN is selected,
nonnegative retry counts and backoff delays, ordered backoff bounds, and positive
monitoring intervals. Zero retries/delays and zero sample retention (no pruning)
remain supported. Likely misspelled application keys produce suggestions;
arbitrary Kafka options and plugin extensions remain accepted. Demo credential
and missing landing-page warnings concern only configured projects and the
Handle profiles they select (`projects = "all"` includes all registered projects).
These checks do not test broker connectivity or credential validity.

`config show`, `config explain`, and `config validate` do not initialize file
logging or create log files. They work even if the configured production log
directory is not writable. `--log` is shown as an override by `explain` but does
not open the file during inspection.

## Routine operation

Plain `piddi consume` prepares JSONL files. Use `piddi consume --publish` for
immediate delivery or `piddi publish` for deferred delivery. Publication mode is
a command choice rather than another site configuration setting.

Select projects with `[consumer].projects`, repeated `--project` flags, or
`--all-projects`. Command-line selection takes precedence. Processes filtering
different projects from the same topic must use different Kafka `group.id` values.

Use `-v` for INFO logging, `-vv` for DEBUG, and `--log PATH` to select a log file.
For permanent changes, set `[logging].level` and `[logging].file`.

Direct `rest` and `pyhandle` publication always appends the prepared record to
the project-scoped Handle JSONL file before contacting the service. This audit
output is not optional; `[consumer].output_dir` controls its root directory.

`piddi publish` always uses the REST Handle client, independently of the
profile's configured `backend`. The deferred command
publishes closed daily files using the project's selected profile and its
`server_url`, `prefix`, `username`, `password`, TLS verification, and timeout
settings. A deferred batch containing multiple projects is rejected; publish
each project's JSONL separately so server selection is unambiguous.

### Publication logs

Every successful publication writes an INFO entry to the configured log file
(`pid.log` by default). Pass `-v` to enable INFO logging and `--log PATH` to
override the configured destination. The entry identifies whether the server
created or updated the Handle and includes its directly resolvable REST URL, project,
dataset ID, file name, and source position. For file assets, the publisher joins
`IS_PART_OF` to a dataset record in the selected batch so the asset log entry
also includes `DATASET_ID`. Context that is unavailable in older JSONL input is
written as `-`.

## Multiple Handle servers

Each named profile inherits `[handles.defaults]` and overrides those values
with its own table. All projects inherit `[handles].default`. A project may select another named
profile with `plugins.<name>.handle`, and `--handle-profile NAME` temporarily
overrides both settings for `consume`, `map`, `publish`, or `retry`. Existing
Handle files retain their mapped prefix, so publication still rejects an
override whose prefix does not match the records.

Plugins can share a service and credentials while using different prefixes:

```toml
[handles]
default = "production"

[handles.profiles.production]
server_url = "https://handles.example.org"
prefix = "21.DEFAULT"
username = "shared-user"
password = "replace-me"

[plugins.cmip6]
handle_prefix = "21.CMIP6"

[plugins.cmip7]
handle_prefix = "21.CMIP7"
```

Both plugins inherit the production profile. A plugin can still select another
service with `handle = "another-profile"`. Prefix precedence is the plugin's
`handle_prefix`, then the selected profile's `prefix`, then
`[handles.defaults].prefix`. Profiles must still provide a fallback prefix,
either directly or through defaults, for callers without a project override.
The plugin prefix also applies when `--handle-profile` selects a different
service. Mapping,
JSONL output, immediate publication, and deferred publication all use this
effective prefix. The shared credentials must have permission to write each
plugin's prefix.

Keep real credentials in an ignored local override. Configuration examples can
live under `etc/`.

### How inheritance works

Suppose the site file contains the `production` profile above, and your local
file contains only:

```toml
[plugins.cmip6]
handle_prefix = "21.LOCAL"
```

For CMIP6, the service and credentials still come from `handles.profiles.production`,
the prefix becomes `21.LOCAL`, and the timeout remains inherited from
`handles.defaults` (10 seconds unless overridden). CMIP7 keeps its own prefix.
`piddi config explain --project cmip6` displays these resolved choices.

File merging happens first: packaged defaults → site file → local/`--config` file.
Project resolution happens afterwards: shared settings → selected profile or
project settings. A local file need only contain the individual keys you change.

## Kafka and project STAC settings

Kafka authentication defaults to `SASL_SSL` with the `PLAIN` mechanism.
Set broker addresses and credentials in your site/local override. Other
protocols and mechanisms can still be selected there.

The Docker test cluster uses unauthenticated Kafka. Its `tests/config.toml`
explicitly overrides the protocol, so run it with
`piddi --config tests/config.toml consume`. For another local configuration, add:

```toml
[kafka]
"security.protocol" = "PLAINTEXT"
```

Omitting this key inherits `SASL_SSL`; it does not select plaintext automatically.

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

STAC settings may be shared while each project selects its own collection:

```toml
[stac]
base_url = "https://discovery.east.esgf.io"
timeout = 10

[plugins.cmip6.stac]
collection = "CMIP6"

[plugins.cmip7.stac]
collection = "CMIP7"
```

Project settings inherit unspecified values from `[stac]`. A project-specific
`base_url` or `timeout` can therefore be added later if that collection moves to
another service. PATCH events continue to use their own `collection_id`; the
configured collection is used by STAC version lookups.

## Settings reference

All existing settings remain supported. Only override values your site needs.

### Site connections and project selection

| Section | Key | Purpose |
| --- | --- | --- |
| `consumer` | `projects` | Plugin names to run, as a list or the string `all`. |
| `consumer` | `topic` | Kafka topic to consume. |
| `consumer` | `output_dir` | Root for global dump/recovery files and `<plugin>/handles/` JSONL output. |
| `consumer` | `max_errors` | Stop after this many processing errors; `-1` disables the limit. |
| `handles` | `default` | Handle profile used when a project does not select one explicitly. |
| `handles.defaults` | `backend`, `verify_https`, `timeout` | Common values inherited by every named Handle profile. |
| `handles.profiles.<name>` | `backend` | `rest` for publication or legacy `pyhandle`; both always write JSONL first. |
| `handles.profiles.<name>` | `server_url`, `prefix`, `username`, `password` | Connection and credentials for one Handle service. |
| `handles.profiles.<name>` | `verify_https`, `timeout` | TLS verification and per-request timeout for one Handle service. |
| `stac` | `base_url`, `timeout`, `collection` | Global STAC defaults; the shared endpoint is `https://discovery.east.esgf.io`. |
| `logging` | `level` | Baseline `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` level. `WARN` is accepted as an alias. |
| `logging` | `file` | Log destination. An empty string selects terminal logging. |

### Mapping and validation

| Section | Key | Purpose |
| --- | --- | --- |
| `schema` | `strict_mode` | Reject incomplete or unsupported records; defaults to `true`. |
| `plugins.<name>` | `handle` | Named Handle profile used by this project. |
| `plugins.<name>` | `handle_prefix` | Optional prefix overriding the selected Handle profile's prefix for this project. |
| `plugins.<name>` | `landing_page_url`, `max_parts`, `excluded_asset_keys` | Project-specific Handle-record behavior. |
| `plugins.<name>.stac` | `base_url`, `timeout`, `collection` | Optional project overrides for the global STAC settings. |

### Advanced tuning and optional services

| Section | Key | Purpose |
| --- | --- | --- |
| `consumer.transient` | `stop_on_skip` | Stop after a transient external failure unless `--force` is used. |
| `consumer.transient` | `retries` | Number of retries for transient STAC patch retrieval. |
| `consumer.transient` | `backoff_initial`, `backoff_max` | Exponential retry delay bounds in seconds. |
| `consumer.transient` | `preflight_stac` | Probe the configured STAC service before consuming; disabled by default. |
| `lookup` | `enabled`, `backend` | Enable version lookup using `stac` or `es`; disabled by default. |
| `elasticsearch` | `base_url`, `index` | Elasticsearch lookup settings when `lookup.backend = "es"`. |
| `stats` | `interval_seconds`, `summary_interval` | Statistics reporting intervals. |
| `stats` | `enable_db`, `db_path`, heartbeat/sample/history intervals | Local SQLite state and history for `piddi consume` and `piddi map`. |

## Compatibility with older configuration

Legacy `[handle]` remains supported and takes precedence over named profiles,
including an explicit `--handle-profile` selection. A project `handle_prefix`
still overrides its prefix. Prefer `[handles.profiles.<name>]` for new setups.

Legacy top-level project tables such as `[cmip6]` also remain supported. Their
keys override `[plugins.cmip6]`; nested values such as `stac` are replaced at
that level rather than recursively merged by project resolution. Prefer
`[plugins.<name>]` for new setups and avoid mixing both forms.
