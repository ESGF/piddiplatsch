# Changelog

## [Unreleased]

### Added

- `top` displays the configured batch event trigger, maximum wait, and batch size.
- `piddi admin clean` removes completed outbox records without rebuilding work
  history; `--apply` performs cleanup and optional `--vacuum` reclaims file space.
  Unfinished batches and completion markers needed for ready exports are retained.
- Transactional SQLite Handle outbox: mapping commits payloads, work states, and
  statistics together; publication progress survives missing batch files and
  resumes ordered Handle updates. Schema 4 upgrades additively to 5, importing
  legacy ready batches and their progress journals when processed. Batch files
  remain audit exports; completed queue payloads are removed transactionally.
- Per-project `priority` weights for sync, integers 0–255 with an equal default
  of 100. Zero disables sync for the project; eligible projects compete by weight
  at each batch boundary. Explicit map/publish and harvest remain independent.
- Companion error log beside the normal log (`piddi.error.log`), containing
  ERROR/CRITICAL messages and tracebacks, with deployment log rotation.
- Raw JSONL dump rotation by UTC day and event count (`consumer.dump_max_events`,
  default 100,000), with numbered parts, restart continuation, and maintenance
  compression of previous days' parts.
- Project-specific mapping error limits under `[plugins.<name>].max_errors`;
  removed the consumer-level setting. Unconfigured projects stop on the first error.
- Per-project numeric `Errors` totals in `piddi top`, including finished runs.
- `make status` lists the state of all deployed Piddi services and timers.
- `piddi reset --project NAME` previews a processing reset; `--apply` backs up
  project output, clears its monitoring counters, and requeues its existing
  catalog items while preserving STAC records and Kafka checkpoints.
- Publication batch ETA and approximate known-backlog completion estimates in
  `piddi top`, based on recent Handle throughput and observed mapping ratios.
- Configurable disk-space warning/stop guards, daily maintenance for closed raw
  dump compression, and storage/backlog/publication health in `piddi top`.
- Shared alias Handle schema and dataset redirects for all projects when
  `force_dataset_pid` is enabled, including CMIP7.
- Service controls: `make start`, `make stop`, and `make restart`.
- Sync options: `--project`, `--workers`, and `--retries`.
- Dependency checks in CI and `make audit`.

### Changed

- Publication acknowledgements waiting behind a SQLite commit share the next
  transaction, reducing disk syncs under contention. Workers still wait for
  their own durable acknowledgement before the next update to the same Handle;
  `synchronous=FULL` and crash-recovery ordering are unchanged.
- `top` distinguishes `waiting` (below both batch triggers), `ready` (eligible
  for mapping), and `queued` (mapped and awaiting publication), using configured
  thresholds. Priority-zero projects with work show `disabled`.
- `admin rebuild` archives and clears completed SQLite outbox records alongside
  work consolidation, refusing unfinished outbox work. Existing `--vacuum`
  optionally returns the freed database pages to the filesystem.
- Standalone `map` defaults its total event limit to `[batch].max_items` (2,000)
  across selected projects. Explicit `--limit N` overrides that total; sync retains
  continuous batch-by-batch mapping/publication.
- Scheduled sync checks default to 20 seconds after each run, with one-second
  timer accuracy; mapping starts at 100 pending events per project or a maximum
  wait of 10 minutes.
- Removed legacy `[handle]` and root project settings, transient mapping/force
  options, failure-file retry helpers, the dummy STAC client, and old Ansible
  timer aliases. Mapping consumes full records; harvest owns PATCH recovery.
  Use named Handle profiles and `[plugins.<project>]` settings.
- Published sync batches use UTC date directories; default sync batches now
  trigger at 100 and contain up to 2,000 catalog items.
- Publication receipts also use UTC date directories under `published/`.
- Simpler INFO progress logs; per-record details moved to DEBUG.
- Statistics summaries every 30 seconds or 10,000 messages by default.
- Four publication workers by default.
- Updated dependencies, security minimums, and development tools.
- Removed obsolete docs, simplified the guides, and added deployment to MkDocs.

### Fixed

- Sync's mapped-batch validation uses the project/state index, avoiding a scan
  of pending and completed work before preparing each new batch.
- `piddi top` retains a batch activity heartbeat through preparation, mapping,
  sealing, verification, publication, and finalization, displaying each phase
  instead of appearing pending between mapping and publishing.
- Sync scheduling uses bounded index checks instead of repeatedly scanning the
  pending backlog before each batch, reducing gaps between mapping/publication.
- A zero error limit stops at the first actual error instead of stopping after
  a successful record.
- `piddi top` retains errors from the latest finished mapping run after switching
  to publication, including batches with rejected items and zero Handles.
- Sync batches retain valid Handles and quarantine rejected items in a durable
  `failures.jsonl`, advancing past both instead of repeatedly generating partial
  batches. Reaching `max_errors` seals processed work before stopping the pass;
  later ticks continue with remaining items. Failed items are retried on a later
  harvested update. Partial Handles from rejected items are removed.
- Keep prepared/published Handle totals visible in `piddi top` alongside harvest,
  and persist live publication progress and heartbeats.
- Explain malformed asset checksums, including duplicated SHA-256 multihash
  prefixes; standalone map rejects invalid assets and processing defaults to
  stopping at the first error when no error limit is configured.
- Suppressed the expected Brotli GIL warning in CLI commands.

## [2.3.0] - 2026-10-12

### Added

- CMIP7, CMIP6Plus, and CORDEX-CMIP6 plugins.
- Separate `harvest`, `map`, and `publish` commands, with date selection.
- Concurrent Handle REST publication, retries, and per-record receipts.
- `piddi top` for live progress and recent history.
- Shared Handle profiles, per-project prefixes, and optional dataset PID generation.
- Configuration inspection and validation, with credentials hidden by default.
- Ansible deployment, systemd services, log rotation, and an Apple Silicon test VM.
- MkDocs guides and overview slides.

### Changed

- Shared processing code across plugins and commands.
- Bounded JSONL streaming with ordered updates to each Handle.
- Manual runs and deployment share `custom.toml`.
- Kafka defaults to `SASL_SSL` with `PLAIN`; local brokers need `PLAINTEXT`.
- Failure files are grouped by project. Publication writes JSONL audit records first.
- Consume and retry defer publication unless `--publish` is set.
- Configuration commands avoid unnecessary client imports and file logging.

### Fixed

- Safer retries, failure handling, output paths, statistics, and Docker builds.
- Tests keep local runtime files untouched.

### Removed

- `retry --dry-run` and legacy list-based JSONL loaders.

## [2.2.1] - 2026-08-25

- Moved the repository to `ESGF/piddiplatsch` and updated links.
- Renamed the notebook environment to `piddiplatsch`.

## [2.2.0] - 2026-03-03

- Moved CMIP6 settings to `[plugins.cmip6]` and added a static plugin registry.
- Improved progress labels and simplified the documentation.
- Updated type annotations, CI checks, and Black configuration.

## [2.1.0] - 2026-01-22

- Added class-based retries with file selection, skipped records, and cleanup options.
- Added `--force` to continue after temporary external failures.
- Shared result models, JSONL helpers, and persistence code.
- Standardized timestamps to UTC.

## [2.0.0] - 2026-01-13

- Initial Kafka consumer, CLI, and project tooling.
- Added version management and release targets.
